// Package aof implements carpincho's persistence: an append-only file (AOF)
// that records every write command in RESP, the same idea and the same file
// format as the Redis AOF. On start the file is replayed to rebuild the
// dataset; BeginRewrite compacts it.
//
// Durability policies (Policy) map to Redis' appendfsync:
//
//	Always    fsync before the client gets its reply (group commit: concurrent
//	          clients share one fsync)
//	Everysec  write immediately, fsync once per second in the background
//	No        write immediately, let the OS decide when to flush
//
// Writers call Append while they still hold the shard locks that protect the
// data they changed (so the log order per key equals the apply order) and call
// Commit, without locks, before replying.
package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MaxDCoc/carpincho/internal/resp"
)

// FileName is the name of the log inside the data directory.
const FileName = "appendonly.aof"

// ErrClosed is returned by Commit after Close.
var ErrClosed = errors.New("aof: closed")

// Policy selects when the log is fsynced.
type Policy int

// The three fsync policies.
const (
	Everysec Policy = iota
	Always
	No
)

// ParsePolicy parses "always", "everysec" or "no" (case-insensitive).
func ParsePolicy(s string) (Policy, error) {
	switch strings.ToLower(s) {
	case "always":
		return Always, nil
	case "everysec":
		return Everysec, nil
	case "no":
		return No, nil
	}
	return 0, fmt.Errorf("invalid appendfsync %q (want always, everysec or no)", s)
}

func (p Policy) String() string {
	switch p {
	case Always:
		return "always"
	case No:
		return "no"
	}
	return "everysec"
}

// LoadStats describes what Open found in the file.
type LoadStats struct {
	Commands  int   // commands replayed
	Truncated int64 // bytes of incomplete tail that were cut off (0 if the file was intact)
}

// maxBuffered bounds the in-memory buffer when a client appends a huge
// pipeline before committing.
const maxBuffered = 4 << 20

// AOF is an open append-only file. All methods are safe for concurrent use.
type AOF struct {
	path   string
	policy Policy

	mu       sync.Mutex // guards f, buf, appended, written, size, closed
	f        *os.File
	buf      []byte // records appended but not yet handed to the OS
	appended uint64 // total bytes ever appended: the "sequence" returned by Append
	written  uint64 // bytes handed to the OS so far (<= appended)
	size     int64  // size of the file on disk
	closed   bool

	syncMu sync.Mutex    // serialises fsyncs and the file swap done by a rewrite
	synced atomic.Uint64 // highest sequence known to be on stable storage

	stop chan struct{}
	wg   sync.WaitGroup
}

// Open opens (creating it if needed) the log inside dir and replays it through
// apply, one command at a time, before returning.
//
// A truncated last record, the typical result of a crash in the middle of a
// write, is cut off and reported in LoadStats. Anything else that cannot be
// parsed is an error: refusing to start beats silently dropping data.
func Open(dir string, policy Policy, apply func(args [][]byte) error) (*AOF, LoadStats, error) {
	var st LoadStats
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, st, fmt.Errorf("aof: create data dir: %w", err)
	}
	path := filepath.Join(dir, FileName)
	_ = os.Remove(path + ".rewrite.tmp") // leftover of an interrupted rewrite; the real log was never touched

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, st, fmt.Errorf("aof: open %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, st, fmt.Errorf("aof: stat: %w", err)
	}
	st.Commands, err = 0, nil
	good, n, err := load(f, apply)
	st.Commands = n
	if err != nil {
		_ = f.Close()
		return nil, st, err
	}
	if good < fi.Size() {
		st.Truncated = fi.Size() - good
		if err := f.Truncate(good); err != nil {
			_ = f.Close()
			return nil, st, fmt.Errorf("aof: cut truncated tail: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return nil, st, err
	}
	f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, st, fmt.Errorf("aof: reopen for append: %w", err)
	}
	a := &AOF{path: path, policy: policy, f: f, size: good, stop: make(chan struct{})}
	if policy == Everysec {
		a.wg.Add(1)
		go a.syncLoop()
	}
	return a, st, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// load replays the file. It returns the offset just after the last complete
// record (good), how many commands were applied, and an error only for
// corruption that is not a simple truncated tail.
func load(f *os.File, apply func([][]byte) error) (good int64, n int, err error) {
	cr := &countingReader{r: f}
	rd := resp.NewReader(cr)
	for {
		args, err := rd.ReadCommand()
		switch {
		case err == nil:
		case err == io.EOF, errors.Is(err, io.ErrUnexpectedEOF):
			return good, n, nil // clean end, or a torn last record that the caller will cut
		default:
			return good, n, fmt.Errorf("aof: corrupted file at byte %d: %w (fix or remove the file to continue)", good, err)
		}
		if err := apply(args); err != nil {
			return good, n, fmt.Errorf("aof: replaying command %d at byte %d: %w", n+1, good, err)
		}
		n++
		good = cr.n - int64(rd.Buffered()) // bytes consumed so far
	}
}

// Append adds one record (a RESP-encoded command) to the in-memory buffer and
// returns its sequence number, to be passed to Commit. It never blocks on the
// disk, except when the buffer exceeds a few MB.
func (a *AOF) Append(rec []byte) uint64 {
	a.mu.Lock()
	a.buf = append(a.buf, rec...)
	a.appended += uint64(len(rec))
	seq := a.appended
	if len(a.buf) > maxBuffered && !a.closed {
		_ = a.flushLocked() // the error resurfaces at the next Commit
	}
	a.mu.Unlock()
	return seq
}

// Commit makes the records up to seq visible to the OS and, with the Always
// policy, durable on disk. Concurrent callers share work: the first one writes
// everybody's pending records, and callers whose records were already covered
// by someone else's fsync return immediately (group commit).
func (a *AOF) Commit(seq uint64) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrClosed
	}
	err := a.flushLocked()
	f, written := a.f, a.written
	a.mu.Unlock()
	if err != nil {
		return err
	}
	if a.policy != Always {
		return nil
	}
	return a.fsync(f, max(seq, written))
}

// flushLocked writes the buffer to the file. a.mu must be held.
func (a *AOF) flushLocked() error {
	if len(a.buf) == 0 {
		return nil
	}
	n, err := a.f.Write(a.buf)
	a.written = a.appended
	if cap(a.buf) > 2*maxBuffered {
		a.buf = nil
	} else {
		a.buf = a.buf[:0]
	}
	if err != nil {
		// Never leave half a record in the middle of the log: cut back to the
		// last record boundary. The records of this batch are lost.
		_ = a.f.Truncate(a.size)
		return fmt.Errorf("aof: write: %w", err)
	}
	a.size += int64(n)
	return nil
}

// fsync makes everything up to sequence upTo durable, unless a concurrent
// fsync already did.
func (a *AOF) fsync(f *os.File, upTo uint64) error {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	if a.synced.Load() >= upTo {
		return nil
	}
	if err := f.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("aof: fsync: %w", err)
	}
	// A rewrite may have raised synced meanwhile: only ever move it forward.
	for cur := a.synced.Load(); cur < upTo; cur = a.synced.Load() {
		if a.synced.CompareAndSwap(cur, upTo) {
			break
		}
	}
	return nil
}

// syncLoop is the Everysec background fsync.
func (a *AOF) syncLoop() {
	defer a.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.mu.Lock()
			if a.closed {
				a.mu.Unlock()
				return
			}
			_ = a.flushLocked()
			f, w := a.f, a.written
			a.mu.Unlock()
			_ = a.fsync(f, w)
		}
	}
}

// Size returns the size of the log including records still in the buffer.
func (a *AOF) Size() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.size + int64(len(a.buf))
}

// Policy returns the fsync policy.
func (a *AOF) Policy() Policy { return a.policy }

// Close writes pending records, fsyncs (whatever the policy: a clean shutdown
// must not lose acknowledged data) and closes the file.
func (a *AOF) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	close(a.stop)
	err := a.flushLocked()
	f, w := a.f, a.written
	a.mu.Unlock()
	a.wg.Wait()
	return errors.Join(err, a.fsync(f, max(w, 1)), f.Close())
}

// Rewrite is a compaction in progress: a new log is being written next to the
// live one and replaces it atomically at Commit.
type Rewrite struct {
	a       *AOF
	tmp     *os.File
	bw      *bufio.Writer
	tmpPath string
	done    bool
}

// BeginRewrite starts a compaction. The caller must hold every shard lock
// (read locks suffice) from before it starts writing the snapshot until Commit
// returns: that guarantees no record is appended in between, so everything
// buffered in the old log is already contained in the snapshot.
func (a *AOF) BeginRewrite() (*Rewrite, error) {
	tmpPath := a.path + ".rewrite.tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("aof: rewrite: %w", err)
	}
	return &Rewrite{a: a, tmp: f, bw: bufio.NewWriterSize(f, 1<<20), tmpPath: tmpPath}, nil
}

// Write appends snapshot data (RESP commands) to the new log.
func (r *Rewrite) Write(p []byte) (int, error) { return r.bw.Write(p) }

// Abort discards the new log and leaves the old one in place.
func (r *Rewrite) Abort() {
	if r.done {
		return
	}
	r.done = true
	_ = r.tmp.Close()
	_ = os.Remove(r.tmpPath)
}

// Commit makes the new log durable and swaps it in for the old one. Records
// appended after Commit go to the new file.
func (r *Rewrite) Commit() error {
	if r.done {
		return errors.New("aof: rewrite already finished")
	}
	if err := r.bw.Flush(); err != nil {
		r.Abort()
		return fmt.Errorf("aof: rewrite flush: %w", err)
	}
	if err := r.tmp.Sync(); err != nil {
		r.Abort()
		return fmt.Errorf("aof: rewrite fsync: %w", err)
	}
	size, _ := r.tmp.Seek(0, io.SeekCurrent)
	if err := r.tmp.Close(); err != nil {
		r.Abort()
		return err
	}
	r.done = true

	a := r.a
	a.syncMu.Lock() // no fsync of the old file may run while we swap it
	defer a.syncMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		_ = os.Remove(r.tmpPath)
		return ErrClosed
	}
	// Whatever is still buffered was applied to memory before the snapshot was
	// taken, so the snapshot already contains it: drop it instead of writing it.
	a.buf = a.buf[:0]
	a.written = a.appended
	old := a.f
	_ = old.Close()
	if err := os.Rename(r.tmpPath, a.path); err != nil {
		// Keep running on the old log.
		if nf, e := os.OpenFile(a.path, os.O_WRONLY|os.O_APPEND, 0o644); e == nil {
			a.f = nf
		}
		_ = os.Remove(r.tmpPath)
		return fmt.Errorf("aof: rewrite rename: %w", err)
	}
	nf, err := os.OpenFile(a.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("aof: reopen after rewrite: %w", err)
	}
	a.f = nf
	a.size = size
	syncDir(filepath.Dir(a.path))
	for cur := a.synced.Load(); cur < a.appended; cur = a.synced.Load() {
		if a.synced.CompareAndSwap(cur, a.appended) {
			break
		}
	}
	return nil
}

// syncDir fsyncs a directory so a rename survives power loss. Best effort: not
// every platform allows it.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
