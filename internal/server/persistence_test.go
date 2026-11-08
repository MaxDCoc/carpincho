package server

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/MaxDCoc/carpincho/internal/aof"
	"github.com/MaxDCoc/carpincho/internal/resp"
)

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000)
	return c
}

// openServer starts a server with persistence in dir, sharing one fake clock
// across restarts so tests can move time forward between a stop and a start.
func openServer(t *testing.T, dir string, clk *fakeClock, p aof.Policy) *harness {
	t.Helper()
	srv, err := New(Config{Dir: dir, Fsync: p, Clock: clk.now})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &harness{t: t, srv: srv, buf: &bytes.Buffer{}, clock: clk}
	h.cl = newClient(srv, nil)
	h.cl.w = resp.NewWriter(h.buf)
	t.Cleanup(func() {
		h.cl.close()
		_ = srv.Shutdown(context.Background())
	})
	return h
}

func (h *harness) stop() {
	h.t.Helper()
	h.cl.close()
	if err := h.srv.Shutdown(context.Background()); err != nil {
		h.t.Fatalf("Shutdown: %v", err)
	}
}

func (h *harness) restart(dir string, p aof.Policy) *harness {
	h.t.Helper()
	h.stop()
	return openServer(h.t, dir, h.clock, p)
}

func TestDataSurvivesARestartWithEveryFsyncPolicy(t *testing.T) {
	for _, p := range []aof.Policy{aof.Always, aof.Everysec, aof.No} {
		t.Run(p.String(), func(t *testing.T) {
			dir := t.TempDir()
			h := openServer(t, dir, newClock(), p)
			h.run(
				"SET s v", "+OK",
				"RPUSH l a b c", ":3",
				"HSET h f1 v1 f2 v2", ":2",
				"SADD st x y", ":2",
				"INCR n", ":1",
				"SET t v EX 100", "+OK",
				"LPOP l", `"a"`,
				"DEL s", ":1",
				"MSET m1 1 m2 2", "+OK",
				"APPEND ap hi", ":2",
				"SET gone v", "+OK",
				"EXPIRE gone -1", ":1",
			)
			h2 := h.restart(dir, p)
			h2.run(
				"GET s", "(nil)",
				"LRANGE l 0 -1", `["b", "c"]`,
				"HLEN h", ":2",
				"HGET h f2", `"v2"`,
				"SCARD st", ":2",
				"GET n", `"1"`,
				"TTL t", ":100",
				"MGET m1 m2", `["1", "2"]`,
				"GET ap", `"hi"`,
				"EXISTS gone", ":0",
				"DBSIZE", ":8",
			)
		})
	}
}

// The AOF must store deadlines, not delays: replaying "EXPIRE k 100" a day
// later must not give the key another 100 seconds.
func TestRelativeExpiryIsStoredAsAnAbsoluteDeadline(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	h := openServer(t, dir, clk, aof.Always)
	h.run("SET k v EX 100", "+OK", "SET j v", "+OK", "EXPIRE j 100", ":1")
	clk.advance(40 * time.Second)
	h2 := h.restart(dir, aof.Always)
	h2.run("TTL k", ":60", "TTL j", ":60")
}

func TestKeysThatExpiredWhileTheServerWasDownStayGone(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	h := openServer(t, dir, clk, aof.Always)
	h.run("SET k v PX 100", "+OK", "SET keep v", "+OK")
	clk.advance(time.Second)
	h2 := h.restart(dir, aof.Always)
	h2.run("GET k", "(nil)", "GET keep", `"v"`)
}

// Without suspending expiry during replay, "now" is an hour later than when the
// commands originally ran: the INCR would find the key already expired and the
// replayed history would diverge from what really happened.
func TestReplayDoesNotExpireKeysThatWereAliveWhenTheCommandRan(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	h := openServer(t, dir, clk, aof.Always)
	h.run("SET n 10 PX 100", "+OK", "INCR n", ":11", "PERSIST n", ":1")
	clk.advance(time.Hour)
	h2 := h.restart(dir, aof.Always)
	h2.run("GET n", `"11"`, "TTL n", ":-1")
}

func TestATornLastRecordIsCutAndTheLogKeepsWorking(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	h := openServer(t, dir, clk, aof.Always)
	h.run("SET a 1", "+OK", "SET b 2", "+OK")
	h.stop()

	f, err := os.OpenFile(filepath.Join(dir, aof.FileName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$5\r\nhel") // the process died here
	_ = f.Close()

	h2 := openServer(t, dir, clk, aof.Always)
	h2.run("GET a", `"1"`, "GET b", `"2"`, "GET c", "(nil)", "SET c 3", "+OK")
	h3 := h2.restart(dir, aof.Always)
	h3.run("GET c", `"3"`)
}

func TestARewriteShrinksTheLogAndKeepsEveryKey(t *testing.T) {
	dir := t.TempDir()
	h := openServer(t, dir, newClock(), aof.Always)
	h.run("SET n 0", "+OK")
	for i := 0; i < 1000; i++ {
		h.do("INCR", "n")
	}
	h.run("RPUSH l a b", ":2", "HSET h f v", ":1", "SADD s m", ":1", "SET t v EX 100", "+OK", "RPUSH l2 x", ":1", "EXPIRE l2 50", ":1")
	before := h.srv.aof.Size()
	if err := h.srv.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	after := h.srv.aof.Size()
	if after*10 > before {
		t.Fatalf("rewrite did not compact: %d -> %d bytes", before, after)
	}
	h.run("INCR n", ":1001") // appends after the swap land in the new file

	h2 := h.restart(dir, aof.Always)
	h2.run("GET n", `"1001"`, "LRANGE l 0 -1", `["a", "b"]`, "HGET h f", `"v"`, "SISMEMBER s m", ":1", "TTL t", ":100", "TTL l2", ":50")
}

func TestBGREWRITEAOFRunsInTheBackground(t *testing.T) {
	dir := t.TempDir()
	h := openServer(t, dir, newClock(), aof.No)
	for i := 0; i < 200; i++ {
		h.do("SET", "k", strconv.Itoa(i))
	}
	before := h.srv.aof.Size()
	h.run("BGREWRITEAOF", "+Background append only file rewriting started")
	deadline := time.Now().Add(10 * time.Second)
	for h.srv.rewriting.Load() {
		if time.Now().After(deadline) {
			t.Fatal("rewrite did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if after := h.srv.aof.Size(); after >= before {
		t.Fatalf("log did not shrink: %d -> %d", before, after)
	}
	h2 := h.restart(dir, aof.No)
	h2.run("GET k", `"199"`)
}

// The strongest check of the rewrite: writers keep incrementing counters while
// rewrites run back to back. If any write fell between "snapshot taken" and
// "new file swapped in", or was applied twice, a counter would be off after the
// restart.
func TestRewriteIsExactWhileWritersAreRunning(t *testing.T) {
	dir := t.TempDir()
	h := openServer(t, dir, newClock(), aof.No)
	const writers, perWriter, counters = 8, 300, 4

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := newClient(h.srv, nil)
			cl.w = resp.NewWriter(io.Discard)
			key := []byte("c" + strconv.Itoa(w%counters))
			for i := 0; i < perWriter; i++ {
				cl.wmu.Lock()
				h.srv.execute(cl, [][]byte{[]byte("INCR"), key})
				cl.wmu.Unlock()
				cl.flushPending()
			}
		}(w)
	}
	stop := make(chan struct{})
	var rewrites sync.WaitGroup
	rewrites.Add(1)
	go func() {
		defer rewrites.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := h.srv.RewriteAOF(); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(stop)
	rewrites.Wait()

	h2 := h.restart(dir, aof.No)
	want := strconv.Quote(strconv.Itoa(writers / counters * perWriter))
	for c := 0; c < counters; c++ {
		if got := h2.do("GET", "c"+strconv.Itoa(c)); got != want {
			t.Errorf("counter c%d = %s after restart, want %s", c, got, want)
		}
	}
}
