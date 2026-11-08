package server

import (
	"errors"
	"io"
	"time"

	"github.com/MaxDCoc/carpincho/internal/resp"
	"github.com/MaxDCoc/carpincho/internal/store"
)

// startRewrite launches BGREWRITEAOF in a goroutine. Only one rewrite runs at a
// time.
func (s *Server) startRewrite() error {
	if s.aof == nil {
		return errors.New("AOF is not enabled")
	}
	if !s.rewriting.CompareAndSwap(false, true) {
		return errors.New("Background append only file rewriting already in progress")
	}
	go func() {
		defer s.rewriting.Store(false)
		start := time.Now()
		if err := s.RewriteAOF(); err != nil {
			s.logf("aof: rewrite failed: %v", err)
			return
		}
		s.logf("aof: rewrite finished in %v", time.Since(start).Round(time.Millisecond))
	}()
	return nil
}

// RewriteAOF compacts the append-only file: it replaces the history of
// commands by the shortest list of commands that rebuilds the current dataset.
//
// Redis forks and lets the child serialise a copy-on-write snapshot. Go cannot
// fork, so carpincho holds a read lock on every shard while it serialises:
// readers keep working, writers wait for the duration of the dump (see
// docs/adr/0002-aof-rewrite.md). Holding the locks until the new file is
// swapped in is what makes the result exact: no write can slip in between the
// snapshot and the swap.
func (s *Server) RewriteAOF() error {
	if s.aof == nil {
		return errors.New("AOF is not enabled")
	}
	rw, err := s.aof.BeginRewrite()
	if err != nil {
		return err
	}
	all := s.store.AcquireAll(false)
	defer all.Release()
	if err := dumpDataset(&all, rw); err != nil {
		rw.Abort()
		return err
	}
	return rw.Commit()
}

const chunk = 128 // elements per RPUSH/HSET/SADD record, to keep records reasonably small

// dumpDataset writes, for every live key, the commands that recreate it.
func dumpDataset(a *store.Access, w io.Writer) error {
	var buf []byte
	var werr error
	emit := func(args ...[]byte) {
		if werr != nil {
			return
		}
		buf = resp.AppendCommand(buf[:0], args)
		_, werr = w.Write(buf)
	}
	a.Range(func(key string, e *store.Entry, expireAt int64) bool {
		k := []byte(key)
		switch e.Kind {
		case store.KindString:
			if expireAt != 0 {
				emit([]byte("SET"), k, e.Str, []byte("PXAT"), itoa(expireAt))
			} else {
				emit([]byte("SET"), k, e.Str)
			}
			return werr == nil // the TTL of a string travels inside SET
		case store.KindList:
			for i := 0; i < e.List.Len(); i += chunk {
				args := [][]byte{[]byte("RPUSH"), k}
				emit(append(args, e.List.Slice(i, i+chunk-1)...)...)
			}
		case store.KindHash:
			args := [][]byte{[]byte("HSET"), k}
			pairs := 0
			for f, v := range e.Hash {
				args = append(args, []byte(f), v)
				if pairs++; pairs%chunk == 0 {
					emit(args...)
					args = [][]byte{[]byte("HSET"), k}
				}
			}
			if len(args) > 2 {
				emit(args...)
			}
		case store.KindSet:
			args := [][]byte{[]byte("SADD"), k}
			n := 0
			for m := range e.Set {
				args = append(args, []byte(m))
				if n++; n%chunk == 0 {
					emit(args...)
					args = [][]byte{[]byte("SADD"), k}
				}
			}
			if len(args) > 2 {
				emit(args...)
			}
		}
		if expireAt != 0 {
			emit([]byte("PEXPIREAT"), k, itoa(expireAt))
		}
		return werr == nil
	})
	return werr
}
