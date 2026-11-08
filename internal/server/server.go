// Package server is carpincho's TCP front end. It accepts connections (one
// goroutine each), parses RESP with package resp, runs commands against the
// sharded keyspace in package store and logs writes to the append-only file in
// package aof.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MaxDCoc/carpincho/internal/aof"
	"github.com/MaxDCoc/carpincho/internal/resp"
	"github.com/MaxDCoc/carpincho/internal/store"
)

// Version is carpincho's version, reported by INFO.
const Version = "0.1.0"

// ErrServerClosed is returned by Serve when Shutdown was called first.
var ErrServerClosed = errors.New("server: closed")

// Config configures a Server.
type Config struct {
	Dir    string       // data directory; empty disables persistence
	Fsync  aof.Policy   // appendfsync policy (ignored without Dir)
	Clock  func() int64 // unix milliseconds; nil means the system clock (tests inject a fake)
	Logger *log.Logger  // nil discards logs
}

// Server is a running (or ready to run) carpincho instance.
type Server struct {
	cfg       Config
	store     *store.Store
	aof       *aof.AOF // nil when persistence is disabled
	commands  registry
	hub       *hub
	log       *log.Logger
	startedAt time.Time

	mu      sync.Mutex
	ln      net.Listener
	conns   map[*Client]struct{}
	closing bool

	wg   sync.WaitGroup // client goroutines
	bg   sync.WaitGroup // background goroutines (active expiry)
	stop chan struct{}

	nextID    atomic.Int64
	rewriting atomic.Bool
	stats     struct{ commands, connections atomic.Int64 }
}

// New builds a Server. With Config.Dir set it opens the append-only file and
// replays it before returning, so the dataset is ready when New returns.
func New(cfg Config) (*Server, error) {
	s := &Server{
		cfg:       cfg,
		store:     store.New(cfg.Clock),
		commands:  newRegistry(),
		hub:       newHub(),
		conns:     make(map[*Client]struct{}),
		stop:      make(chan struct{}),
		startedAt: time.Now(),
		log:       cfg.Logger,
	}
	if s.log == nil {
		s.log = log.New(io.Discard, "", 0)
	}
	if cfg.Dir != "" {
		if err := s.openAOF(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// openAOF replays the log through the same command table the network uses.
func (s *Server) openAOF() error {
	rc := newClient(s, nil)
	rc.replay = true
	rc.w = resp.NewWriter(io.Discard)

	s.store.SetLoading(true) // keys must not expire while history is replayed
	start := time.Now()
	a, st, err := aof.Open(s.cfg.Dir, s.cfg.Fsync, func(args [][]byte) error {
		cmd := s.lookup(args[0])
		if cmd == nil {
			return fmt.Errorf("unknown command %q", args[0])
		}
		if (cmd.arity > 0 && len(args) != cmd.arity) || len(args) < -cmd.arity {
			return fmt.Errorf("wrong number of arguments for %q", cmd.name)
		}
		cmd.handler(rc, args)
		return nil
	})
	s.store.SetLoading(false)
	if err != nil {
		return err
	}
	s.aof = a
	s.logf("aof: replayed %d commands in %v (%d keys)", st.Commands, time.Since(start).Round(time.Millisecond), s.store.Len())
	if st.Truncated > 0 {
		s.logf("aof: WARNING cut %d bytes of incomplete last record (crash during a write?)", st.Truncated)
	}
	return nil
}

func (s *Server) logf(format string, args ...any) { s.log.Printf(format, args...) }

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// Addr returns the address the server listens on, or nil before Serve.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve accepts connections on ln until Shutdown. Each connection is served by
// its own goroutine: Go's scheduler multiplexes them over the CPU cores, and a
// blocked Read only parks that goroutine.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = ln.Close()
		return ErrServerClosed
	}
	s.ln = ln
	s.mu.Unlock()

	s.bg.Add(1)
	go s.expireLoop()

	var backoff time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			if s.isClosing() {
				return ErrServerClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() { // e.g. out of file descriptors: back off and retry
				backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
				s.logf("accept: %v; retrying in %v", err, backoff)
				time.Sleep(backoff)
				continue
			}
			return err
		}
		backoff = 0
		s.wg.Add(1)
		go s.serveConn(nc)
	}
}

func (s *Server) serveConn(nc net.Conn) {
	defer s.wg.Done()
	c := newClient(s, nc)

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = nc.Close()
		return
	}
	s.conns[c] = struct{}{}
	s.mu.Unlock()
	s.stats.connections.Add(1)
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
	}()
	defer c.close()

	for {
		args, err := c.r.ReadCommand()
		if err != nil {
			c.handleReadError(err)
			return
		}
		c.wmu.Lock()
		s.execute(c, args)
		quit := c.quit
		c.wmu.Unlock()
		if quit {
			return // close() flushes the pending replies
		}
	}
}

// expireLoop runs the active expiration cycle ten times per second, using at
// most a quarter of each tick (the same budget Redis gives its slow cycle).
func (s *Server) expireLoop() {
	defer s.bg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.store.ExpireCycle(25 * time.Millisecond)
		}
	}
}

// Shutdown stops accepting connections, lets every command already received
// finish and flush its reply, then closes the connections and the AOF (with a
// final fsync). If ctx expires first, remaining connections are closed hard.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	ln := s.ln
	conns := make([]*Client, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	close(s.stop)
	s.bg.Wait()

	// A read deadline in the past wakes up connections blocked waiting for a
	// command; their goroutines flush pending replies and exit.
	for _, c := range conns {
		_ = c.conn.SetReadDeadline(time.Now())
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		for _, c := range conns {
			c.kill()
		}
		<-done
	}
	if s.aof != nil {
		return s.aof.Close()
	}
	return nil
}
