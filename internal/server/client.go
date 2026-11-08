package server

import (
	"net"
	"sync"

	"github.com/MaxDCoc/carpincho/internal/resp"
)

// pushQueue is how many pub/sub frames may wait for a slow subscriber before
// it is disconnected (Redis has an equivalent output-buffer limit for pubsub).
const pushQueue = 4096

// Client is the state of one connection. Its command loop runs in a single
// goroutine (serveConn); the only other goroutines that touch it are the
// pub/sub publishers (through push) and the push writer.
type Client struct {
	id   int64
	srv  *Server
	conn net.Conn // nil in unit tests
	r    *resp.Reader
	w    *resp.Writer
	wmu  sync.Mutex // serialises writes to w: the command loop vs. the push writer
	name string

	subs   map[string]struct{} // subscribed channels; owned by the command loop
	pushCh chan []byte         // frames queued by publishers for the push writer
	done   chan struct{}

	quit      bool
	replay    bool   // replaying the AOF: no propagation, replies discarded
	aofSeq    uint64 // AOF sequence of the last record this client appended
	scratch   []byte
	closeOnce sync.Once
}

func newClient(s *Server, nc net.Conn) *Client {
	c := &Client{id: s.nextID.Add(1), srv: s, conn: nc, done: make(chan struct{})}
	if nc != nil {
		c.r = resp.NewReader(flushingReader{c})
		c.w = resp.NewWriter(nc)
	}
	return c
}

// flushingReader delivers replies exactly when the server is about to wait for
// the client. bufio calls Read only when its buffer is empty, so a pipeline of
// N commands that arrived together is answered with one write, while a lone
// command is answered immediately. This is the same batching Redis does in its
// beforeSleep hook.
type flushingReader struct{ c *Client }

func (f flushingReader) Read(p []byte) (int, error) {
	f.c.flushPending()
	return f.c.conn.Read(p)
}

// flushPending makes the AOF records of this client's commands durable per the
// fsync policy and only then sends the buffered replies: a client never sees
// "OK" for a write that is not yet in the log.
func (c *Client) flushPending() {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.aofSeq != 0 && c.srv.aof != nil {
		if err := c.srv.aof.Commit(c.aofSeq); err != nil {
			c.srv.logf("aof: commit failed: %v", err)
		}
		c.aofSeq = 0
	}
	if c.w != nil {
		_ = c.w.Flush()
	}
}

// propagate appends a write command to the AOF. Call it while the command still
// holds the shard locks of the keys it changed: that makes the order of the
// records of any key equal to the order in which the changes were applied.
func (c *Client) propagate(args ...[]byte) {
	if c.replay || c.srv.aof == nil {
		return
	}
	c.scratch = resp.AppendCommand(c.scratch[:0], args)
	c.aofSeq = c.srv.aof.Append(c.scratch)
}

func (c *Client) subscribed() bool { return len(c.subs) > 0 }

// handleReadError reacts to the error that ended the command loop.
func (c *Client) handleReadError(err error) {
	if pe, ok := err.(*resp.ProtocolError); ok {
		c.wmu.Lock()
		c.w.Error("ERR " + pe.Error())
		c.wmu.Unlock()
	}
	// EOF, closed connection, shutdown deadline: nothing to tell the client.
}

// push queues a pub/sub frame. It never blocks the publisher: a subscriber that
// does not keep up is disconnected instead of stalling everybody else.
func (c *Client) push(frame []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.pushCh <- frame:
		return true
	default:
		c.srv.logf("disconnecting slow subscriber %d: %d frames queued", c.id, pushQueue)
		c.kill()
		return false
	}
}

// pushLoop writes queued pub/sub frames to the connection, batching whatever
// is already waiting into a single flush.
func (c *Client) pushLoop() {
	for {
		select {
		case <-c.done:
			return
		case f := <-c.pushCh:
			c.wmu.Lock()
			c.w.Raw(f)
		drain:
			for {
				select {
				case f = <-c.pushCh:
					c.w.Raw(f)
				default:
					break drain
				}
			}
			err := c.w.Flush()
			c.wmu.Unlock()
			if err != nil {
				c.kill()
				return
			}
		}
	}
}

// kill closes the socket; the command loop notices and runs close().
func (c *Client) kill() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// close releases everything the client owns. It is idempotent.
func (c *Client) close() {
	c.closeOnce.Do(func() {
		c.flushPending()
		c.srv.hub.unsubscribeAll(c)
		close(c.done)
		c.kill()
	})
}
