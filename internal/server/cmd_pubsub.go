package server

import (
	"sync"

	"github.com/MaxDCoc/carpincho/internal/resp"
)

// hub routes published messages to subscribers. Its lock protects only the
// channel -> subscribers map; delivery happens outside the lock through each
// client's bounded queue.
type hub struct {
	mu    sync.RWMutex
	chans map[string]map[*Client]struct{}
}

func newHub() *hub { return &hub{chans: make(map[string]map[*Client]struct{})} }

func (h *hub) subscribe(ch string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.chans[ch] == nil {
		h.chans[ch] = make(map[*Client]struct{})
	}
	h.chans[ch][c] = struct{}{}
}

func (h *hub) unsubscribe(ch string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs := h.chans[ch]; subs != nil {
		delete(subs, c)
		if len(subs) == 0 {
			delete(h.chans, ch)
		}
	}
}

func (h *hub) unsubscribeAll(c *Client) {
	for ch := range c.subs {
		h.unsubscribe(ch, c)
	}
	c.subs = nil
}

// publish delivers payload to every subscriber of ch and returns how many
// accepted it. The frame is encoded once and shared by all of them.
func (h *hub) publish(ch string, payload []byte) int {
	h.mu.RLock()
	subs := h.chans[ch]
	if len(subs) == 0 {
		h.mu.RUnlock()
		return 0
	}
	targets := make([]*Client, 0, len(subs))
	for c := range subs {
		targets = append(targets, c)
	}
	h.mu.RUnlock()

	frame := resp.AppendArrayHeader(nil, 3)
	frame = resp.AppendBulkString(frame, "message")
	frame = resp.AppendBulkString(frame, ch)
	frame = resp.AppendBulk(frame, payload)
	n := 0
	for _, c := range targets {
		if c.push(frame) {
			n++
		}
	}
	return n
}

func registerPubSub(r registry) {
	r.add("subscribe", -2, cmdSubscribe).pubsubSafe = true
	r.add("unsubscribe", -1, cmdUnsubscribe).pubsubSafe = true
	r.add("publish", 3, func(c *Client, args [][]byte) {
		c.w.Int(int64(c.srv.hub.publish(string(args[1]), args[2])))
	})
}

func cmdSubscribe(c *Client, args [][]byte) {
	if c.pushCh == nil {
		// First subscription: start the goroutine that writes published
		// messages to this connection while the command loop is blocked reading.
		c.pushCh = make(chan []byte, pushQueue)
		go c.pushLoop()
	}
	for _, a := range args[1:] {
		ch := string(a)
		if _, already := c.subs[ch]; !already {
			if c.subs == nil {
				c.subs = make(map[string]struct{})
			}
			c.subs[ch] = struct{}{}
			c.srv.hub.subscribe(ch, c)
		}
		c.w.ArrayHeader(3)
		c.w.BulkString("subscribe")
		c.w.Bulk(a)
		c.w.Int(int64(len(c.subs)))
	}
}

func cmdUnsubscribe(c *Client, args [][]byte) {
	targets := args[1:]
	if len(targets) == 0 {
		if len(c.subs) == 0 {
			c.w.ArrayHeader(3)
			c.w.BulkString("unsubscribe")
			c.w.Null()
			c.w.Int(0)
			return
		}
		for ch := range c.subs {
			targets = append(targets, []byte(ch))
		}
	}
	for _, a := range targets {
		ch := string(a)
		if _, ok := c.subs[ch]; ok {
			delete(c.subs, ch)
			c.srv.hub.unsubscribe(ch, c)
		}
		c.w.ArrayHeader(3)
		c.w.BulkString("unsubscribe")
		c.w.Bulk(a)
		c.w.Int(int64(len(c.subs)))
	}
}
