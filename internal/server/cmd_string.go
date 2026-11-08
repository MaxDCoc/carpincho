package server

import (
	"math"
	"strings"

	"github.com/MaxDCoc/carpincho/internal/store"
)

func registerString(r registry) {
	r.add("get", 2, cmdGet)
	r.add("set", -3, cmdSet)
	r.add("mget", -2, cmdMget)
	r.add("mset", -3, cmdMset)
	r.add("incr", 2, func(c *Client, args [][]byte) { incrBy(c, args, 1) })
	r.add("decr", 2, func(c *Client, args [][]byte) { incrBy(c, args, -1) })
	r.add("incrby", 3, func(c *Client, args [][]byte) { incrByArg(c, args, false) })
	r.add("decrby", 3, func(c *Client, args [][]byte) { incrByArg(c, args, true) })
	r.add("append", 3, cmdAppend)
	r.add("strlen", 2, cmdStrlen)
}

func cmdGet(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindString)
	var val []byte
	if e != nil {
		val = e.Str // strings are immutable once stored, so it is safe to use after Release
	}
	a.Release()
	switch {
	case wrong:
		c.w.Error(errWrongType)
	case e == nil:
		c.w.Null()
	default:
		c.w.Bulk(val)
	}
}

// cmdSet implements SET key value [NX|XX] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL].
// In the AOF it is logged without NX/XX (the decision was already taken) and
// with any expiry converted to an absolute PXAT.
func cmdSet(c *Client, args [][]byte) {
	var nx, xx, keepTTL bool
	var mode string
	var amount int64
	for i := 3; i < len(args); i++ {
		switch {
		case eqFold(args[i], "NX"):
			nx = true
		case eqFold(args[i], "XX"):
			xx = true
		case eqFold(args[i], "KEEPTTL"):
			keepTTL = true
		case eqFold(args[i], "EX"), eqFold(args[i], "PX"), eqFold(args[i], "EXAT"), eqFold(args[i], "PXAT"):
			if mode != "" || i+1 >= len(args) {
				c.w.Error(errSyntax)
				return
			}
			n, ok := parseInt(args[i+1])
			if !ok {
				c.w.Error(errNotInt)
				return
			}
			mode, amount = strings.ToUpper(string(args[i])), n
			i++
		default:
			c.w.Error(errSyntax)
			return
		}
	}
	if (nx && xx) || (keepTTL && mode != "") {
		c.w.Error(errSyntax)
		return
	}
	const invalid = "ERR invalid expire time in 'set' command"
	var ms int64
	if mode != "" {
		unit := int64(1000)
		if mode == "PX" || mode == "PXAT" {
			unit = 1
		}
		if amount <= 0 || amount > math.MaxInt64/unit {
			c.w.Error(invalid)
			return
		}
		ms = amount * unit
	}

	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	cur := a.Get(key)
	if (nx && cur != nil) || (xx && cur == nil) {
		a.Release()
		c.w.Null()
		return
	}
	var deadline int64
	if mode != "" {
		deadline = ms
		if mode == "EX" || mode == "PX" {
			deadline = a.Now() + ms
			if deadline < ms { // overflow
				a.Release()
				c.w.Error(invalid)
				return
			}
		}
	}
	var kept int64
	var hadTTL bool
	if keepTTL && cur != nil {
		kept, hadTTL = a.ExpireAt(key)
	}
	a.Put(key, store.NewString(args[2])) // args[2] was allocated by the parser for this command: no copy needed
	switch {
	case deadline != 0:
		a.SetExpireAt(key, deadline)
		c.propagate([]byte("SET"), args[1], args[2], []byte("PXAT"), itoa(deadline))
	case hadTTL:
		a.SetExpireAt(key, kept)
		c.propagate([]byte("SET"), args[1], args[2], []byte("KEEPTTL"))
	default:
		c.propagate([]byte("SET"), args[1], args[2])
	}
	a.Release()
	c.w.SimpleString("OK")
}

func cmdMget(c *Client, args [][]byte) {
	keys := make([]string, len(args)-1)
	for i, k := range args[1:] {
		keys[i] = string(k)
	}
	vals := make([][]byte, len(keys))
	a := c.srv.store.Acquire(false, keys...)
	for i, k := range keys {
		if e := a.Get(k); e != nil && e.Kind == store.KindString {
			vals[i] = e.Str
		}
	}
	a.Release()
	c.w.ArrayHeader(len(vals))
	for _, v := range vals {
		if v == nil {
			c.w.Null()
		} else {
			c.w.Bulk(v)
		}
	}
}

func cmdMset(c *Client, args [][]byte) {
	if len(args)%2 != 1 {
		c.w.Error(wrongArity("mset"))
		return
	}
	keys := make([]string, 0, len(args)/2)
	for i := 1; i < len(args); i += 2 {
		keys = append(keys, string(args[i]))
	}
	a := c.srv.store.Acquire(true, keys...) // all shards are locked at once: MSET is atomic
	for i, k := range keys {
		a.Put(k, store.NewString(args[2+2*i]))
	}
	c.propagate(args...)
	a.Release()
	c.w.SimpleString("OK")
}

func incrByArg(c *Client, args [][]byte, negate bool) {
	d, ok := parseInt(args[2])
	if !ok {
		c.w.Error(errNotInt)
		return
	}
	if negate {
		if d == math.MinInt64 {
			c.w.Error(errOverflow)
			return
		}
		d = -d
	}
	incrBy(c, args, d)
}

func incrBy(c *Client, args [][]byte, delta int64) {
	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	e, wrong := lookupKind(&a, key, store.KindString)
	var cur int64
	if wrong {
		a.Release()
		c.w.Error(errWrongType)
		return
	}
	if e != nil {
		v, ok := parseInt(e.Str)
		if !ok {
			a.Release()
			c.w.Error(errNotInt)
			return
		}
		cur = v
	}
	if (delta < 0 && cur < math.MinInt64-delta) || (delta > 0 && cur > math.MaxInt64-delta) {
		a.Release()
		c.w.Error(errOverflow)
		return
	}
	cur += delta
	if e != nil {
		e.Str = itoa(cur) // replace, never modify in place: readers may hold the old slice
	} else {
		a.Put(key, store.NewString(itoa(cur)))
	}
	c.propagate(args...)
	a.Release()
	c.w.Int(cur)
}

func cmdAppend(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	e, wrong := lookupKind(&a, key, store.KindString)
	if wrong {
		a.Release()
		c.w.Error(errWrongType)
		return
	}
	var n int
	if e == nil {
		a.Put(key, store.NewString(args[2]))
		n = len(args[2])
	} else {
		// append writes only beyond the current length, so a reader that still
		// holds the old slice never sees bytes change under its feet.
		e.Str = append(e.Str, args[2]...)
		n = len(e.Str)
	}
	c.propagate(args...)
	a.Release()
	c.w.Int(int64(n))
}

func cmdStrlen(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindString)
	n := 0
	if e != nil {
		n = len(e.Str)
	}
	a.Release()
	if wrong {
		c.w.Error(errWrongType)
		return
	}
	c.w.Int(int64(n))
}
