package server

import "math"

func registerKeys(r registry) {
	r.add("del", -2, cmdDel)
	r.add("unlink", -2, cmdDel)
	r.add("exists", -2, cmdExists)
	r.add("expire", -3, expireCmd("expire", 1000, false))
	r.add("pexpire", -3, expireCmd("pexpire", 1, false))
	r.add("expireat", -3, expireCmd("expireat", 1000, true))
	r.add("pexpireat", -3, expireCmd("pexpireat", 1, true))
	r.add("ttl", 2, ttlCmd(false))
	r.add("pttl", 2, ttlCmd(true))
	r.add("persist", 2, cmdPersist)
	r.add("type", 2, cmdType)
	r.add("keys", 2, cmdKeys)
	r.add("dbsize", 1, func(c *Client, _ [][]byte) { c.w.Int(int64(c.srv.store.Len())) })
	r.add("flushall", -1, cmdFlushAll)
	r.add("flushdb", -1, cmdFlushAll)
}

func cmdDel(c *Client, args [][]byte) {
	keys := make([]string, len(args)-1)
	for i, k := range args[1:] {
		keys[i] = string(k)
	}
	a := c.srv.store.Acquire(true, keys...)
	n := 0
	for _, k := range keys {
		if a.Delete(k) {
			n++
		}
	}
	if n > 0 {
		c.propagate(args...)
	}
	a.Release()
	c.w.Int(int64(n))
}

func cmdExists(c *Client, args [][]byte) {
	keys := make([]string, len(args)-1)
	for i, k := range args[1:] {
		keys[i] = string(k)
	}
	a := c.srv.store.Acquire(false, keys...)
	n := 0
	for _, k := range keys {
		if a.Get(k) != nil { // duplicates count once per occurrence, like Redis
			n++
		}
	}
	a.Release()
	c.w.Int(int64(n))
}

// expireCmd builds EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT. unit is how many
// milliseconds one unit of the argument is worth; absolute tells whether the
// argument is a point in time or a delay. A deadline in the past deletes the
// key, as in Redis. In the AOF every variant becomes PEXPIREAT with an absolute
// time, so replaying the log later does not restart the countdown.
func expireCmd(name string, unit int64, absolute bool) handler {
	invalid := "ERR invalid expire time in '" + name + "' command"
	return func(c *Client, args [][]byte) {
		if len(args) != 3 {
			c.w.Error(errSyntax) // NX/XX/GT/LT options are not implemented
			return
		}
		n, ok := parseInt(args[2])
		if !ok {
			c.w.Error(errNotInt)
			return
		}
		if n > math.MaxInt64/unit || n < math.MinInt64/unit {
			c.w.Error(invalid)
			return
		}
		ms := n * unit
		key := string(args[1])
		a := c.srv.store.Acquire(true, key)
		if a.Get(key) == nil {
			a.Release()
			c.w.Int(0)
			return
		}
		at := ms
		if !absolute {
			at = a.Now() + ms
			if ms > 0 && at < a.Now() { // overflow
				a.Release()
				c.w.Error(invalid)
				return
			}
		}
		if at <= a.Now() {
			a.Delete(key)
			c.propagate([]byte("DEL"), args[1])
		} else {
			a.SetExpireAt(key, at)
			c.propagate([]byte("PEXPIREAT"), args[1], itoa(at))
		}
		a.Release()
		c.w.Int(1)
	}
}

func ttlCmd(millis bool) handler {
	return func(c *Client, args [][]byte) {
		key := string(args[1])
		a := c.srv.store.Acquire(false, key)
		defer a.Release()
		if a.Get(key) == nil {
			c.w.Int(-2)
			return
		}
		at, ok := a.ExpireAt(key)
		if !ok {
			c.w.Int(-1)
			return
		}
		left := at - a.Now()
		if millis {
			c.w.Int(left)
		} else {
			c.w.Int((left + 500) / 1000) // round to the nearest second, like Redis
		}
	}
}

func cmdPersist(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	removed := a.Get(key) != nil && a.Persist(key)
	if removed {
		c.propagate(args...)
	}
	a.Release()
	if removed {
		c.w.Int(1)
	} else {
		c.w.Int(0)
	}
}

func cmdType(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e := a.Get(key)
	a.Release()
	if e == nil {
		c.w.SimpleString("none")
		return
	}
	c.w.SimpleString(e.Kind.String())
}

func cmdKeys(c *Client, args [][]byte) {
	a := c.srv.store.AcquireAll(false)
	keys := a.Keys(string(args[1]))
	a.Release()
	c.w.ArrayHeader(len(keys))
	for _, k := range keys {
		c.w.BulkString(k)
	}
}

func cmdFlushAll(c *Client, args [][]byte) {
	if len(args) > 2 || (len(args) == 2 && !eqFold(args[1], "ASYNC") && !eqFold(args[1], "SYNC")) {
		c.w.Error(errSyntax)
		return
	}
	a := c.srv.store.AcquireAll(true)
	a.Flush()
	c.propagate([]byte("FLUSHALL"))
	a.Release()
	c.w.SimpleString("OK")
}
