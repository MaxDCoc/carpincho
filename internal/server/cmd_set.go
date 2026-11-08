package server

import "github.com/MaxDCoc/carpincho/internal/store"

func registerSet(r registry) {
	r.add("sadd", -3, cmdSadd)
	r.add("srem", -3, cmdSrem)
	r.add("smembers", 2, cmdSmembers)
	r.add("sismember", 3, cmdSismember)
	r.add("scard", 2, cmdScard)
}

func cmdSadd(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	e, wrong := lookupKind(&a, key, store.KindSet)
	if wrong {
		a.Release()
		c.w.Error(errWrongType)
		return
	}
	if e == nil {
		e = store.NewSet()
		a.Put(key, e)
	}
	added := 0
	for _, m := range args[2:] {
		if _, ok := e.Set[string(m)]; !ok {
			e.Set[string(m)] = struct{}{}
			added++
		}
	}
	c.propagate(args...)
	a.Release()
	c.w.Int(int64(added))
}

func cmdSrem(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	e, wrong := lookupKind(&a, key, store.KindSet)
	if wrong {
		a.Release()
		c.w.Error(errWrongType)
		return
	}
	n := 0
	if e != nil {
		for _, m := range args[2:] {
			if _, ok := e.Set[string(m)]; ok {
				delete(e.Set, string(m))
				n++
			}
		}
		if len(e.Set) == 0 {
			a.Delete(key)
		}
		if n > 0 {
			c.propagate(args...)
		}
	}
	a.Release()
	c.w.Int(int64(n))
}

func cmdSmembers(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindSet)
	var members [][]byte
	if e != nil {
		members = make([][]byte, 0, len(e.Set))
		for m := range e.Set {
			members = append(members, []byte(m))
		}
	}
	a.Release()
	if wrong {
		c.w.Error(errWrongType)
		return
	}
	c.w.BulkArray(members)
}

func cmdSismember(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindSet)
	var ok bool
	if e != nil {
		_, ok = e.Set[string(args[2])]
	}
	a.Release()
	switch {
	case wrong:
		c.w.Error(errWrongType)
	case ok:
		c.w.Int(1)
	default:
		c.w.Int(0)
	}
}

func cmdScard(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindSet)
	n := 0
	if e != nil {
		n = len(e.Set)
	}
	a.Release()
	if wrong {
		c.w.Error(errWrongType)
		return
	}
	c.w.Int(int64(n))
}
