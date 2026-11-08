package server

import "github.com/MaxDCoc/carpincho/internal/store"

func registerHash(r registry) {
	r.add("hset", -4, cmdHset)
	r.add("hget", 3, cmdHget)
	r.add("hdel", -3, cmdHdel)
	r.add("hgetall", 2, cmdHgetall)
	r.add("hlen", 2, cmdHlen)
}

func cmdHset(c *Client, args [][]byte) {
	if len(args)%2 != 0 { // key + field/value pairs
		c.w.Error(wrongArity("hset"))
		return
	}
	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	e, wrong := lookupKind(&a, key, store.KindHash)
	if wrong {
		a.Release()
		c.w.Error(errWrongType)
		return
	}
	if e == nil {
		e = store.NewHash()
		a.Put(key, e)
	}
	added := 0
	for i := 2; i < len(args); i += 2 {
		f := string(args[i])
		if _, exists := e.Hash[f]; !exists {
			added++
		}
		e.Hash[f] = args[i+1]
	}
	c.propagate(args...)
	a.Release()
	c.w.Int(int64(added))
}

func cmdHget(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindHash)
	var val []byte
	var found bool
	if e != nil {
		val, found = e.Hash[string(args[2])]
	}
	a.Release()
	switch {
	case wrong:
		c.w.Error(errWrongType)
	case !found:
		c.w.Null()
	default:
		c.w.Bulk(val)
	}
}

func cmdHdel(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(true, key)
	e, wrong := lookupKind(&a, key, store.KindHash)
	if wrong {
		a.Release()
		c.w.Error(errWrongType)
		return
	}
	n := 0
	if e != nil {
		for _, f := range args[2:] {
			if _, ok := e.Hash[string(f)]; ok {
				delete(e.Hash, string(f))
				n++
			}
		}
		if len(e.Hash) == 0 {
			a.Delete(key)
		}
		if n > 0 {
			c.propagate(args...)
		}
	}
	a.Release()
	c.w.Int(int64(n))
}

func cmdHgetall(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindHash)
	var flat [][]byte
	if e != nil {
		flat = make([][]byte, 0, 2*len(e.Hash))
		for f, v := range e.Hash {
			flat = append(flat, []byte(f), v)
		}
	}
	a.Release()
	if wrong {
		c.w.Error(errWrongType)
		return
	}
	c.w.BulkArray(flat)
}

func cmdHlen(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindHash)
	n := 0
	if e != nil {
		n = len(e.Hash)
	}
	a.Release()
	if wrong {
		c.w.Error(errWrongType)
		return
	}
	c.w.Int(int64(n))
}
