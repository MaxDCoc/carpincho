package server

import "github.com/MaxDCoc/carpincho/internal/store"

func registerList(r registry) {
	r.add("lpush", -3, pushCmd(true))
	r.add("rpush", -3, pushCmd(false))
	r.add("lpop", -2, popCmd("lpop", true))
	r.add("rpop", -2, popCmd("rpop", false))
	r.add("lrange", 4, cmdLrange)
	r.add("llen", 2, cmdLlen)
}

func pushCmd(front bool) handler {
	return func(c *Client, args [][]byte) {
		key := string(args[1])
		a := c.srv.store.Acquire(true, key)
		e, wrong := lookupKind(&a, key, store.KindList)
		if wrong {
			a.Release()
			c.w.Error(errWrongType)
			return
		}
		if e == nil {
			e = store.NewList()
			a.Put(key, e)
		}
		for _, v := range args[2:] { // LPUSH k a b c pushes a, then b, then c: the list reads c b a
			if front {
				e.List.PushFront(v)
			} else {
				e.List.PushBack(v)
			}
		}
		n := e.List.Len()
		c.propagate(args...)
		a.Release()
		c.w.Int(int64(n))
	}
}

func popCmd(name string, front bool) handler {
	return func(c *Client, args [][]byte) {
		if len(args) > 3 {
			c.w.Error(wrongArity(name))
			return
		}
		withCount := len(args) == 3
		count := int64(1)
		if withCount {
			n, ok := parseInt(args[2])
			if !ok || n < 0 {
				c.w.Error("ERR value is out of range, must be positive")
				return
			}
			count = n
		}
		key := string(args[1])
		a := c.srv.store.Acquire(true, key)
		e, wrong := lookupKind(&a, key, store.KindList)
		if wrong {
			a.Release()
			c.w.Error(errWrongType)
			return
		}
		if e == nil {
			a.Release()
			if withCount {
				c.w.NullArray()
			} else {
				c.w.Null()
			}
			return
		}
		var popped [][]byte
		for i := int64(0); i < count; i++ {
			var v []byte
			var ok bool
			if front {
				v, ok = e.List.PopFront()
			} else {
				v, ok = e.List.PopBack()
			}
			if !ok {
				break
			}
			popped = append(popped, v)
		}
		if e.List.Len() == 0 {
			a.Delete(key) // an empty list does not exist in Redis
		}
		c.propagate(args...)
		a.Release()
		if !withCount {
			c.w.Bulk(popped[0])
			return
		}
		c.w.BulkArray(popped)
	}
}

func cmdLrange(c *Client, args [][]byte) {
	start, ok1 := parseInt(args[2])
	stop, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		c.w.Error(errNotInt)
		return
	}
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindList)
	var items [][]byte
	if e != nil {
		items = e.List.Slice(clampInt(start), clampInt(stop))
	}
	a.Release()
	if wrong {
		c.w.Error(errWrongType)
		return
	}
	c.w.BulkArray(items)
}

// clampInt narrows an int64 index to int without wrapping on 32-bit platforms.
func clampInt(n int64) int {
	const maxInt = int(^uint(0) >> 1)
	if n > int64(maxInt) {
		return maxInt
	}
	if n < -int64(maxInt) {
		return -maxInt
	}
	return int(n)
}

func cmdLlen(c *Client, args [][]byte) {
	key := string(args[1])
	a := c.srv.store.Acquire(false, key)
	e, wrong := lookupKind(&a, key, store.KindList)
	n := 0
	if e != nil {
		n = e.List.Len()
	}
	a.Release()
	if wrong {
		c.w.Error(errWrongType)
		return
	}
	c.w.Int(int64(n))
}
