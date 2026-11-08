package store

// List is a double-ended queue of byte strings backed by a ring buffer. Push
// and pop at either end are O(1) amortised and indexed access is O(1).
//
// It is not safe for concurrent use: the shard lock that owns the key protects
// it. The zero value is an empty list ready to use.
type List struct {
	buf  [][]byte
	head int // index in buf of the first element
	n    int // number of elements
}

// Len returns the number of elements.
func (l *List) Len() int { return l.n }

func (l *List) grow() {
	newCap := max(4, len(l.buf)*2)
	nb := make([][]byte, newCap)
	for i := 0; i < l.n; i++ {
		nb[i] = l.buf[(l.head+i)%len(l.buf)]
	}
	l.buf, l.head = nb, 0
}

// PushBack appends v at the tail.
func (l *List) PushBack(v []byte) {
	if l.n == len(l.buf) {
		l.grow()
	}
	l.buf[(l.head+l.n)%len(l.buf)] = v
	l.n++
}

// PushFront inserts v at the head.
func (l *List) PushFront(v []byte) {
	if l.n == len(l.buf) {
		l.grow()
	}
	l.head = (l.head - 1 + len(l.buf)) % len(l.buf)
	l.buf[l.head] = v
	l.n++
}

// PopFront removes and returns the first element.
func (l *List) PopFront() ([]byte, bool) {
	if l.n == 0 {
		return nil, false
	}
	v := l.buf[l.head]
	l.buf[l.head] = nil // let the GC reclaim the value
	l.head = (l.head + 1) % len(l.buf)
	l.n--
	return v, true
}

// PopBack removes and returns the last element.
func (l *List) PopBack() ([]byte, bool) {
	if l.n == 0 {
		return nil, false
	}
	i := (l.head + l.n - 1) % len(l.buf)
	v := l.buf[i]
	l.buf[i] = nil
	l.n--
	return v, true
}

// At returns the element at position i (0 is the head). i must be in [0, Len()).
func (l *List) At(i int) []byte { return l.buf[(l.head+i)%len(l.buf)] }

// Slice returns the elements from start to stop, both inclusive, following the
// index rules of LRANGE: negative indexes count from the tail (-1 is the last
// element), out of range indexes are clamped and an empty range yields nil.
// The result is a new slice; the elements themselves are shared and immutable.
func (l *List) Slice(start, stop int) [][]byte {
	n := l.n
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if start > stop || start >= n {
		return nil
	}
	if stop >= n {
		stop = n - 1
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, l.At(i))
	}
	return out
}
