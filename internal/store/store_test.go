package store

import (
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() int64              { return c.ms.Load() }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

func newTestStore() (*Store, *fakeClock) {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000)
	return New(c.now), c
}

func put(s *Store, key, val string) {
	a := s.Acquire(true, key)
	a.Put(key, NewString([]byte(val)))
	a.Release()
}

func get(s *Store, key string) (string, bool) {
	a := s.Acquire(false, key)
	defer a.Release()
	e := a.Get(key)
	if e == nil {
		return "", false
	}
	return string(e.Str), true
}

func expireIn(s *Store, key string, d time.Duration) {
	a := s.Acquire(true, key)
	a.SetExpireAt(key, a.Now()+d.Milliseconds())
	a.Release()
}

// globalLockMap is the naive design the sharded Store replaces: one RWMutex in
// front of one map. It exists only so the benchmarks below can compare both.
type globalLockMap struct {
	mu sync.RWMutex
	m  map[string]*Entry
}

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	return keys
}

// BenchmarkParallelGetSet runs the same 90% read / 10% write mix from every CPU
// against the sharded store and against a single global RWMutex.
func BenchmarkParallelGetSet(b *testing.B) {
	keys := benchKeys(100_000)
	val := NewString([]byte("value"))

	b.Run("sharded-256", func(b *testing.B) {
		s := New(nil)
		for _, k := range keys {
			a := s.Acquire(true, k)
			a.Put(k, val)
			a.Release()
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%len(keys)]
				if i%10 == 0 {
					a := s.Acquire(true, k)
					a.Put(k, val)
					a.Release()
				} else {
					a := s.Acquire(false, k)
					a.Get(k)
					a.Release()
				}
				i++
			}
		})
	})

	b.Run("single-RWMutex", func(b *testing.B) {
		g := &globalLockMap{m: make(map[string]*Entry, len(keys))}
		for _, k := range keys {
			g.m[k] = val
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keys[i%len(keys)]
				if i%10 == 0 {
					g.mu.Lock()
					g.m[k] = val
					g.mu.Unlock()
				} else {
					g.mu.RLock()
					_ = g.m[k]
					g.mu.RUnlock()
				}
				i++
			}
		})
	})
}

func TestPutGetDelete(t *testing.T) {
	s, _ := newTestStore()
	if _, ok := get(s, "k"); ok {
		t.Fatal("missing key must not be found")
	}
	put(s, "k", "v")
	if v, ok := get(s, "k"); !ok || v != "v" {
		t.Fatalf("got %q %v", v, ok)
	}
	a := s.Acquire(true, "k")
	if !a.Delete("k") || a.Delete("k") {
		t.Fatal("Delete must report whether the key existed")
	}
	a.Release()
	if s.Len() != 0 {
		t.Fatalf("Len = %d", s.Len())
	}
}

func TestMutationUnderReadAccessPanics(t *testing.T) {
	s, _ := newTestStore()
	a := s.Acquire(false, "k")
	defer a.Release()
	defer func() {
		if recover() == nil {
			t.Fatal("Put under a read access must panic: it would be a data race")
		}
	}()
	a.Put("k", NewString(nil))
}

func TestLazyExpiryDeletesOnWriteAccess(t *testing.T) {
	s, clk := newTestStore()
	put(s, "k", "v")
	expireIn(s, "k", 100*time.Millisecond)

	clk.advance(99 * time.Millisecond)
	if _, ok := get(s, "k"); !ok {
		t.Fatal("key must still be alive 1ms before its deadline")
	}
	clk.advance(1 * time.Millisecond) // exactly at the deadline: expired
	a := s.Acquire(true, "k")
	if a.Get("k") != nil {
		t.Fatal("key must be expired at its deadline")
	}
	a.Release()
	if s.Len() != 0 || s.ExpiresLen() != 0 {
		t.Fatalf("expired key must be physically removed on write access (len=%d expires=%d)", s.Len(), s.ExpiresLen())
	}
}

func TestReadAccessHidesExpiredKeysButDoesNotMutate(t *testing.T) {
	s, clk := newTestStore()
	put(s, "k", "v")
	expireIn(s, "k", time.Second)
	clk.advance(2 * time.Second)
	if _, ok := get(s, "k"); ok {
		t.Fatal("expired key must be invisible")
	}
	if s.Len() != 1 {
		t.Fatal("a read lock must not mutate the maps; the active cycle will collect it")
	}
}

func TestPutClearsTTLPersistAndExpireAt(t *testing.T) {
	s, _ := newTestStore()
	put(s, "k", "v")
	expireIn(s, "k", time.Minute)
	a := s.Acquire(true, "k")
	if at, ok := a.ExpireAt("k"); !ok || at != a.Now()+60_000 {
		t.Fatalf("ExpireAt = %d %v", at, ok)
	}
	if !a.Persist("k") || a.Persist("k") {
		t.Fatal("Persist must report whether a TTL was removed")
	}
	a.SetExpireAt("k", a.Now()+5)
	a.Put("k", NewString([]byte("new")))
	if _, ok := a.ExpireAt("k"); ok {
		t.Fatal("overwriting a key must clear its TTL")
	}
	a.Release()
}

func TestLoadingFlagSuspendsExpiry(t *testing.T) {
	s, clk := newTestStore()
	put(s, "k", "v")
	expireIn(s, "k", time.Millisecond)
	clk.advance(time.Hour)
	s.SetLoading(true)
	if _, ok := get(s, "k"); !ok {
		t.Fatal("while replaying the AOF, keys must not expire: later commands may still depend on them")
	}
	s.SetLoading(false)
	if _, ok := get(s, "k"); ok {
		t.Fatal("after loading, normal expiry applies")
	}
}

func fillWithTTL(s *Store, expiring, persistent int, ttl time.Duration) {
	for i := 0; i < expiring; i++ {
		k := "exp:" + strconv.Itoa(i)
		put(s, k, "x")
		expireIn(s, k, ttl)
	}
	for i := 0; i < persistent; i++ {
		put(s, "keep:"+strconv.Itoa(i), "x")
	}
}

func TestExpireCycleCollectsExpiredKeysWithoutAnyAccess(t *testing.T) {
	s, clk := newTestStore()
	fillWithTTL(s, 50_000, 1_000, 100*time.Millisecond)
	clk.advance(101 * time.Millisecond)

	deleted := s.ExpireCycle(5 * time.Second)

	if deleted != 50_000 {
		t.Fatalf("deleted %d, want 50000", deleted)
	}
	if s.Len() != 1_000 || s.ExpiresLen() != 0 {
		t.Fatalf("len=%d expires=%d, want 1000/0: persistent keys must survive", s.Len(), s.ExpiresLen())
	}
}

func TestExpireCycleLeavesLiveKeysAlone(t *testing.T) {
	s, clk := newTestStore()
	fillWithTTL(s, 5_000, 0, time.Hour)
	clk.advance(time.Minute)
	if n := s.ExpireCycle(time.Second); n != 0 {
		t.Fatalf("deleted %d live keys", n)
	}
	if s.Len() != 5_000 {
		t.Fatalf("len = %d", s.Len())
	}
}

func TestExpireCycleHonoursItsTimeBudgetAndResumes(t *testing.T) {
	s, clk := newTestStore()
	fillWithTTL(s, 20_000, 0, time.Millisecond)
	clk.advance(time.Second)

	calls := 0
	for s.Len() > 0 {
		calls++
		if calls > 100_000 {
			t.Fatal("repeated tiny-budget cycles never finished: the cursor must advance between calls")
		}
		s.ExpireCycle(time.Nanosecond)
	}
	if calls < 2 {
		t.Fatalf("a 1ns budget should not be able to clear everything in one call (calls=%d)", calls)
	}
}

func TestAcquireAllRangeKeysAndFlush(t *testing.T) {
	s, clk := newTestStore()
	for i := 0; i < 100; i++ {
		put(s, "user:"+strconv.Itoa(i), "x")
	}
	put(s, "other", "x")
	put(s, "gone", "x")
	expireIn(s, "gone", time.Millisecond)
	clk.advance(time.Second)

	a := s.AcquireAll(false)
	keys := a.Keys("user:*")
	seen := 0
	a.Range(func(key string, e *Entry, expireAt int64) bool { seen++; return true })
	a.Release()
	if len(keys) != 100 {
		t.Fatalf("KEYS user:* = %d keys", len(keys))
	}
	if seen != 101 {
		t.Fatalf("Range visited %d entries, want 101 (expired ones are skipped)", seen)
	}

	w := s.AcquireAll(true)
	w.Flush()
	w.Release()
	if s.Len() != 0 {
		t.Fatal("Flush must empty the store")
	}
}

func distinctShardKeys(s *Store, n int) []string {
	var keys []string
	seen := map[uint32]bool{}
	for i := 0; len(keys) < n; i++ {
		k := "key" + strconv.Itoa(i)
		if ix := s.index(k); !seen[ix] {
			seen[ix] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// Two goroutines locking {a,b} and {b,a} would deadlock if shards were locked
// in argument order. They are locked in index order, so this must finish.
func TestMultiKeyAcquireLocksInOrderAndDoesNotDeadlock(t *testing.T) {
	s, _ := newTestStore()
	keys := distinctShardKeys(s, 2)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ks := []string{keys[0], keys[1]}
			if g%2 == 1 {
				ks = []string{keys[1], keys[0]}
			}
			for i := 0; i < 2_000; i++ {
				a := s.Acquire(true, ks...)
				a.Put(ks[0], NewString([]byte("x")))
				a.Put(ks[1], NewString([]byte("y")))
				a.Release()
			}
		}(g)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: shards must be locked in a global order")
	}
}

func TestSameShardKeysAreLockedOnce(t *testing.T) {
	s, _ := newTestStore()
	k1 := "a"
	var k2 string
	for i := 0; ; i++ {
		k2 = "b" + strconv.Itoa(i)
		if s.index(k2) == s.index(k1) {
			break
		}
	}
	a := s.Acquire(true, k1, k2, k1) // would self-deadlock if the shard were locked twice
	a.Put(k1, NewString([]byte("1")))
	a.Put(k2, NewString([]byte("2")))
	a.Release()
	if s.Len() != 2 {
		t.Fatalf("len = %d", s.Len())
	}
}

func TestConcurrentMixedWorkloadIsRaceFree(t *testing.T) {
	s := New(nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 3_000; i++ {
				k := "k" + strconv.Itoa(rng.Intn(50))
				switch rng.Intn(4) {
				case 0:
					put(s, k, "v")
				case 1:
					get(s, k)
				case 2:
					a := s.Acquire(true, k)
					a.Delete(k)
					a.Release()
				case 3:
					expireIn(s, k, time.Millisecond)
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			s.ExpireCycle(time.Millisecond)
			s.Len()
		}
	}()
	wg.Wait()
}

func TestListMatchesSliceModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var l List
	var model [][]byte
	for i := 0; i < 20_000; i++ {
		v := []byte(strconv.Itoa(i))
		switch rng.Intn(4) {
		case 0:
			l.PushBack(v)
			model = append(model, v)
		case 1:
			l.PushFront(v)
			model = append([][]byte{v}, model...)
		case 2:
			got, ok := l.PopFront()
			if len(model) == 0 {
				if ok {
					t.Fatal("PopFront on empty list returned a value")
				}
				break
			}
			if !ok || string(got) != string(model[0]) {
				t.Fatalf("PopFront = %q %v, want %q", got, ok, model[0])
			}
			model = model[1:]
		case 3:
			got, ok := l.PopBack()
			if len(model) == 0 {
				if ok {
					t.Fatal("PopBack on empty list returned a value")
				}
				break
			}
			want := model[len(model)-1]
			if !ok || string(got) != string(want) {
				t.Fatalf("PopBack = %q %v, want %q", got, ok, want)
			}
			model = model[:len(model)-1]
		}
		if l.Len() != len(model) {
			t.Fatalf("step %d: Len = %d, want %d", i, l.Len(), len(model))
		}
	}
	for i, want := range model {
		if string(l.At(i)) != string(want) {
			t.Fatalf("At(%d) = %q, want %q", i, l.At(i), want)
		}
	}
}

func TestListSliceFollowsRedisIndexRules(t *testing.T) {
	var l List
	for _, s := range []string{"0", "1", "2", "3", "4"} {
		l.PushBack([]byte(s))
	}
	tests := []struct {
		start, stop int
		want        []string
	}{
		{0, -1, []string{"0", "1", "2", "3", "4"}},
		{-3, -1, []string{"2", "3", "4"}},
		{1, 1, []string{"1"}},
		{3, 1, nil},
		{-100, 100, []string{"0", "1", "2", "3", "4"}},
		{5, 10, nil},
		{0, -6, nil},
		{2, -2, []string{"2", "3"}},
	}
	for _, tt := range tests {
		var got []string
		for _, b := range l.Slice(tt.start, tt.stop) {
			got = append(got, string(b))
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("Slice(%d,%d) = %q, want %q", tt.start, tt.stop, got, tt.want)
		}
	}
}
