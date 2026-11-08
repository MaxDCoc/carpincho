// Package store is the in-memory keyspace of carpincho: a sharded hash map with
// one reader/writer lock per shard, per-key expiry, and the four value types
// (string, list, hash, set).
//
// Concurrency model (see docs/adr/0001-concurrency-model.md):
//
//   - keys are spread over 256 shards by a seeded hash;
//   - a command takes the locks of the shards it touches through Acquire,
//     always in ascending shard order, so multi-key commands cannot deadlock;
//   - no I/O is ever done while a shard lock is held;
//   - string values are immutable once stored (writers replace or append, never
//     modify bytes in place), so a reader may keep using a value after Release.
package store

import (
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Kind is the type of the value stored under a key.
type Kind uint8

// Value kinds.
const (
	KindString Kind = iota + 1
	KindList
	KindHash
	KindSet
)

// String returns the name TYPE reports for the kind.
func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindHash:
		return "hash"
	case KindSet:
		return "set"
	}
	return "none"
}

// Entry is a stored value. Exactly one of the data fields is meaningful,
// selected by Kind.
type Entry struct {
	Kind Kind
	Str  []byte
	List *List
	Hash map[string][]byte
	Set  map[string]struct{}
}

// NewString returns a string entry holding b (b is not copied).
func NewString(b []byte) *Entry { return &Entry{Kind: KindString, Str: b} }

// NewList returns an empty list entry.
func NewList() *Entry { return &Entry{Kind: KindList, List: &List{}} }

// NewHash returns an empty hash entry.
func NewHash() *Entry { return &Entry{Kind: KindHash, Hash: make(map[string][]byte)} }

// NewSet returns an empty set entry.
func NewSet() *Entry { return &Entry{Kind: KindSet, Set: make(map[string]struct{})} }

const (
	numShards = 256 // power of two: shard index = hash & (numShards-1)

	// Active expiration (the algorithm of Redis' activeExpireCycle): sample up
	// to sampleSize keys that have a TTL; delete the expired ones; if more than
	// staleThresholdPercent of the sample was expired, sample again. The Redis
	// documentation says 25%; Redis 7's source defaults to 10%.
	sampleSize            = 20
	staleThresholdPercent = 25
)

// shard is one lock plus the maps it protects. expires holds the absolute
// deadline (unix milliseconds) of the keys that have a TTL, and only of those:
// that is what makes sampling "keys with a TTL" cheap.
type shard struct {
	mu      sync.RWMutex
	data    map[string]*Entry
	expires map[string]int64
	_       [24]byte // pad to 64 bytes, one cache line, so neighbours do not false-share
}

// Store is the sharded keyspace. Create it with New.
type Store struct {
	shards  [numShards]shard
	seed    maphash.Seed
	clock   func() int64 // unix milliseconds; replaceable in tests
	loading atomic.Bool  // true while the AOF is replayed: expiry is suspended
	cursor  atomic.Uint32
}

// New creates an empty Store. clock returns the current time in unix
// milliseconds; nil means the system clock.
func New(clock func() int64) *Store {
	if clock == nil {
		clock = func() int64 { return time.Now().UnixMilli() }
	}
	s := &Store{seed: maphash.MakeSeed(), clock: clock}
	for i := range s.shards {
		s.shards[i].data = make(map[string]*Entry)
		s.shards[i].expires = make(map[string]int64)
	}
	return s
}

// SetLoading suspends (true) or resumes (false) key expiry. While an AOF is
// replayed, "now" is later than when the commands originally ran, so keys with
// a short TTL would look expired too early and later commands would see a
// different state than they saw the first time. Redis does the same.
func (s *Store) SetLoading(v bool) { s.loading.Store(v) }

func (s *Store) index(key string) uint32 {
	return uint32(maphash.String(s.seed, key)) & (numShards - 1)
}

// Len returns the number of keys, including expired keys not yet collected
// (like Redis' DBSIZE). It locks one shard at a time, so the result is not an
// atomic snapshot under concurrent writes.
func (s *Store) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

// ExpiresLen returns the number of keys that carry a TTL.
func (s *Store) ExpiresLen() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.expires)
		sh.mu.RUnlock()
	}
	return n
}

// Access is a held set of shard locks. Obtain one with Acquire or AcquireAll
// and always Release it. Reads are allowed through any Access; mutations need
// an Access acquired with write=true (they panic otherwise, because a write
// under a read lock would be a data race).
type Access struct {
	st     *Store
	single *shard   // set when the Access covers exactly one key
	locked []*shard // otherwise: all locked shards, ascending index
	write  bool
	now    int64
}

func lockShard(sh *shard, write bool) {
	if write {
		sh.mu.Lock()
	} else {
		sh.mu.RLock()
	}
}

func unlockShard(sh *shard, write bool) {
	if write {
		sh.mu.Unlock()
	} else {
		sh.mu.RUnlock()
	}
}

// Acquire locks the shards that own keys. With several keys the shards are
// locked in ascending index order and each shard is locked once, so two
// commands such as MSET a b and MSET b a cannot deadlock. A single-key Access
// may only be used with that key.
func (s *Store) Acquire(write bool, keys ...string) Access {
	a := Access{st: s, write: write, now: s.clock()}
	switch len(keys) {
	case 0:
	case 1:
		a.single = &s.shards[s.index(keys[0])]
		lockShard(a.single, write)
	default:
		idx := make([]uint32, len(keys))
		for i, k := range keys {
			idx[i] = s.index(k)
		}
		slices.Sort(idx)
		idx = slices.Compact(idx)
		a.locked = make([]*shard, len(idx))
		for i, ix := range idx {
			sh := &s.shards[ix]
			lockShard(sh, write)
			a.locked[i] = sh
		}
	}
	return a
}

// AcquireAll locks every shard (KEYS, FLUSHALL, the AOF rewrite).
func (s *Store) AcquireAll(write bool) Access {
	a := Access{st: s, write: write, now: s.clock(), locked: make([]*shard, numShards)}
	for i := range s.shards {
		sh := &s.shards[i]
		lockShard(sh, write)
		a.locked[i] = sh
	}
	return a
}

// Release unlocks everything the Access holds.
func (a *Access) Release() {
	if a.single != nil {
		unlockShard(a.single, a.write)
		a.single = nil
		return
	}
	for i := len(a.locked) - 1; i >= 0; i-- {
		unlockShard(a.locked[i], a.write)
	}
	a.locked = nil
}

// Now returns the time (unix ms) sampled when the Access was acquired. All
// expiry decisions of a command use this single instant.
func (a *Access) Now() int64 { return a.now }

func (a *Access) shardOf(key string) *shard {
	if a.single != nil {
		return a.single
	}
	return &a.st.shards[a.st.index(key)]
}

func (a *Access) mustWrite() {
	if !a.write {
		panic("store: mutation through a read-only Access")
	}
}

// Get returns the entry for key, or nil if it does not exist or has expired.
// Under a write Access an expired key is deleted on the spot (lazy expiration);
// under a read Access it is only hidden and left for the next writer or for the
// active cycle, because a read lock cannot mutate the maps.
func (a *Access) Get(key string) *Entry {
	sh := a.shardOf(key)
	e := sh.data[key]
	if e == nil {
		return nil
	}
	if len(sh.expires) != 0 && !a.st.loading.Load() {
		if at, ok := sh.expires[key]; ok && at <= a.now {
			if a.write {
				delete(sh.data, key)
				delete(sh.expires, key)
			}
			return nil
		}
	}
	return e
}

// Put stores e under key, replacing any previous value and clearing its TTL.
func (a *Access) Put(key string, e *Entry) {
	a.mustWrite()
	sh := a.shardOf(key)
	sh.data[key] = e
	if len(sh.expires) != 0 {
		delete(sh.expires, key)
	}
}

// Delete removes key and reports whether it existed (and was not expired).
func (a *Access) Delete(key string) bool {
	a.mustWrite()
	if a.Get(key) == nil {
		return false
	}
	sh := a.shardOf(key)
	delete(sh.data, key)
	delete(sh.expires, key)
	return true
}

// ExpireAt returns the absolute deadline (unix ms) of key's TTL. Call Get first
// to know the key is alive.
func (a *Access) ExpireAt(key string) (int64, bool) {
	at, ok := a.shardOf(key).expires[key]
	return at, ok
}

// SetExpireAt sets key's absolute deadline (unix ms). It does nothing if the
// key does not exist.
func (a *Access) SetExpireAt(key string, at int64) {
	a.mustWrite()
	sh := a.shardOf(key)
	if _, ok := sh.data[key]; ok {
		sh.expires[key] = at
	}
}

// Persist removes key's TTL and reports whether it had one.
func (a *Access) Persist(key string) bool {
	a.mustWrite()
	sh := a.shardOf(key)
	if _, ok := sh.expires[key]; !ok {
		return false
	}
	delete(sh.expires, key)
	return true
}

// Range calls fn for every live key of an AcquireAll Access until fn returns
// false. expireAt is 0 for keys without TTL. Expired keys are skipped.
func (a *Access) Range(fn func(key string, e *Entry, expireAt int64) bool) {
	loading := a.st.loading.Load()
	for _, sh := range a.locked {
		for k, e := range sh.data {
			at, has := sh.expires[k]
			if has && at <= a.now && !loading {
				continue
			}
			if !fn(k, e, at) {
				return
			}
		}
	}
}

// Keys returns the live keys that match the glob pattern (AcquireAll Access).
func (a *Access) Keys(pattern string) []string {
	var out []string
	a.Range(func(k string, _ *Entry, _ int64) bool {
		if pattern == "*" || GlobMatch(pattern, k) {
			out = append(out, k)
		}
		return true
	})
	return out
}

// Flush deletes every key (write AcquireAll Access).
func (a *Access) Flush() {
	a.mustWrite()
	for _, sh := range a.locked {
		sh.data = make(map[string]*Entry)
		sh.expires = make(map[string]int64)
	}
}

// ExpireCycle runs one pass of active expiration and returns how many keys it
// deleted. It visits shards round-robin starting where the previous pass
// stopped. In each shard it samples up to 20 keys that have a TTL, deletes the
// expired ones and, while more than 25% of a sample was expired, samples the
// same shard again: a shard full of garbage is cleaned quickly, a healthy one
// costs a single small sample. The pass stops when budget is spent (always
// after at least one sample), saving its position for the next call, so it can
// never monopolise the CPU.
func (s *Store) ExpireCycle(budget time.Duration) (deleted int) {
	if s.loading.Load() {
		return 0
	}
	deadline := time.Now().Add(budget)
	start := s.cursor.Load()
	for i := uint32(0); i < numShards; i++ {
		idx := (start + i) % numShards
		sh := &s.shards[idx]

		sh.mu.RLock()
		volatile := len(sh.expires)
		sh.mu.RUnlock()
		if volatile == 0 {
			continue
		}

		for {
			sampled, expired := s.sample(sh)
			deleted += expired
			if sampled == 0 || expired*100 <= sampled*staleThresholdPercent {
				break
			}
			if time.Now().After(deadline) {
				s.cursor.Store(idx) // still stale: continue here next time
				return deleted
			}
		}
		if time.Now().After(deadline) {
			s.cursor.Store((idx + 1) % numShards)
			return deleted
		}
	}
	s.cursor.Store(start)
	return deleted
}

// sample inspects up to sampleSize keys with a TTL in one shard. Ranging over a
// Go map starts at a random position, which gives a cheap pseudo-random sample.
func (s *Store) sample(sh *shard) (sampled, expired int) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	now := s.clock()
	for k, at := range sh.expires {
		sampled++
		if at <= now {
			delete(sh.expires, k)
			delete(sh.data, k)
			expired++
		}
		if sampled >= sampleSize {
			break
		}
	}
	return sampled, expired
}
