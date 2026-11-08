# ADR 0001: Concurrency model, sharded locks instead of a single event loop

- **Status:** accepted
- **Date:** 2026-11-08
- **Deciders:** project author

## Context

A key-value server has to execute commands from many clients against shared data. Redis does this with **one thread and an event loop**: every command runs to completion, one after another, so there are no data races and no locks, and `MULTI/EXEC`, Lua scripts and ordering guarantees come for free. Its scalability limit is one core per instance.

carpincho is written in Go, where the natural unit of concurrency is the goroutine, the runtime already multiplexes many blocked goroutines over few OS threads, and the standard library gives us `sync.RWMutex`. We need to decide how connection goroutines coordinate access to the keyspace, and the decision must keep `go test -race ./...` green.

Requirements that shape it:

1. Commands on **different keys must not serialise** more than necessary (we want to use more than one core).
2. **Multi-key commands** (`MSET`, `DEL a b c`, `MGET`) must be atomic with respect to each other and must not deadlock.
3. The append-only file must record the writes to any given key **in the order they were applied**.
4. **No network or disk I/O may happen while a lock is held**, otherwise one stalled client could freeze part of the keyspace.

## Options considered

### A. Single executor goroutine (event loop, like Redis)

Connection goroutines parse commands and send them over a channel to one goroutine that executes them in order and sends the reply back.

- (+) No locks, trivially correct ordering, `MULTI/EXEC` is easy, AOF order equals execution order by construction.
- (-) Every command pays two channel hand-offs (request and reply), roughly 200 to 500 ns of latency plus scheduler wakeups, which is comparable to the cost of the command itself (a `GET` costs ~180 ns in carpincho).
- (-) Throughput is capped by one core, no matter how many we have.

### B. One global `sync.RWMutex` around one map

- (+) Smallest possible implementation.
- (-) Every write blocks every read; with many cores even the read lock is a contended atomic counter. Measured below: it collapses under parallel load.

### C. Sharded map, one `RWMutex` per shard (chosen)

Keys are spread over 256 shards by a seeded hash (`hash/maphash`). A command locks only the shards of the keys it touches.

- (+) Different keys mostly hit different locks, reads of the same shard still run in parallel.
- (+) Multi-key commands are possible by locking several shards in a fixed order.
- (-) More code to get right: lock ordering, and cross-key transactions need extra machinery.

### D. `sync.Map` or lock-free structures

Rejected: `sync.Map` is tuned for read-mostly, write-once workloads; our mix includes frequent overwrites and we need compound operations (read-modify-write under one lock: `INCR`, `LPUSH`, `SET NX`) that it cannot express.

## Decision

Use **option C**: 256 shards, each a `{RWMutex, map[string]*Entry, map[string]int64 (TTLs)}` padded to a 64-byte cache line.

The rules that keep it correct (all enforced in `internal/store` and followed by every handler in `internal/server`):

1. **Access goes through `Store.Acquire(write, keys...)`**, which returns an `Access` holding the locks. Mutating methods panic if the `Access` was acquired for reading, so a write under a read lock fails loudly in tests instead of becoming a rare race.
2. **Lock order is ascending shard index, each shard once.** `MSET a b` and `MSET b a` therefore cannot deadlock (`TestMultiKeyAcquireLocksInOrderAndDoesNotDeadlock`). Commands that need every shard (`KEYS`, `FLUSHALL`, the AOF rewrite) lock them in the same order.
3. **Lock hierarchy:** shard locks, then the AOF mutex, never the other way around. `Append` runs while the shard lock is held, which is what makes the log order of any key equal its apply order. `Commit` (the slow part: `write(2)` and `fsync`) runs after the locks are released.
4. **No I/O under a shard lock.** Handlers copy what they need (slice headers, not bytes), release the `Access`, and only then write to the socket.
5. **String values are immutable once stored.** Writers replace the slice or `append` beyond its length; nobody modifies bytes in place. That is what makes "copy the slice header, release the lock, use it later" safe, and it avoids copying values on every read.
6. **Expired keys:** under a write `Access` a lazily expired key is deleted; under a read `Access` it is only hidden, because a read lock may not mutate maps. The active expiry cycle takes each shard's write lock for at most one 20-key sample at a time.

## Evidence

`go test -bench ParallelGetSet ./internal/store` (Windows 11, Ryzen 7 7735H, 16 threads; 100 000 keys; 90% reads / 10% writes from 16 goroutines):

| Design | ns/op |
|--------|------:|
| 256 shards, one `RWMutex` each | 29.5 |
| one global `RWMutex` | 346.0 |

About 11.7x. The absolute numbers are noisy (other programs were running) but the order of magnitude is the argument.

The command layer on top (`BenchmarkExecute`, parse-free and socket-free) costs 143 to 318 ns per command aggregated over 16 goroutines, i.e. several million commands per second, so a channel hand-off per command (option A) would have been a significant fraction of the whole cost.

## Consequences

**Good**

- Scales with cores for workloads that touch many keys, and stays correct under `-race` stress tests (50 concurrent clients, 8 writers against a running rewrite).
- Multi-key commands (`MSET`, `MGET`, `DEL`, `EXISTS`) are atomic.
- Failure modes are local: a bug in one command can only corrupt what its `Access` covers.

**Bad / to remember**

- **Hot keys do not scale**: all clients hitting one key serialise on one shard, same as Redis.
- **`MULTI/EXEC` is not free.** A transaction must hold all the shards of all its keys for the whole block, but the keys of queued commands are not known up front in general. See [issue 04](../issues/04-multi-exec-watch.md) for the two candidate designs (a global transaction lock, or declaring keys at `EXEC` time by scanning the queue).
- **Global operations** (`KEYS`, `FLUSHALL`, the AOF rewrite) take all 256 locks and stall writers while they run.
- **`DBSIZE`/`Len` are not atomic snapshots**: they lock one shard at a time.
- The design relies on discipline (rules 1 to 5). The tests that protect it are the multi-key deadlock test, `TestConcurrentMixedWorkloadIsRaceFree`, the go-redis concurrency test and the rewrite-under-writers test; run them with `go test -race`.

## Revisit when

- we add `MULTI/EXEC` or Lua-style scripting (consider a per-instance "transaction mode" lock);
- profiles show shard contention on realistic workloads (increase the shard count, or move hot counters out of the map);
- we add eviction ([issue 03](../issues/03-maxmemory-lru-lfu-eviction.md)), which needs a global memory counter (atomic) and per-entry access metadata.
