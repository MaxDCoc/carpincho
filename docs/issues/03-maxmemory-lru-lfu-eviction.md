# `maxmemory` with approximate LRU / LFU eviction

| | |
|---|---|
| **Difficulty** | Hard (4/5) |
| **Labels** | `enhancement`, `memory`, `algorithms` |
| **Estimate** | 3 to 5 days |
| **Depends on** | nothing (the `evicted` keyspace event needs the notifications issue) |

## Context

carpincho grows until the machine runs out of memory. A cache needs a ceiling and a policy for what to drop when it is reached. Redis does not keep an exact LRU list (too expensive); it **samples** a few random keys, evicts the best candidate, and keeps a small pool of good candidates between rounds. Approximate LRU with 5 samples is close to true LRU at a fraction of the cost. LFU keeps a small logarithmic counter of how often a key is used, with periodic decay.

Reference: <https://redis.io/docs/latest/develop/reference/eviction/>

## Acceptance criteria

- [ ] Flags / config: `--maxmemory <bytes|kb|mb|gb>` (0 = unlimited, the default) and `--maxmemory-policy` with at least `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-ttl`; readable and settable through `CONFIG GET` / `CONFIG SET` (add `CONFIG SET` for these two parameters).
- [ ] **Memory accounting**: an atomic counter maintained by the store (key bytes + value bytes + a documented constant overhead per entry and per collection element). It is an estimate on purpose; explain why `runtime.MemStats` is not used for decisions.
- [ ] `noeviction`: write commands that would add data fail with `-OOM command not allowed when used memory > 'maxmemory'.` while reads keep working.
- [ ] Eviction runs **before** a write command executes and loops until the counter is under the limit (or no candidate exists -> OOM error). It must not run while holding a shard lock (it takes its own).
- [ ] Per-entry metadata: LRU clock (seconds, 24 bits is enough) and LFU counter. **Careful**: reads happen under a *read* lock, so updating them is a concurrent write to shared memory. Use atomics (`atomic.Uint32`), never a plain field, or the race detector will (rightly) fail.
- [ ] Approximate LRU: sample `maxmemory-samples` (default 5) keys, keep a pool of 16 candidates sorted by idle time, evict the best. LFU: probabilistic increment (Morris counter, `lfu-log-factor`) and decay (`lfu-decay-time`).
- [ ] `INFO memory` (`used_memory`, `maxmemory`, `maxmemory_policy`) and `INFO stats` (`evicted_keys`).
- [ ] Tests with the fake clock: deterministic LRU case (sample = whole keyspace -> exact order); a statistical test on a Zipf-distributed workload showing the approximate LRU hit rate within a few percent of a reference exact LRU; LFU keeps a hot key that LRU would drop after a scan; `-race` clean with 50 concurrent clients.
- [ ] Benchmark: `GET` overhead of the metadata update stays under 10%.
- [ ] ADR `0003` explaining the accounting model, the sampling parameters and the measured trade-offs.

## Files to touch

- `internal/store/store.go` (entry metadata, memory counter, hooks in `Put` / `Delete` / expiry), `internal/store/evict.go` (new) + tests.
- `internal/server/commands.go` (pre-write check -> OOM error; a `write` flag on `command` helps), `cmd_conn.go` (`CONFIG SET`, `INFO`).
- `cmd/carpincho/main.go` (flags).

## Design notes

- Sampling uniformly from a Go map is awkward: ranging over a map starts at a random offset (the expiry cycle already abuses this). Think about whether that is random enough for eviction, or whether you want a slice-backed index of keys.
- A collection's size changes after `Put` (an `LPUSH` adds an element): decide where deltas are applied.

## Out of scope

Exact memory accounting of Go's allocator overhead, active defragmentation.
