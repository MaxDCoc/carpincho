# Sorted sets (`ZADD`, `ZRANGE`, ...) backed by a skip list

| | |
|---|---|
| **Difficulty** | Hard (4/5): a real data-structure exercise |
| **Labels** | `enhancement`, `data-structures` |
| **Estimate** | 2 to 4 days |
| **Depends on** | nothing |

## Context

A sorted set maps unique members to float scores and keeps them ordered by `(score, member)`. It powers leaderboards, rate limiters and priority queues. Redis implements it with a **skip list** (ordered, O(log N) insert / delete / rank) plus a hash table (O(1) `ZSCORE`).

A skip list is a linked list with extra "express lanes": each node gets a random height (probability 1/4 of going one level up, max 32), and every forward pointer stores a **span** (how many nodes it jumps over). Spans are what make `ZRANK` and "element at index i" O(log N) too. Reference: William Pugh, *Skip Lists: A Probabilistic Alternative to Balanced Trees*.

## Acceptance criteria

- [ ] `store.KindZSet`, `NewZSet()`, `TYPE` reports `zset`, `WRONGTYPE` everywhere it applies.
- [ ] A skip list in its own file, with spans, ordered by score then by member (bytewise), with the hash map `member -> score` next to it. API along the lines of `Insert/Update(score, member)`, `Remove(member)`, `Rank(member)`, `Range(startRank, stopRank)`, `RangeByScore(min, max)`.
- [ ] Commands, with Redis-identical replies and errors: `ZADD key [NX|XX] [GT|LT] [CH] score member ...`, `ZREM`, `ZSCORE`, `ZCARD`, `ZRANK`, `ZREVRANK`, `ZRANGE key start stop [WITHSCORES]`, `ZREVRANGE`, `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` (with `(` exclusive bounds and `-inf` / `+inf`), `ZCOUNT`, `ZINCRBY`.
- [ ] Scores are `float64`; NaN is rejected with `ERR value is not a valid float`; replies format scores like Redis (`strconv.FormatFloat(f, 'g', 17, 64)`; `inf`, `-inf`).
- [ ] An empty sorted set disappears (like lists and hashes).
- [ ] Persistence: write commands are propagated to the AOF, and `dumpDataset` in `rewrite.go` emits `ZADD` in chunks (plus `PEXPIREAT`).
- [ ] **Property test** against a trivially correct model (a sorted slice), as `TestListMatchesSliceModel` does for lists: thousands of random `ZADD` / `ZREM` / `ZINCRBY` operations, then compare order, ranks and `RangeByScore`.
- [ ] go-redis integration test (`ZAdd`, `ZRangeWithScores`, `ZRank`, `ZRangeByScore`, `ZIncrBy`) and a restart/rewrite test.
- [ ] A benchmark: insert and rank at 10 000 / 100 000 / 1 000 000 members shows O(log N) growth (a `sort.Slice` baseline will not).
- [ ] README command table updated.

## Files to touch

- `internal/store/skiplist.go` + `skiplist_test.go` (new).
- `internal/store/store.go`: kind, constructor, `Kind.String`.
- `internal/server/cmd_zset.go` (new) and `commands.go` (`registerZSet`).
- `internal/server/rewrite.go`, `persistence_test.go`, `integration_test.go`.

## Design notes

- Use one `rand.Rand` per skip list (or `math/rand/v2` top-level functions) for the level; the shard lock already serialises access, so no extra locking is needed.
- Get `ZADD`/`ZSCORE`/`ZCARD` working with a plain sorted structure first, then swap the skip list in behind the same interface.
- `ZRANGE` in Redis 6.2+ also accepts `BYSCORE`, `BYLEX`, `REV`; the old commands above are enough here.

## Out of scope

`ZUNIONSTORE` / `ZINTERSTORE`, lexicographic ranges, `ZPOPMIN` (a good follow-up, it is cheap once ranks work).
