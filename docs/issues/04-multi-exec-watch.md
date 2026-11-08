# Transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH`

| | |
|---|---|
| **Difficulty** | Hard (4/5): touches the concurrency model |
| **Labels** | `enhancement`, `concurrency` |
| **Estimate** | 3 to 5 days |
| **Depends on** | read [ADR 0001](../adr/0001-concurrency-model.md) first |

## Context

`MULTI` starts a transaction: following commands are *queued* (reply `+QUEUED`) and `EXEC` runs them all **atomically** (no other client's command runs in between) and returns an array with every reply. `WATCH key ...` adds optimistic locking: if a watched key is modified by anyone else between `WATCH` and `EXEC`, `EXEC` runs nothing and returns a null array, and the client retries.

Redis gets atomicity for free from its single thread. carpincho executes commands from many goroutines over 256 shard locks, so atomicity across several commands and several keys is the interesting problem.

## Acceptance criteria

- [ ] `MULTI` (`-ERR MULTI calls can not be nested`), `EXEC` (`-ERR EXEC without MULTI`), `DISCARD` (`-ERR DISCARD without MULTI`), `WATCH` (`-ERR WATCH inside MULTI is not allowed`), `UNWATCH`.
- [ ] While in `MULTI`, valid commands reply `+QUEUED`; a command with an unknown name or wrong arity marks the transaction as failed and `EXEC` answers `-EXECABORT Transaction discarded because of previous errors.`
- [ ] `EXEC` is atomic with respect to every other client. A test proves it: two goroutines run `MULTI; INCR a; INCR b; EXEC` while a third repeatedly reads both keys inside its own `EXEC`; it must never see `a != b`.
- [ ] Runtime errors inside `EXEC` do **not** roll back (Redis semantics): the reply array contains the error as one element and the other commands still run. Document it.
- [ ] `WATCH`: per-key version tracking (a monotonically increasing counter bumped on every modification, including expiry and in-place changes to collections). `EXEC` compares versions and returns a null array if any watched key changed. A watched key that was deleted or expired counts as changed.
- [ ] Persistence: a transaction is written to the AOF as `MULTI ... EXEC` in one `Append`, and the loader discards a trailing `MULTI` block without `EXEC` (the crash happened mid-write), so replay is also atomic.
- [ ] go-redis tests: `TxPipelined` (queued replies), `Watch` helper with a retry loop under contention (N goroutines doing a check-and-set on one key must end with the exact count), `Discard`.
- [ ] `-race` clean; no deadlock with `MSET`, `DEL a b`, `KEYS`, `FLUSHALL` running at the same time.
- [ ] Update ADR 0001 ("Revisit when") with what you chose and what it cost, with numbers.

## Files to touch

- `internal/server/client.go`: transaction state (queued commands, failed flag, watched keys with their versions).
- `internal/server/cmd_tx.go` (new); `commands.go`: in `execute`, queue instead of run when the client is in `MULTI`.
- `internal/store/store.go`: version counters and a way to read them under an `Access`.
- `internal/server/client.go` (`propagate`): wrap in `MULTI/EXEC`; `internal/aof/aof.go` loader: drop an unterminated block.
- `internal/server/integration_test.go`, `commands_test.go`, `persistence_test.go`.

## Design notes

Two candidate designs for atomic `EXEC`; try the simple one and measure before building the clever one.

1. **Global transaction lock**: a `sync.RWMutex`; every normal command takes the read side (cheap, uncontended), `EXEC` takes the write side. Trivially correct, but `EXEC` pauses the whole server for its duration.
2. **Key-set locking**: `EXEC` asks each queued command which keys it touches (a `keys(args)` function per command), calls `Acquire(true, allKeys...)` once and runs the handlers against that single `Access`. This needs the handlers to accept an `Access` instead of acquiring their own (a refactor of every `cmd_*.go`) but only blocks the shards involved.

For `WATCH`, version counters per shard are coarser (false positives, always safe) and cheaper than per key; per key is exact. Discuss in the PR.

## Out of scope

Lua scripting, `MULTI` inside pub/sub subscribed mode.
