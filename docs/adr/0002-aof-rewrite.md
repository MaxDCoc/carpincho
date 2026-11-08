# ADR 0002: AOF persistence and rewrite without `fork`

- **Status:** accepted
- **Date:** 2026-11-08

## Context

Persistence uses an append-only file (AOF) in the same format as Redis: every write command, RESP-encoded, in the order it was applied. On start the file is replayed through the normal command table. This ADR records the decisions that are easy to get subtly wrong.

## Decisions

### 1. Three durability levels, with group commit

`always`, `everysec` and `no` map to Redis' `appendfsync`.

- A write handler calls `AOF.Append(record)` **while it still holds the shard lock**. This only copies bytes into an in-memory buffer and returns a sequence number (total bytes appended so far).
- Before sending the reply, the connection calls `AOF.Commit(seq)` (from the "flush before you block" hook, after the locks were released). `Commit` writes the whole buffer with one `write(2)`; with `always` it then `fsync`s.
- **Group commit:** `fsync` is serialised by a mutex and remembers the highest sequence already synced. If another client's `fsync` already covered my sequence number, I return without syncing. Under concurrency N clients share one `fsync` instead of paying N.
- `everysec`: a goroutine fsyncs once per second; the `write(2)` still happens before the reply, so a process crash (not a power cut) loses nothing.
- Clean shutdown always fsyncs, whatever the policy.

### 2. Expirations are absolute in the log

`SET k v EX 100`, `EXPIRE`, `PEXPIRE`, `EXPIREAT` are all logged as `SET ... PXAT <ms>` / `PEXPIREAT <ms>`. Replaying a relative `EXPIRE 100` a day later would give the key another 100 seconds; a deadline stays correct. Test: `TestRelativeExpiryIsStoredAsAnAbsoluteDeadline`.

### 3. Expiry is suspended while the log is replayed

During replay "now" is later than when each command originally ran. If lazy expiry were active, `SET n 10 PX 100; INCR n; PERSIST n` replayed an hour later would find `n` already expired at the `INCR` and diverge from what really happened. The store has a `loading` flag that disables expiry during replay; afterwards normal expiry applies. Test: `TestReplayDoesNotExpireKeysThatWereAliveWhenTheCommandRan`.

### 4. A torn last record is cut, anything else is an error

A crash in the middle of a `write(2)` leaves an incomplete last record. The RESP reader distinguishes `io.EOF` (clean end between commands) from `io.ErrUnexpectedEOF` (ended inside a command); on the latter the loader truncates the file to the last complete record, logs a warning and carries on. A record that is **complete but malformed** is corruption, not a crash artefact: startup fails with the byte offset and leaves the file untouched.

### 5. Rewrite (`BGREWRITEAOF`): hold read locks on every shard, dump, swap

Redis `fork`s; the child holds a copy-on-write snapshot of memory and rewrites the log at leisure while the parent keeps serving, buffering new writes to append later. **Go cannot fork a process safely** (the runtime has threads, a GC, a scheduler).

Options considered:

| Option | Verdict |
|--------|---------|
| **Fuzzy snapshot**: walk the shards one by one without a global lock, log every concurrent write to a side buffer, append the buffer at the end | **Incorrect** for non-idempotent commands. A key `n` is `INCR`ed at time t1 and snapshotted at t2 > t1: the snapshot already contains the increment and the side buffer replays it again. Redis avoids this because the fork gives an instantaneous snapshot. |
| Copy-on-write persistent data structures / per-key versions to get a cheap consistent snapshot | Correct but a large redesign of the store; recorded as future work. |
| Copy the whole dataset into memory under the global lock, release, write it out | Pause is shorter than writing to disk but needs up to 2x RAM. |
| **Read-lock all shards, stream the dump to a temporary file, fsync, atomic rename, reopen, release** | **Chosen.** Simple and exactly correct: while every shard is read-locked no handler can be between "applied to memory" and "appended to the log", so everything still buffered in the old log is already in the snapshot and is discarded instead of written. |

Mechanics (`server.RewriteAOF`, `aof.BeginRewrite` / `Rewrite.Commit`):

1. `AcquireAll(read)` locks the 256 shards in ascending order.
2. The dataset is serialised as the shortest command list that rebuilds it: `SET k v [PXAT ms]`, `RPUSH` / `HSET` / `SADD` in chunks of 128 elements, plus `PEXPIREAT` for aggregates with a TTL. Expired keys are skipped.
3. The temporary file (`appendonly.aof.rewrite.tmp`) is flushed and `fsync`ed, then the old file is closed, the temporary one is `rename`d over it (atomic on POSIX; on Windows the old handle must be closed first, which is why the order matters), the directory is synced when the OS allows it, and the log is reopened in append mode.
4. The locks are released only after the swap. Writes that arrive afterwards go to the new file.
5. A crash before the rename leaves the old log intact (the stale temporary file is deleted at the next start); a crash after it leaves the new one. There is no moment with no valid log.

**Cost, stated plainly:** writers (and readers queued behind a waiting writer on the same shard) are paused for the duration of the dump, proportional to dataset size. `BGREWRITEAOF` is "background" only in the sense that the command returns immediately. For a learning project that trade is acceptable; the hardest-to-notice failure mode (silently wrong data) is the one we refuse.

The strongest test of this decision is `TestRewriteIsExactWhileWritersAreRunning`: 8 goroutines `INCR` four shared counters while rewrites run back to back; after a restart every counter must equal the number of acknowledged increments.

## Consequences

- The log is human-readable RESP and compatible in spirit with `redis-check-aof`.
- The log is not compacted automatically (no `auto-aof-rewrite-percentage`); call `BGREWRITEAOF`. Easy follow-up.
- Write errors on the AOF are logged, not fatal (Redis stops accepting writes). Easy but important follow-up.
- RDB snapshots ([issue 02](../issues/02-rdb-snapshots.md)) have the same consistency problem and can reuse this locking approach or improve on it.
