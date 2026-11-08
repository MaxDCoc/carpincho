# RDB-style snapshots (`SAVE`, `BGSAVE`, load on start)

| | |
|---|---|
| **Difficulty** | Hard (4/5) |
| **Labels** | `enhancement`, `persistence` |
| **Estimate** | 2 to 4 days |
| **Depends on** | nothing (read [ADR 0002](../adr/0002-aof-rewrite.md) first) |

## Context

The AOF is a log of every write, so recovery replays history (slow for big datasets) and the file only shrinks when you rewrite it. A **snapshot** is a compact, point-in-time image of the whole dataset in a binary format: loading it is much faster than replaying a log, and it makes a good backup artefact. Redis calls it RDB and writes it periodically (`save 60 10000`) or on demand.

The hard part is the same one as the AOF rewrite: taking a *consistent* snapshot of a live, sharded, multi-writer store without `fork`. ADR 0002 explains why a fuzzy snapshot is wrong and what the current rewrite does (read-lock every shard). This issue is the chance to do it better or at least to compare the options with measurements.

## Acceptance criteria

- [ ] A documented binary format (put it in the package doc or an ADR): magic + version header, then per key: type byte, optional expire (ms), length-prefixed key and value(s), and a trailer with a CRC (`hash/crc64`). It does **not** have to be byte-compatible with Redis' RDB; if you want a stretch goal, make a Redis-compatible reader for strings/lists/hashes/sets of RDB v9.
- [ ] New package `internal/rdb` with `Write(w io.Writer, snapshot)` / `Read(r io.Reader, apply)` and unit tests with every type and TTL.
- [ ] Commands: `SAVE` (blocking), `BGSAVE` (`+Background saving started`, errors if one is running), `LASTSAVE`; `INFO persistence` shows `rdb_bgsave_in_progress`, `rdb_last_save_time`, `rdb_last_bgsave_status`.
- [ ] Flag `--save "60 1000"` (seconds / changes) with a background goroutine that triggers `BGSAVE`; and `--dbfilename`.
- [ ] Atomic file replacement: write a temporary file in the same directory, `fsync`, `rename`, `fsync` the directory. A crash mid-save must never leave a corrupt `dump` file in place.
- [ ] On start: if there is an AOF, use it (it is more up to date, same rule as Redis); otherwise load the snapshot. Corrupt or truncated snapshot -> clear error with the offset, never a half-loaded dataset.
- [ ] Expired keys are not written and not loaded.
- [ ] Tests: round trip of all types, TTL preserved as an absolute deadline, truncated file, flipped byte caught by the CRC, **fuzz the reader** (`FuzzRead`), snapshot taken while writers run is consistent (like `TestRewriteIsExactWhileWritersAreRunning`: counters must equal the acknowledged increments).
- [ ] A short comparison in the PR or an ADR: load time of N keys from AOF replay vs. snapshot.

## Files to touch

- `internal/rdb/` (new: `format.go`, `writer.go`, `reader.go`, tests).
- `internal/server/cmd_conn.go` (`SAVE`, `BGSAVE`, `LASTSAVE`, `INFO`), `server.go` (load at start, periodic goroutine), possibly `rewrite.go` to share the dump traversal.
- `cmd/carpincho/main.go` (flags).

## Design notes

- Options to get consistency: (1) hold all read locks while streaming (what the rewrite does), (2) take each shard's read lock, copy its entries into a private slice, release, then serialise from the copies (shorter pauses, more memory, but you get a *per-shard* snapshot only unless you add a global sequence number), (3) per-entry version counters. Pick one, measure the write pause, document the trade-off.
- Streams are bytes: reuse the `bufio.Writer` + CRC via `io.MultiWriter`.

## Out of scope

Replication using the snapshot (see the replication issue), compression.
