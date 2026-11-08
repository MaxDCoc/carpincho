# Asynchronous replication with `REPLICAOF`

| | |
|---|---|
| **Difficulty** | Very hard (5/5): distributed-systems territory |
| **Labels** | `enhancement`, `replication`, `distributed-systems` |
| **Estimate** | 1 to 2 weeks |
| **Depends on** | the dump traversal in `rewrite.go` (or the RDB issue); read ADR 0001 and 0002 |

## Context

A replica is a second carpincho that continuously copies a primary: it gives read scaling and a warm standby. The protocol is conceptually simple and full of subtle failure modes:

1. The replica connects and announces itself (`PING`, `REPLCONF listening-port <port>`, `PSYNC ? -1`).
2. The primary answers `+FULLRESYNC <replid> <offset>` and sends a **snapshot** of the dataset as a bulk payload (`$<len>\r\n<bytes>`, no trailing CRLF).
3. From then on the primary streams every write command it executes, in order, over the same connection; the replica applies them with the same command table that replays the AOF.

The critical detail is step 2/3: the snapshot and the start of the command stream must line up exactly, with no write lost and none applied twice (the same bug family as the fuzzy rewrite in ADR 0002).

## Acceptance criteria

- [ ] `REPLICAOF host port` makes the instance a replica (and disconnects it from any previous primary); `REPLICAOF NO ONE` promotes it. `ROLE` and `INFO replication` report role, link status, offsets, connected replicas.
- [ ] **Full resync** works: the replica ends up with the primary's data and then follows live writes. Writes that happen *during* the snapshot are not lost or duplicated (hold the read locks while registering the replica's stream buffer, like the rewrite does).
- [ ] A replica is **read-only** for normal clients: writes answer `-READONLY You can't write against a read only replica.`; the stream from its primary is the only writer.
- [ ] **Expiry**: replicas do not expire keys on their own clock; the primary must propagate a `DEL` when a key expires (lazily or in the active cycle). Today expirations are not logged at all (they are implied by absolute deadlines), so this issue must add propagation of expiry deletions, to the replicas and to the AOF.
- [ ] Per-replica output buffer is bounded; a replica that falls too far behind is disconnected and must do a new full resync (same philosophy as the slow pub/sub subscriber).
- [ ] The replica reconnects with backoff when the link drops and resyncs.
- [ ] Stretch: **partial resync**: a replication backlog (ring buffer of the last N MB with offsets), `PSYNC <replid> <offset>` answered with `+CONTINUE` when the offset is still in the backlog.
- [ ] Tests with two or three in-process servers: write on the primary appears on the replica; replica rejects writes; promotion; primary restart -> full resync; expired keys disappear on the replica; many concurrent writers on the primary while a replica attaches (final states equal); `-race` clean.
- [ ] ADR `0004` describing the protocol subset, the consistency guarantees (asynchronous = writes acknowledged by the primary can be lost on failover) and what is deliberately missing.

## Files to touch

- `internal/replica/` (new): primary-side feed (per-replica buffered stream, backlog) and replica-side client loop.
- `internal/server/server.go` (role state), `client.go` (replica clients; `propagate` also feeds the replication stream), `commands.go` (read-only check), `cmd_repl.go` (new: `REPLICAOF`, `ROLE`, `REPLCONF`, `PSYNC`), `cmd_conn.go` (`INFO replication`).
- `internal/server/rewrite.go`: factor the dump traversal so the snapshot can be produced into an arbitrary writer.
- `internal/store/store.go` and `server.go`: expiry deletions propagated.

## Design notes

- The `replay` client trick used for the AOF (a Client whose replies are discarded and which does not propagate) is exactly what the replica applies the stream with.
- Test failure injection by wrapping the connection in a `net.Conn` that you can cut.

## Out of scope

Sentinel / automatic failover, cluster mode, `WAIT`, diskless replication.
