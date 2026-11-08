# Support RESP3 (`HELLO 3`)

| | |
|---|---|
| **Difficulty** | Medium (3/5) |
| **Labels** | `enhancement`, `protocol` |
| **Estimate** | 1 to 2 days |
| **Depends on** | nothing (benefits from the sorted sets issue for `ZSCORE` doubles) |

## Context

carpincho speaks RESP2 only. `HELLO 3` currently answers `-NOPROTO`, and clients such as go-redis v9 (whose default is RESP3) fall back to RESP2 (`TestGoRedisDefaultProtocolFallsBackToResp2`). RESP3 adds typed replies: maps, sets, doubles, booleans, a real null, verbatim strings, big numbers, and out-of-band push messages. With it, clients no longer have to guess that a flat array is really a map.

Spec: <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>

## Acceptance criteria

- [ ] `Client` has a protocol version (2 by default). `HELLO 3` switches the connection and replies with a **map**; `HELLO 2` switches back; other versions keep answering `-NOPROTO`.
- [ ] `resp.Reader.ReadValue` parses every RESP3 type: `_` null, `#` boolean, `,` double (including `inf`, `-inf`, `nan`), `(` big number, `!` bulk error, `=` verbatim string, `%` map, `~` set, `|` attribute (skipped), `>` push. New `Kind` constants and `Value` fields as needed.
- [ ] `resp.Writer` can write them: `Map(n)`, `Set(n)`, `Bool`, `Double`, `NullV3` (`_\r\n`).
- [ ] Replies change **only** for RESP3 connections:
  - [ ] null bulk / null array become `_`
  - [ ] `HGETALL`, `CONFIG GET` become maps
  - [ ] `SMEMBERS` becomes a set
  - [ ] pub/sub deliveries become push (`>`) frames; in RESP3 a subscribed client may run other commands
  - [ ] `HELLO` reply is a map
- [ ] No `if proto == 3` scattered over every handler: give the writer or the client a small reply abstraction (for example `c.reply.NullBulk()`, `c.reply.Map(n)`) so handlers stay readable.
- [ ] `FuzzReadValue` is extended with RESP3 seeds and still round-trips.
- [ ] The go-redis integration tests run **twice**, with `Protocol: 2` and `Protocol: 3`, and pass in both. `redis.NewClient(&redis.Options{Addr: ...})` with defaults must now negotiate RESP3 for real (add a test asserting the connection is in RESP3, for example via `HELLO` reply map or a `CLIENT INFO`-style field).
- [ ] README command table and limitations updated.

## Files to touch

- `internal/resp/value.go`, `reader.go`, `writer.go`, `fuzz_test.go`.
- `internal/server/client.go` (protocol field, reply helpers), `cmd_conn.go` (`HELLO`), `cmd_hash.go`, `cmd_set.go`, `cmd_pubsub.go`.
- `internal/server/integration_test.go` (parametrise by protocol), `commands_test.go`.

## Design notes

- The frames pre-encoded once and shared by all subscribers (`hub.publish`) are RESP2 arrays. Decide whether to keep two encodings per message or encode per protocol lazily.
- Start with the reader/writer plus `HELLO`, ship that, then migrate replies command by command.

## Out of scope

Client-side caching (`CLIENT TRACKING`), attributes beyond skipping them.
