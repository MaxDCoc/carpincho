# Keyspace notifications (and `PSUBSCRIBE`)

| | |
|---|---|
| **Difficulty** | Medium (3/5) |
| **Labels** | `enhancement`, `pubsub` |
| **Estimate** | 1 to 2 days |
| **Depends on** | nothing (includes the `PSUBSCRIBE` it needs) |

## Context

Redis can publish an event whenever a key changes, so clients can react to data instead of polling: channel `__keyspace@0__:<key>` receives the event name (`set`, `del`, `expire`, `lpush`, ...), channel `__keyevent@0__:<event>` receives the key. It is controlled by `notify-keyspace-events` (classes `g` generic, `$` string, `l` list, `s` set, `h` hash, `x` expired, `e` evicted, `A` = all of them; `K` and `E` choose the channel families).

Most clients consume them with **pattern subscriptions** (`PSUBSCRIBE __keyevent@0__:expired`), which carpincho does not have yet, so that is part of this issue.

Reference: <https://redis.io/docs/latest/develop/use/keyspace-notifications/>

## Acceptance criteria

- [ ] `PSUBSCRIBE`, `PUNSUBSCRIBE` with glob patterns (reuse `store.GlobMatch`); deliveries use the frame `["pmessage", pattern, channel, payload]`; `PUBLISH` counts both exact and pattern subscribers; subscribed-mode rules include the new commands.
- [ ] Flag `--notify-keyspace-events KEA` and read/write support through `CONFIG GET` / `CONFIG SET notify-keyspace-events` (add a minimal `CONFIG SET` for this one parameter, thread-safe).
- [ ] Events are emitted for every mutation of every supported type (`set`, `del`, `expire`, `persist`, `incrby`, `append`, `lpush`, `rpush`, `lpop`, `rpop`, `hset`, `hdel`, `sadd`, `srem`, `rename` if present) following Redis' event names.
- [ ] `expired` events from **both** lazy expiry and the active cycle (the cycle runs in its own goroutine, so `Store.ExpireCycle` needs a callback or a returned list of keys).
- [ ] Zero cost when disabled: a benchmark shows no extra allocations in `BenchmarkExecute/SET` with notifications off.
- [ ] Events are published **after** the shard locks are released (keep the "no work under a shard lock" rule): handlers collect events in the `Client` during the command and `execute` publishes them at the end.
- [ ] Tests (fake clock for expiry): each flag combination; `K` only, `E` only, both; class filtering; go-redis `PSubscribe` receives `expired` for a key that nobody reads.
- [ ] README and `INFO`/`CONFIG` docs updated.

## Files to touch

- `internal/server/notify.go` (new): event names, flag parsing, `Client.notify`.
- `internal/server/cmd_pubsub.go`: patterns in the hub, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `pmessage` frames.
- `internal/server/cmd_conn.go`: `CONFIG SET` / `GET`.
- `internal/server/cmd_*.go`: one `c.notify(...)` call per successful mutation.
- `internal/store/store.go`: expired-key callback from `ExpireCycle` and lazy deletion.
- `cmd/carpincho/main.go`: the flag.

## Design notes

- The hub today is `channel -> set of clients`. Pattern subscribers need a second structure; iterating all patterns on every publish is fine to start with (Redis does the same).
- Think about what `expired` means for a key that is lazily expired inside a *read* access (it is not deleted there). Where should the event fire?

## Out of scope

Persisting notification settings in the AOF, `evicted` events (they need the eviction issue).
