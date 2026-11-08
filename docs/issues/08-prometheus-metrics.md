# Expose Prometheus metrics on an HTTP endpoint

| | |
|---|---|
| **Difficulty** | Easy (2/5) |
| **Labels** | `enhancement`, `observability`, `good first issue` |
| **Estimate** | 2 to 4 hours |
| **Depends on** | nothing |

## Context

`INFO` gives a human a snapshot, but nothing can scrape it. A production server needs a `/metrics` endpoint in the [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/) so operators can graph throughput, latency and persistence health.

The server already keeps two counters (`Server.stats.commands`, `Server.stats.connections`), `Store.Len()` / `ExpiresLen()`, `AOF.Size()` and the `rewriting` flag. This issue turns them into proper metrics and adds the ones that are missing.

## Acceptance criteria

- [ ] New flag `--metrics-addr` (for example `:9121`). Empty (the default) means no HTTP listener at all.
- [ ] `GET /metrics` returns valid exposition text with at least:
  - [ ] `carpincho_connected_clients` (gauge) and `carpincho_connections_received_total` (counter)
  - [ ] `carpincho_commands_processed_total{command="get"}` (counter **per command**)
  - [ ] `carpincho_command_duration_seconds` (histogram, buckets from 50µs to 100ms)
  - [ ] `carpincho_keys` and `carpincho_keys_with_ttl` (gauges)
  - [ ] `carpincho_expired_keys_total` (counter; `Store.ExpireCycle` already returns how many it deleted, lazy expiry needs a hook)
  - [ ] `carpincho_aof_size_bytes`, `carpincho_aof_fsync_duration_seconds` (histogram), `carpincho_aof_rewrites_total`, `carpincho_aof_rewrite_in_progress`
  - [ ] `carpincho_pubsub_channels` and `carpincho_pubsub_subscribers`
- [ ] The hot path stays cheap: per-command counters are atomics in a fixed array indexed by command (no map lookup or lock per command). `BenchmarkExecute/GET` regresses by less than 3%.
- [ ] Latency timing is either sampled or can be switched off (`--metrics-latency=false`), and the issue description says what it measured.
- [ ] A test starts a server, runs a few commands, scrapes `/metrics` and asserts the counters (parse it with `github.com/prometheus/common/expfmt`, or a tiny parser in the test).
- [ ] The HTTP server shuts down in `Server.Shutdown` and its goroutine is not leaked (`go test -race` clean).
- [ ] README: a "Metrics" section with the metric list and a Prometheus scrape config snippet.

## Files to touch

- `internal/server/metrics.go` (new): metric definitions, the `/metrics` handler.
- `internal/server/commands.go`: count (and optionally time) inside `execute`; give `command` an index.
- `internal/server/server.go`: start/stop the HTTP listener; hold the counters.
- `internal/aof/aof.go`: expose fsync duration and rewrite counters (a small `Stats()` method).
- `cmd/carpincho/main.go`: the flag.
- `README.md`.

## Design notes

- You can write the exposition format by hand (it is a few `fmt.Fprintf` lines) or use `github.com/prometheus/client_golang`. Writing it by hand keeps the zero-dependency server; using the client library is what most teams do. Pick one and explain why in the PR.
- Beware of cardinality: label by command name only, never by key.
- Gauges that call `Store.Len()` take 256 read locks briefly; scrape interval is seconds, so that is fine.

## Out of scope

Tracing, a Grafana dashboard (nice stretch goal), authentication on the metrics port.
