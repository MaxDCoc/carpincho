#!/usr/bin/env bash
# Benchmarks carpincho against Redis 7 with redis-benchmark, which ships in the
# official redis:7-alpine image. Needs Docker. The servers can be placed in two ways:
#
#   MODE=network bash bench/run-docker-bench.sh     (default; the fair comparison)
#   MODE=host    bash bench/run-docker-bench.sh     (a smoke test, biased: see below)
#
# MODE=network  Both servers run in Linux containers on one Docker network with the
#               same CPU limit, and redis-benchmark reaches them directly: same kernel,
#               same network path, no host hop. carpincho is the static linux/amd64
#               binary built from this repository (needs Go in the PATH). The
#               redis:7-alpine image is only used as a tiny Linux root filesystem to run
#               it: the Redis server is not started in that container.
#
# MODE=host     carpincho runs natively on your machine: start it first with
#               `carpincho --port 6380`. Redis 7 runs in a container whose port is
#               published on 127.0.0.1:6381, and redis-benchmark runs in another
#               container. Both servers are reached through host.docker.internal, so
#               every request crosses Docker Desktop's network layer, but NOT the same
#               path (the published Redis port takes extra hops) and the two servers run
#               on different operating systems. Do not read it as a verdict on the
#               servers. (Docker Desktop only: on plain Linux Docker add
#               --add-host=host.docker.internal:host-gateway to the docker run lines.)
#
# Neither server persists anything (Redis runs with --save "" --appendonly no). The
# measurements are interleaved (carpincho, redis, carpincho, redis, ...) and the
# keyspace is flushed before each one. Output: Markdown tables on stdout and the raw
# numbers in bench/results/<timestamp>-<mode>.tsv.
#
# Tunables (environment variables): REQUESTS CLIENTS KEYSPACE RUNS PIPELINES TESTS CPUS THREADS
# THREADS sets redis-benchmark --threads. It is single-threaded by default, which caps
# what it can generate (a few hundred thousand requests/s) and can hide the difference
# between two fast servers; raise it to check whether the generator is the bottleneck.
#
# Keep every measurement long: with a pipeline depth of 16, 100 000 requests take well
# under a second, connection set-up is a visible part of that, and in --threads mode
# redis-benchmark times a run in steps of 250 ms. Use REQUESTS=1000000 or more there.
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash on Windows: do not rewrite the "/path" arguments given to docker

MODE="${MODE:-network}"
REQUESTS="${REQUESTS:-100000}" # requests per test
CLIENTS="${CLIENTS:-50}"       # parallel connections (-c)
KEYSPACE="${KEYSPACE:-100000}" # random keys (-r); without it every request touches the same key
RUNS="${RUNS:-2}"              # repetitions of every measurement
PIPELINES="${PIPELINES:-1 16}" # commands per round trip (-P); one table per value
TESTS="${TESTS:-ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,lrange_100,mset}"
CPUS="${CPUS:-2}"              # MODE=network: CPU limit of each server container
THREADS="${THREADS:-}"         # redis-benchmark --threads; empty keeps its default (one thread)
IMAGE="${IMAGE:-redis:7-alpine}"
CARP_ADDR="${CARP_ADDR:-host.docker.internal:6380}" # MODE=host: where the native carpincho listens
REDIS_PORT="${REDIS_PORT:-6381}"                    # MODE=host: host port published for Redis

REDIS_NAME=carpincho-baseline
CARP_NAME=carpincho-bench
NET=carpincho-bench-net

cd "$(dirname "$0")/.."
mkdir -p bench/results
TSV="bench/results/$(date +%Y%m%d-%H%M%S)-$MODE${THREADS:+-threads$THREADS}${TAG:+-$TAG}.tsv" # TAG: optional file name suffix

cleanup() {
  docker rm -f "$REDIS_NAME" "$CARP_NAME" >/dev/null 2>&1 || true
  if [[ "$MODE" == network ]]; then docker network rm "$NET" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
cleanup

NETARGS=()
case "$MODE" in
host)
  docker run -d --name "$REDIS_NAME" -p "127.0.0.1:$REDIS_PORT:6379" "$IMAGE" \
    redis-server --save "" --appendonly no >/dev/null
  CARP_HOST="${CARP_ADDR%:*}" CARP_PORT="${CARP_ADDR##*:}"
  REDIS_HOST=host.docker.internal REDIS_PRT="$REDIS_PORT"
  ;;
network)
  docker network create "$NET" >/dev/null
  BIN=bin/linux/carpincho
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o "$BIN" ./cmd/carpincho
  docker create --name "$CARP_NAME" --network "$NET" --cpus="$CPUS" --entrypoint /carpincho "$IMAGE" \
    --bind 0.0.0.0 --port 6380 >/dev/null
  docker cp "$BIN" "$CARP_NAME:/carpincho"
  docker start "$CARP_NAME" >/dev/null
  docker run -d --name "$REDIS_NAME" --network "$NET" --cpus="$CPUS" "$IMAGE" \
    redis-server --save "" --appendonly no >/dev/null
  CARP_HOST="$CARP_NAME" CARP_PORT=6380
  REDIS_HOST="$REDIS_NAME" REDIS_PRT=6379
  NETARGS=(--network "$NET")
  ;;
*)
  echo "error: MODE must be host or network" >&2
  exit 2
  ;;
esac

# ${NETARGS[@]+...}: an empty array must not trip "set -u" on older bash versions
cli() { docker run --rm ${NETARGS[@]+"${NETARGS[@]}"} "$IMAGE" "$@"; }

wait_ready() { # host port
  local i
  for i in $(seq 1 30); do
    if [[ "$(cli redis-cli -h "$1" -p "$2" PING 2>/dev/null || true)" == *PONG* ]]; then return 0; fi
    sleep 1
  done
  echo "error: nothing answers PING at $1:$2 (MODE=host needs carpincho running: carpincho --port 6380)" >&2
  return 1
}

run_bench() { # host port pipeline -> CSV on stdout
  cli redis-benchmark -h "$1" -p "$2" -n "$REQUESTS" -c "$CLIENTS" -P "$3" -r "$KEYSPACE" -t "$TESTS" ${THREADS:+--threads "$THREADS"} --csv
}

wait_ready "$CARP_HOST" "$CARP_PORT"
wait_ready "$REDIS_HOST" "$REDIS_PRT"

{
  echo "# date: $(date '+%Y-%m-%d %H:%M:%S %z')   mode: $MODE"
  echo "# requests/test=$REQUESTS clients=$CLIENTS keyspace=$KEYSPACE runs=$RUNS pipelines=($PIPELINES) generator threads=${THREADS:-1} cpus/server=$CPUS(network mode only)"
  echo "# tests: $TESTS"
  echo "# $(docker info --format 'docker {{.ServerVersion}} | VM: {{.NCPU}} CPUs, {{.MemTotal}} bytes | {{.OperatingSystem}}')"
  echo "# $(docker exec "$REDIS_NAME" redis-server --version | cut -c1-40) | $(cli redis-benchmark --version)"
  echo "# carpincho: $(git describe --always --dirty 2>/dev/null || echo unknown)"
} | tee "$TSV" >&2

record() { # server pipeline run host port
  local out
  cli redis-cli -h "$4" -p "$5" FLUSHALL >/dev/null
  out="$(run_bench "$4" "$5" "$2")"
  [[ -n "$out" ]] || { echo "error: redis-benchmark printed nothing for $4:$5" >&2; exit 1; }
  # CSV columns: test, rps, avg, min, p50, p95, p99, max
  printf '%s\n' "$out" | tr -d '"\r' |
    awk -F, -v s="$1" -v p="$2" -v r="$3" '$1 != "test" && NF >= 5 { printf "%s\t%s\t%s\t%s\t%.0f\t%s\n", s, p, r, $1, $2, $5 }' >>"$TSV"
}

for P in $PIPELINES; do
  echo "[$(date +%H:%M:%S)] -P $P: warm-up (not recorded)" >&2
  for target in "$CARP_HOST $CARP_PORT" "$REDIS_HOST $REDIS_PRT"; do
    # shellcheck disable=SC2086
    set -- $target
    cli redis-benchmark -h "$1" -p "$2" -n 20000 -c "$CLIENTS" -P "$P" -r "$KEYSPACE" -t ping_mbulk,set -q >/dev/null
  done
  for run in $(seq 1 "$RUNS"); do
    echo "[$(date +%H:%M:%S)] -P $P: run $run, carpincho" >&2
    record carpincho "$P" "$run" "$CARP_HOST" "$CARP_PORT"
    echo "[$(date +%H:%M:%S)] -P $P: run $run, redis" >&2
    record redis "$P" "$run" "$REDIS_HOST" "$REDIS_PRT"
  done
done

report() { # tsv file -> Markdown tables
  awk -v runs="$RUNS" '
    /^#/ { next }
    BEGIN { FS = "\t" }
    {
      v[$1, $2, $3, $4] = $5
      if (!($2 in sp)) { sp[$2] = 1; plist[++np] = $2 }
      if (!(($2, $4) in st)) { st[$2, $4] = 1; tl[$2, ++nt[$2]] = $4 }
    }
    END {
      for (i = 1; i <= np; i++) {
        p = plist[i]
        printf "\n#### Pipeline depth %s (requests per second, higher is better)\n\n| test |", p
        for (r = 1; r <= runs; r++) printf " carpincho #%d |", r
        for (r = 1; r <= runs; r++) printf " Redis #%d |", r
        printf " carpincho / Redis |\n|---|"
        for (c = 0; c < 2 * runs + 1; c++) printf "--:|"
        printf "\n"
        for (j = 1; j <= nt[p]; j++) {
          t = tl[p, j]; cs = 0; rs = 0
          printf "| %s |", t
          for (r = 1; r <= runs; r++) { x = v["carpincho", p, r, t]; cs += x; printf " %d |", x }
          for (r = 1; r <= runs; r++) { x = v["redis", p, r, t]; rs += x; printf " %d |", x }
          if (rs > 0) printf " %.2fx |\n", cs / rs; else printf " n/a |\n"
        }
      }
    }' "$1"
}

echo
grep '^#' "$TSV"
report "$TSV"
echo
echo "raw numbers: $TSV"
