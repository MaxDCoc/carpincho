#!/usr/bin/env bash
# Benchmarks carpincho against Redis 7 with redis-benchmark (from the official
# redis:7-alpine image). Both servers run in containers on one Docker network,
# with the same CPU limit, so they get the same kernel and the same network path.
#
#   ./bench/run-docker-bench.sh
#   TARGET=host.docker.internal:6380 ./bench/run-docker-bench.sh   # also test a carpincho running natively on the host
#
# Needs Docker. Tunables (environment variables):
#   CPUS      CPU limit of each server container          (default 2)
#   REQUESTS  requests per test                            (default 200000)
#   CLIENTS   parallel connections                         (default 50)
#   PIPELINE  commands per round trip                      (default 1)
#   TESTS     comma separated redis-benchmark tests        (only commands carpincho implements)
set -euo pipefail

CPUS="${CPUS:-2}"
REQUESTS="${REQUESTS:-200000}"
CLIENTS="${CLIENTS:-50}"
PIPELINE="${PIPELINE:-1}"
TESTS="${TESTS:-ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,lrange_100,mset}"
NET=carpincho-bench

cleanup() {
  docker rm -f bench-redis bench-carpincho >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$(dirname "$0")/.."

echo "# machine"
docker info --format 'docker {{.ServerVersion}} | {{.NCPU}} CPUs | {{.MemTotal}} bytes of memory | {{.OperatingSystem}}'
echo "# settings: cpus/server=$CPUS requests=$REQUESTS clients=$CLIENTS pipeline=$PIPELINE"

docker build -t carpincho:bench .
cleanup
docker network create "$NET" >/dev/null

docker run -d --name bench-redis --network "$NET" --cpus="$CPUS" \
  redis:7-alpine redis-server --save "" --appendonly no >/dev/null
docker run -d --name bench-carpincho --network "$NET" --cpus="$CPUS" \
  carpincho:bench --bind 0.0.0.0 --port 6380 >/dev/null
sleep 2

bench() { # label host port
  echo
  echo "== $1 ($2:$3)"
  docker run --rm --network "$NET" redis:7-alpine redis-benchmark \
    -h "$2" -p "$3" -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -t "$TESTS" --csv |
    tr -d '"' |
    awk -F, 'NR > 1 { printf "%-14s %12.0f req/s   p50 = %s ms\n", $1, $2, $5 }'
}

bench "redis 7" bench-redis 6379
bench "carpincho" bench-carpincho 6380
if [[ -n "${TARGET:-}" ]]; then
  bench "carpincho on the host" "${TARGET%%:*}" "${TARGET##*:}"
fi
