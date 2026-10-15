#!/usr/bin/env bash
set -euo pipefail

benchmark_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$benchmark_dir"

language="${1:?usage: run-profile.sh node|dotnet|go workflow profile [externalLatencyMs] [failureEvery] [itemCount]}"
workflow="${2:?missing workflow}"
profile="${3:?missing profile}"
external_latency_ms="${4:-0}"
failure_every="${5:-0}"
item_count="${6:-100}"
host_uid="$(id -u)"
host_gid="$(id -g)"

case "$language" in
  node) host_port=38001 ;;
  dotnet) host_port=38002 ;;
  go) host_port=38003 ;;
  *) echo "language must be node, dotnet, or go" >&2; exit 2 ;;
esac

case "$workflow" in
  approval|enrichment|synchronization|mixed) ;;
  *) echo "workflow must be approval, enrichment, synchronization, or mixed" >&2; exit 2 ;;
esac

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_name="${stamp}-${language}-${workflow}-${profile}-lat${external_latency_ms}-fail${failure_every}"
container_id="$(docker compose ps -q "app-${language}")"
if [ -z "$container_id" ]; then
  echo "app-${language} must be running before this command" >&2
  exit 1
fi

for attempt in $(seq 1 40); do
  if curl --fail --silent "http://127.0.0.1:${host_port}/health" >/dev/null; then
    break
  fi
  if [ "$attempt" -eq 40 ]; then
    echo "app-${language} did not become healthy on port ${host_port}" >&2
    exit 1
  fi
  sleep 1
done

mkdir -p results
docker stats --format '{{.CPUPerc}},{{.MemUsage}}' "$container_id" > "results/${run_name}.resources.csv" &
stats_pid="$!"
stop_stats() {
  kill "$stats_pid" >/dev/null 2>&1 || true
  wait "$stats_pid" 2>/dev/null || true
}
trap stop_stats EXIT

docker compose --profile load run --rm -T \
  --user "${host_uid}:${host_gid}" \
  -e "BASE_URL=http://app-${language}:3000" \
  -e "WORKFLOW=${workflow}" \
  -e "PROFILE=${profile}" \
  -e "ITEM_COUNT=${item_count}" \
  -e "EXTERNAL_LATENCY_MS=${external_latency_ms}" \
  -e "FAILURE_EVERY=${failure_every}" \
  -e "RESULTS_TAG=${run_name}" \
  -e "SUMMARY_PATH=/results/${run_name}.summary.json" \
  k6 \
  run \
  --out="json=/results/${run_name}.raw.json" \
  /scripts/jobs.js
