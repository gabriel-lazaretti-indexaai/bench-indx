#!/usr/bin/env bash
set -euo pipefail

benchmark_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$benchmark_dir"

docker compose build app-node
docker compose build app-dotnet
docker compose build app-go
docker compose up -d --wait postgres redis
mkdir -p results
scenario_failures=0

run_scenario() {
  if ./scripts/run-profile.sh "$@"; then
    return 0
  else
    local status=$?
  fi
  printf 'Scenario failed (exit %d): %s\n' "$status" "$*" | tee -a results/matrix-environment.txt
  scenario_failures=$((scenario_failures + 1))
}

cat > results/matrix-environment.txt <<EOF
UTC start: $(date -u +%Y-%m-%dT%H:%M:%SZ)
Docker: $(docker version --format '{{.Server.Version}}')
PostgreSQL image: $(docker compose images -q postgres)
Redis image: $(docker compose images -q redis)
Load generator: grafana/k6:1.8.2
Compose limits: application 2 CPU / 2 GiB; PostgreSQL 2 CPU / 2 GiB; Redis 1 CPU / 512 MiB
Worker concurrency: 8 jobs; 8 items per job; page size 100; PostgreSQL pool max 20
EOF

for language in node dotnet go; do
  docker compose up -d --wait "app-${language}"
  case "$language" in
    node) host_port=38001 ;;
    dotnet) host_port=38002 ;;
    go) host_port=38003 ;;
  esac

  for attempt in $(seq 1 40); do
    if curl --fail --silent "http://127.0.0.1:${host_port}/health" >/dev/null; then
      break
    fi
    if [ "$attempt" -eq 40 ]; then
      docker compose logs --no-color "app-${language}"
      exit 1
    fi
    sleep 1
  done
  printf '\n%s health: ' "$language" >> results/matrix-environment.txt
  curl --fail --silent "http://127.0.0.1:${host_port}/health" >> results/matrix-environment.txt
  printf '\n' >> results/matrix-environment.txt

  for workflow in approval enrichment synchronization; do
    ./scripts/reset-state.sh
    run_scenario "$language" "$workflow" smoke 0 0 100

    ./scripts/reset-state.sh
    run_scenario "$language" "$workflow" smoke 5 10 100

    ./scripts/reset-state.sh
    run_scenario "$language" "$workflow" stress 0 0 100
  done

  ./scripts/reset-state.sh
  run_scenario "$language" mixed quick 5 0 100
  docker compose stop "app-${language}"
done

for raw_output in results/*.raw.json; do
  [ -f "$raw_output" ] || continue
  gzip -1 "$raw_output"
done

printf '\nUTC end: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> results/matrix-environment.txt
if [ "$scenario_failures" -gt 0 ]; then
  printf 'Matrix finished with %d scenario failure(s); inspect the saved k6 summaries.\n' "$scenario_failures" | tee -a results/matrix-environment.txt
  exit 1
fi
echo "Matrix complete. Results are in ${benchmark_dir}/results/."
