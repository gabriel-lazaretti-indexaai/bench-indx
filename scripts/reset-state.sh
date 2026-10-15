#!/usr/bin/env bash
set -euo pipefail

benchmark_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$benchmark_dir"

docker compose exec -T postgres psql -U benchmark -d benchmark -v ON_ERROR_STOP=1 \
  -c "TRUNCATE benchmark_job_items, benchmark_outbox, benchmark_products, benchmark_jobs RESTART IDENTITY CASCADE"
docker compose exec -T redis redis-cli XTRIM benchmark:jobs MAXLEN = 0 >/dev/null 2>&1 || true
