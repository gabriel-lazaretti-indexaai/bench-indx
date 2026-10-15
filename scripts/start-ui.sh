#!/usr/bin/env bash
set -euo pipefail

benchmark_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$benchmark_dir"
exec node ui/server.mjs
