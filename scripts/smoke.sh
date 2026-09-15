#!/usr/bin/env bash
# End-to-end smoke test against the live service, writing to ./data.
# Pulls a handful of stations for a short window: a few dozen requests total.
set -euo pipefail

export PCDS_ROOT="${PCDS_ROOT:-./data}"
export PCDS_CONCURRENCY="${PCDS_CONCURRENCY:-2}"
export PCDS_MAX_RPS="${PCDS_MAX_RPS:-1}"
# Tiny targets so the roll-to-new-file path actually exercises on a small pull.
export PCDS_TARGET_FILE_BYTES="${PCDS_TARGET_FILE_BYTES:-$((4 * 1024 * 1024))}"
export PCDS_MIN_FILE_BYTES="${PCDS_MIN_FILE_BYTES:-$((1 * 1024 * 1024))}"
export PCDS_ROW_GROUP_ROWS="${PCDS_ROW_GROUP_ROWS:-20000}"

run() { echo; echo "==> pcds $*"; uv run pcds "$@"; }

run metadata
run layout --start-year 2024
run plan
run backfill --start-year 2025 --end-year 2026 --networks EC_raw --limit 12 --chunk-years 1
run compact
run catalog
run verify
run portolan --base-url "https://example.org/pcds" --s3-uri "s3://example-bucket/pcds"

echo
echo "==> tree"
find "$PCDS_ROOT" -type f \( -name '*.parquet' -o -name '*.json' -o -name '*.md' -o -name '*.png' \) \
  -printf '%10s  %P\n' | sort -k2

echo
echo "==> validate with rashid (optional)"
echo "    uvx rashid validate $PCDS_ROOT"
echo "    Expect PORTO-FMT-034 to fail; see DEVIATIONS.md."
