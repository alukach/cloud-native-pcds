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
# Relative hrefs on purpose: rashid then reads the bytes on disk. Given an
# absolute base it would try to fetch them, 404, and downgrade every byte-level
# check to an info. The published-URL shape is covered by tests/test_portolan.py.
run portolan

echo
echo "==> tree"
# `find -printf` is GNU-only and this runs on macOS too.
find "$PCDS_ROOT" -type f \( -name '*.parquet' -o -name '*.json' -o -name '*.md' -o -name '*.png' \) \
  | sort | while read -r f; do printf '%10s  %s\n' "$(wc -c <"$f")" "${f#"$PCDS_ROOT"/}"; done

echo
echo "==> validate with rashid"
uvx --from 'rashid>=0.1.8,<0.2.0' rashid check "$PCDS_ROOT"
