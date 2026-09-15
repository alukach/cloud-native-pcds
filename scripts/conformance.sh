#!/usr/bin/env bash
# Portolan conformance gate against the published catalog.
#
# rashid reads a local directory only; it rejects s3:// and https:// outright.
# So pull the metadata down and check that. Partitions are skipped because
# observations declares no data asset (DEVIATIONS.md, PORTO-FMT-034) and rashid
# never opens one. `pcds verify` gates those instead.
set -euo pipefail

dir="${1:-${RUNNER_TEMP:-/tmp}/pcds-catalog}"
rm -rf "$dir"

# ${VAR:+...} not an array: unset means real AWS and the flag must vanish, but
# expanding an empty array under `set -u` breaks on the bash 3.2 macOS ships.
aws s3 sync "${PCDS_ROOT%/}/" "$dir" \
  ${PCDS_S3_ENDPOINT:+--endpoint-url "$PCDS_S3_ENDPOINT"} \
  --exclude 'observations/period=*' \
  --exclude '_staging/*' --exclude '_state/*' --exclude '_manifest/*'

# --live is the point: range and CORS against the real host. The offline pass is
# already in CI (tests/test_conformance.py).
uv run --with rashid rashid check "$dir" --live --live-base-url "$PCDS_BASE_URL"
