#!/usr/bin/env bash
# Backfill the whole archive, one layout period at a time, 8 shards per period.
#
# One period per run is not tidiness, it is the safety property. `backfill`
# names its output s<shard>-<NNNNN>.parquet with the index reset every run, so
# two runs that both touch a period overwrite each other's files rather than
# adding to them. Taking start/end straight from the period means a run can
# never cover half of one, which is the only way that overwrite loses rows.
#
# Restartable: a compacted partition is a finished one, so re-running skips
# what is already done. Safe to interrupt between periods.
#
# No `set -e`: a failing shard is handled explicitly below, and xargs returns
# 123 for "some invocation failed", which is worth a message rather than a
# silent exit.
set -uo pipefail

export PCDS_ROOT="${PCDS_ROOT:-./data}"
# Per process. These multiply by the number of parallel shards: 8 x 1.5 is
# twice what .github/workflows/backfill.yml puts on PCIC. See the Constraints
# section of CLAUDE.md before raising either.
export PCDS_CONCURRENCY="${PCDS_CONCURRENCY:-3}"
export PCDS_MAX_RPS="${PCDS_MAX_RPS:-1.5}"
SHARDS="${SHARDS:-8}"

mkdir -p logs

while read -r period y0 y1; do
    if compgen -G "$PCDS_ROOT/observations/period=$period/part-*.parquet" >/dev/null; then
        echo "== $period: done, skipping"
        continue
    fi
    echo "== $period ($y0-$y1) starting $(date +%H:%M:%S)"
    if ! seq 0 $((SHARDS - 1)) | xargs -P "$SHARDS" -I{} sh -c \
        "uv run pcds backfill --start-year $y0 --end-year $y1 \
         --shard {}/$SHARDS >logs/$period-s{}.log 2>&1"; then
        echo "!! $period: a shard failed, see logs/$period-s*.log"
        exit 1
    fi
    # Compacting here does double duty: it merges the per-shard files, and the
    # part-*.parquet it leaves behind is the marker the skip above looks for.
    uv run pcds compact --period "$period" || {
        echo "!! $period: compact failed"
        exit 1
    }
done < <(python3 -c "
import json
for p in json.load(open('$PCDS_ROOT/layout.json'))['periods']:
    print(p['period'], p['start_year'], p['end_year'])
")

uv run pcds catalog
