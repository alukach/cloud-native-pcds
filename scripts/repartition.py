"""Move already-written partitions into the periods a new layout.json asks for.

`pcds layout` re-plans the period map, but nothing moves rows to follow it:
compaction only ever rewrites a period in place. Re-planning therefore orphans
every written partition whose label left the plan, and the documented recovery
is to re-fetch those years from PCIC.

That is the right answer when boundaries cross. It is far too expensive when
they only ever get coarser, which is the usual case: a floor that went up merges
old periods without splitting any of them, so every orphaned partition nests
whole inside exactly one new period and the rows are already on disk. This moves
them there and lets `compact_period` do the dedupe, the sort and the row-group
sizing, so nothing is re-fetched.

It refuses to touch anything if that nesting does not hold, because a written
period straddling two new ones genuinely does need the years re-fetched.

Nesting is judged on the rows, never on the period label. A label is only an
upper bound on what the partition holds: a walk that stopped early, or data
dropped afterwards, leaves `period=1872-1999` holding nothing after 1997, and
comparing the label to a re-plan that now says `1872-1998` would refuse a move
that is perfectly safe.

    uv run pcds layout                                  # once, not per run
    uv run python scripts/repartition.py --apply
    uv run pcds catalog

Dry run by default: it prints the moves and changes nothing.

Run `pcds layout` once and leave it alone. It re-plans from scratch every time,
and the plan shifts as the walk writes more years: with data to 1998 the sparse
head came out as `1872-1999`, and after 1998 was dropped the same command said
`1872-1998`. Chaining the two means every run can re-cut boundaries under data
that is already on disk. Plan, then move, then let the walk continue.

Safe to re-run, including after an interrupt. Moves land before the fold, so a
run killed in between leaves the pieces under the target label; the next run
sees a partition in more than one file and finishes the fold.
"""

from __future__ import annotations

import argparse

import pyarrow.parquet as pq

from pcds import compact, paths, storage
from pcds.catalog import _stat
from pcds.cli import _layout  # the same loader `pcds layout` writes through
from pcds.config import Settings


def written_periods(store) -> dict[str, list[str]]:
    """period label -> its parquet paths, for what is on disk now."""
    out: dict[str, list[str]] = {}
    for info in store.ls(paths.OBSERVATIONS, recursive=True):
        if "period=" not in info.path or not info.path.endswith(".parquet"):
            continue
        label = info.path.rsplit("period=", 1)[-1].split("/")[0]
        out.setdefault(label, []).append(info.path)
    return out


def parse_period(label: str) -> tuple[int, int]:
    lo, _, hi = label.partition("-")
    return int(lo), int(hi or lo)


def data_years(store, files: list[str]) -> tuple[int, int] | None:
    """The real year span of a partition, from `obs_time` footer statistics.

    Footers rather than a scan: this has to stay cheap on a remote store, and
    min/max per row group is exactly what is needed. Returns None for a
    partition with no usable statistics, which the caller treats as "trust the
    label" rather than guessing.
    """
    lo = hi = None
    for path in files:
        with store.fs.open_input_file(path) as f:
            md = pq.ParquetFile(f).metadata
            a, b = _stat(md, list(md.schema.names).index("obs_time"), "obs_time")
        if a is None or b is None:
            continue
        lo = a if lo is None else min(lo, a)
        hi = b if hi is None else max(hi, b)
    return (lo.year, hi.year) if lo is not None else None


def plan_moves(layout, written: dict[str, list[str]], spans: dict) -> dict[str, list[str]]:
    """new period -> old period labels that must fold into it.

    Raises if an old period is not wholly contained in one new period; that is
    the case this script is not allowed to guess at.
    """
    moves: dict[str, list[str]] = {}
    for label in sorted(written):
        lo, hi = spans[label] or parse_period(label)
        holders = {p.period for p in layout.periods if p.contains(lo) or p.contains(hi)}
        if len(holders) != 1:
            raise SystemExit(
                f"period={label} holds data for {lo}-{hi}, which spans "
                f"{sorted(holders) or ['no planned period']} in the new layout. "
                "Boundaries crossed, so those years have to be re-fetched; this "
                "script will not guess. Nothing was changed."
            )
        target = holders.pop()
        if target != label:
            moves.setdefault(target, []).append(label)
    return moves


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None)
    ap.add_argument("--apply", action="store_true", help="Without this, dry run.")
    args = ap.parse_args()

    settings = Settings()
    store = storage.open_store(settings, root=args.root)
    layout = _layout(store)
    written = written_periods(store)
    spans = {label: data_years(store, files) for label, files in written.items()}
    moves = plan_moves(layout, written, spans)

    # A partition already under the right label but still in pieces is an
    # interrupted earlier run: its moves landed and its compaction did not.
    # Folding it is the rest of that job, so finish it rather than reporting
    # nothing to do and leaving the pieces behind.
    touched = set(moves) | {src for srcs in moves.values() for src in srcs}
    for label, files in written.items():
        if label not in touched and len(files) > 1:
            moves[label] = []

    if not moves:
        print("Every written partition already matches the layout. Nothing to do.")
        return

    for target, sources in sorted(moves.items()):
        if not sources:
            print(f"period={target}  <-  fold {len(written[target])} file(s) already in place")
            continue
        n = sum(len(written[s]) for s in sources)
        print(f"period={target}  <-  {n} file(s) from {', '.join(sources)}")
    if not args.apply:
        print("\nDry run. Re-run with --apply to move them.")
        return

    for target, sources in sorted(moves.items()):
        dest_prefix = paths.period_prefix(target)
        store.mkdirs(dest_prefix)
        for src_label in sources:
            for path in written[src_label]:
                base = path.rsplit("/", 1)[-1]
                # Old periods all name their file part-00000.parquet, so the
                # source label has to survive into the name or they collide.
                store.fs.move(path, store.join(dest_prefix, f"{src_label}-{base}"))
            store.delete(paths.period_prefix(src_label))
        stats = compact.compact_period(
            store, settings, target, parse_period(target), include_deltas=False
        )
        print(f"period={target}: {stats['rows']:,} rows -> {stats['files']} file(s)")
    print("\nDone. Re-run `pcds catalog` so the manifest matches.")


if __name__ == "__main__":
    main()
