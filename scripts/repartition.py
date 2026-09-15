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

    uv run python scripts/repartition.py --root ./data [--apply]

Dry run by default: it prints the moves and changes nothing.
"""

from __future__ import annotations

import argparse

from pcds import compact, paths, storage
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


def plan_moves(layout, written: dict[str, list[str]]) -> dict[str, list[str]]:
    """new period -> old period labels that must fold into it.

    Raises if an old period is not wholly contained in one new period; that is
    the case this script is not allowed to guess at.
    """
    moves: dict[str, list[str]] = {}
    for label in sorted(written):
        lo, hi = parse_period(label)
        holders = {p.period for p in layout.periods if p.contains(lo) or p.contains(hi)}
        if len(holders) != 1:
            raise SystemExit(
                f"period={label} spans {sorted(holders) or ['no planned period']} "
                "in the new layout. Boundaries crossed, so those years have to be "
                "re-fetched; this script will not guess. Nothing was changed."
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
    moves = plan_moves(layout, written)

    if not moves:
        print("Every written partition already matches the layout. Nothing to do.")
        return

    for target, sources in sorted(moves.items()):
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
