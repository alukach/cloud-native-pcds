"""No function may bind a name its own module imported.

This bug class has bitten twice. `paths = writer.close()` in `compact.py` made
`compact_period` raise `UnboundLocalError` on the module-level `paths` at its
first line, so compaction never ran; `paths = [...]` in `cli.backfill` did the
same through a closure, so every backfill died on its first station with rows.

Ruff's F823 catches the straight-line case but not the closure one, and both
read as ordinary variable names at a glance. An AST walk catches either.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "pcds"


def _module_level_imports(tree: ast.Module) -> set[str]:
    """Names bound by imports at module scope, which functions must not rebind."""
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names |= {(a.asname or a.name).split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            names |= {a.asname or a.name for a in node.names}
    return names


def _shadowed(path: pathlib.Path) -> list[str]:
    tree = ast.parse(path.read_text())
    imported = _module_level_imports(tree)
    hits = []
    for func in ast.walk(tree):
        if not isinstance(func, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for node in ast.walk(func):
            if (
                isinstance(node, ast.Name)
                and isinstance(node.ctx, ast.Store)
                and node.id in imported
            ):
                hits.append(f"{path.name}:{node.lineno} {func.name}() rebinds '{node.id}'")
    return hits


@pytest.mark.parametrize("path", sorted(SRC.glob("*.py")), ids=lambda p: p.name)
def test_no_function_shadows_a_module_import(path):
    assert _shadowed(path) == []


def test_the_check_would_have_caught_the_real_bug(tmp_path):
    """Falsify it: the shape that shipped twice must fail."""
    bug = tmp_path / "bug.py"
    bug.write_text(
        "from . import paths\n"
        "def backfill(writers):\n"
        "    def flush(period):\n"
        "        return paths.period_prefix(period)\n"
        "    paths = [w.close() for w in writers]\n"
        "    return flush, paths\n"
    )
    assert _shadowed(bug)
