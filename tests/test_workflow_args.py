"""`_setup.yml` runs `uv run $PCDS_COMMAND` unquoted, and bash word-splits the
result of an expansion without applying quote removal to it. A caller that
writes `--period "${{ ... }}"` therefore ships the quote characters into argv:
`--period ""` became a request to compact a period literally named `""`, which
silently folded nothing and kept the deltas, and `--base-url "https://x"` put
literal quotes into every published STAC href.

This only applies to the `command:` values handed to `_setup.yml`. Quoting in a
real `run:` shell block is correct and must not be flagged. Ruff cannot see
YAML, so this is the guard.
"""

import pathlib
import re

ROOT = pathlib.Path(__file__).parent.parent
WORKFLOWS = sorted((ROOT / ".github/workflows").glob("*.yml"))

# A long flag whose value opens with a quote, e.g. `--base-url "https://x"`.
QUOTED_ARG = re.compile(r"--[\w-]+[ =]\"")


def command_blocks(text: str):
    """Yield (line_number, line) for every line of every `command:` value,
    including the continuation lines of a `>-` folded block."""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        m = re.match(r"^(\s*)command:\s*(.*)$", lines[i])
        if not m:
            i += 1
            continue
        indent, inline = len(m.group(1)), m.group(2).strip()
        if inline and inline not in (">-", ">", "|", "|-"):
            yield i + 1, inline
        i += 1
        while i < len(lines):
            line = lines[i]
            if line.strip() and (len(line) - len(line.lstrip())) <= indent:
                break
            if line.strip():
                yield i + 1, line
            i += 1


def test_command_blocks_are_found():
    found = [b for path in WORKFLOWS for b in command_blocks(path.read_text())]
    assert found, "parsed no command: blocks; the parser above is wrong"
    assert any("pcds compact" in line for _, line in found)


def test_no_quoted_argument_values():
    offenders = [
        f"{path.name}:{n}: {line.strip()}"
        for path in WORKFLOWS
        for n, line in command_blocks(path.read_text())
        if QUOTED_ARG.search(line)
    ]
    assert not offenders, (
        "quoted argument values reach argv with their quotes intact:\n"
        + "\n".join(offenders)
        + "\nEmit the flag only when its value is non-empty and leave the value bare."
    )


def test_setup_runs_the_command_unquoted_and_never_evals_it():
    # The step holds AWS_SECRET_ACCESS_KEY. Unquoted expansion cannot run a
    # command substitution; eval and direct interpolation both can.
    run_lines = [
        line
        for line in (ROOT / ".github/workflows/_setup.yml").read_text().splitlines()
        if re.match(r"^\s*(- )?run:", line)
    ]
    assert any(line.strip().endswith("uv run $PCDS_COMMAND") for line in run_lines)
    assert not any("eval" in line or "${{" in line for line in run_lines)
