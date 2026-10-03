"""Executes every ```python block in README.md and docs/*.md, so documented code cannot rot.

Blocks in one file share a namespace and run top to bottom, like a notebook.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
FILES = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_python_blocks_in_docs_run(path):
    blocks = re.findall(r"```python\n(.*?)```", path.read_text(), re.DOTALL)
    assert blocks, f"{path.name} has no python examples"
    ns: dict = {"__name__": "__docs__"}
    for i, code in enumerate(blocks, 1):
        exec(compile(code, f"{path.name}[block {i}]", "exec"), ns)  # noqa: S102
