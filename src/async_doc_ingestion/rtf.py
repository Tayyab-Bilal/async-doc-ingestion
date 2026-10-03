r"""Plain-text extraction from RTF without a dependency.

Simplification: handles text, paragraphs, tabs, hex escapes and skips font/colour tables and
`{\*...}` groups. No tables, no non-Latin code pages; production uses a full RTF parser.
"""
from __future__ import annotations

import re

_SKIP = {"fonttbl", "colortbl", "stylesheet", "info", "pict"}
_GROUP = re.compile(r"\{\s*(\\\*)?\s*\\?([a-zA-Z]+)?")
_TOKEN = re.compile(r"\\([a-zA-Z]+)-?\d* ?|\\'([0-9a-fA-F]{2})|\\(.)", re.DOTALL)


def rtf_to_text(rtf: str) -> str:
    out: list[str] = []
    depth, skip_from, i = 0, None, 0
    while i < len(rtf):
        ch = rtf[i]
        if ch == "{":
            depth += 1
            m = _GROUP.match(rtf, i)
            if skip_from is None and m and (m.group(1) or m.group(2) in _SKIP):
                skip_from = depth
            i += 1
        elif ch == "}":
            if skip_from == depth:
                skip_from = None
            depth -= 1
            i += 1
        elif ch == "\\":
            m = _TOKEN.match(rtf, i)
            if m is None:  # lone trailing backslash
                break
            word, hexv, sym = m.groups()
            i = m.end()
            if skip_from is not None:
                continue
            if word in ("par", "line"):
                out.append("\n")
            elif word == "tab":
                out.append("\t")
            elif hexv:
                out.append(chr(int(hexv, 16)))
            elif sym and sym in "\\{}":
                out.append(sym)
        else:
            if skip_from is None and ch not in "\r\n":
                out.append(ch)
            i += 1
    return re.sub(r"\n{3,}", "\n\n", "".join(out)).strip()
