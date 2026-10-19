"""Document routing. One entry point looks at the stored file type and picks the specialists.

The caller never says what kind of file it is: the type comes from the record that was saved
when the file was attached. Spreadsheets go to the spreadsheet specialist AND the document
specialist at the same time; everything else goes to the document specialist.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Protocol

from .models import NO_CONTENT

SPREADSHEET_MIMES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


@dataclass
class Answer:
    status: str  # ok | no_content | not_found | not_ready | no_match
    text: str
    via: str = ""
    sources: list[str] = field(default_factory=list)


class SpreadsheetSpecialist(Protocol):
    async def answer(self, question: str, content: str) -> str: ...


class DocumentSpecialist(Protocol):
    async def answer(self, question: str, content: str) -> str: ...


class FakeSpecialist:
    """Deterministic stand-in for an LLM specialist. `events` records start/end so tests can
    prove two specialists ran at the same time."""

    def __init__(self, name: str, events: list[str] | None = None) -> None:
        self.name, self.events, self.calls = name, events if events is not None else [], []

    async def answer(self, question: str, content: str) -> str:
        self.calls.append((question, content))
        self.events.append(f"{self.name}:start")
        await asyncio.sleep(0)  # yield, so a parallel peer can start before we finish
        self.events.append(f"{self.name}:end")
        first = content.strip().splitlines()[0] if content.strip() else ""
        return f"[{self.name}] {first}"


def is_spreadsheet(mime: str, filename: str) -> bool:
    return mime in SPREADSHEET_MIMES or filename.lower().endswith((".xlsx", ".xls", ".csv"))


class DocumentRouter:
    def __init__(self, spreadsheet: SpreadsheetSpecialist, document: DocumentSpecialist) -> None:
        self.spreadsheet, self.document = spreadsheet, document

    async def ask(self, question: str, mime: str, filename: str, content: str) -> Answer:
        if not content.strip():
            # Hard stop. Answering anyway would mean searching elsewhere and answering from a
            # different document, which is the bug this rule exists to prevent.
            return Answer("no_content", NO_CONTENT)
        if is_spreadsheet(mime, filename):
            sheet, doc = await asyncio.gather(self.spreadsheet.answer(question, content),
                                              self.document.answer(question, content))
            return Answer("ok", f"{sheet}\n{doc}", via="spreadsheet+document")
        return Answer("ok", await self.document.answer(question, content), via="document")
