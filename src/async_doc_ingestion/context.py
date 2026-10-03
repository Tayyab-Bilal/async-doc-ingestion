"""Per-call context envelope.

A single-valued shared slot ("current attachment") is last-writer-wins when queries run in
parallel: query A reads the id query B just wrote. Each call instead carries its own envelope in a
ContextVar (asyncio tasks get a copy of the context, so tasks cannot see each other's values).
Precedence: explicit argument, then per-call envelope, then shared default.
"""
from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from .store import Store


@dataclass(frozen=True)
class CallEnvelope:
    attachment_id: str
    tenant: str


_envelope: ContextVar[CallEnvelope | None] = ContextVar("call_envelope", default=None)


def current_envelope() -> CallEnvelope | None:
    return _envelope.get()


def resolve(explicit: Any, envelope: Any, shared: Any) -> Any:
    for value in (explicit, envelope, shared):
        if value is not None:
            return value
    return None


async def answer_from_attachment(store: Store, question: str, *, attachment_id: str | None = None,
                                 shared_default: str | None = None) -> dict:
    """A stand-in for an LLM tool: it never receives the attachment id as a parameter, it looks
    it up from the ambient envelope, as tools called by an agent do."""
    env = current_envelope()
    att = resolve(attachment_id, env.attachment_id if env else None, shared_default)
    await asyncio.sleep(0)  # the tool 'thinks'; other queries run meanwhile
    doc = store.get_extracted(att) if att else None
    return {"question": question, "attachment_id": att, "text": doc["text"] if doc else None}


async def query_attachments(store: Store, queries: list[tuple[str, str, str]]) -> list[dict]:
    """queries: (question, attachment_id, tenant). Runs in parallel, one envelope each."""

    async def one(question: str, attachment_id: str, tenant: str) -> dict:
        _envelope.set(CallEnvelope(attachment_id, tenant))  # task-local: gather wraps in a task
        return await answer_from_attachment(store, question)

    return list(await asyncio.gather(*(one(*q) for q in queries)))
