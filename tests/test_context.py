from __future__ import annotations

import asyncio

from async_doc_ingestion.context import (
    CallEnvelope,
    _envelope,
    answer_from_attachment,
    query_attachments,
    resolve,
)


def test_resolve_precedence():
    assert resolve("explicit", "env", "shared") == "explicit"
    assert resolve(None, "env", "shared") == "env"
    assert resolve(None, None, "shared") == "shared"
    assert resolve(None, None, None) is None


async def test_envelope_beats_shared_default_but_explicit_beats_envelope(env):
    _, _, a = env.ingest("document about alpha")
    _, _, b = env.ingest("document about beta")
    _envelope.set(CallEnvelope(str(a.attachment_id), "acme"))
    from_env = await answer_from_attachment(env.store, "q", shared_default=str(b.attachment_id))
    explicit = await answer_from_attachment(env.store, "q", attachment_id=str(b.attachment_id))
    assert "alpha" in from_env["text"] and "beta" in explicit["text"]


async def test_parallel_queries_do_not_cross_contaminate(env):
    _, _, a = env.ingest("document about alpha")
    _, _, b = env.ingest("document about beta")
    att_a, att_b = str(a.attachment_id), str(b.attachment_id)

    results = await query_attachments(env.store, [("qa", att_a, "acme"), ("qb", att_b, "acme")])
    assert [r["attachment_id"] for r in results] == [att_a, att_b]
    assert "alpha" in results[0]["text"] and "beta" in results[1]["text"]

    # The deliberately naive design: one shared single-valued slot, last writer wins.
    slot: dict[str, str] = {}

    async def naive_query(att: str) -> str:
        slot["attachment_id"] = att
        await asyncio.sleep(0)  # the other query writes its id here
        return slot["attachment_id"]

    naive = await asyncio.gather(naive_query(att_a), naive_query(att_b))
    assert naive == [att_b, att_b]  # query A silently read B's attachment: the bug we avoid
