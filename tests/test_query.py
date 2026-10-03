from __future__ import annotations

import asyncio

import pytest

from async_doc_ingestion.chat import ChatAttachmentProcessor
from async_doc_ingestion.context import CallEnvelope, _envelope
from async_doc_ingestion.models import NO_CONTENT
from async_doc_ingestion.query import AttachmentQuery
from async_doc_ingestion.specialists import DocumentRouter, FakeSpecialist


@pytest.fixture
def q(env):
    events: list[str] = []
    sheet, doc = FakeSpecialist("sheet", events), FakeSpecialist("doc", events)
    query = AttachmentQuery(env.store, env.prov, DocumentRouter(sheet, doc))
    query.sheet, query.doc, query.events = sheet, doc, events
    return query


async def test_hybrid_retrieval_finds_the_right_workspace_attachment(env, q):
    env.ingest("invoice payment budget for the finance team")
    _, _, other = env.ingest("server deploy database release notes")
    ans = await q.ask("which deploy release happened?", "acme")
    assert ans.status == "ok" and ans.via == "hybrid"
    assert str(other.attachment_id) in ans.sources
    assert q.hybrid_search("which deploy release happened?", "acme")[0]["attachment_id"] == str(
        other.attachment_id)  # best-ranked chunk comes from the matching document


async def test_keyword_overlap_lifts_a_chunk_dense_alone_would_miss(env, q):
    env.ingest("zebra quokka aardvark")
    hits = q.hybrid_search("quokka", "acme")
    assert hits and "quokka" in hits[0]["text"]


async def test_scoped_query_never_reads_another_attachment(env, q):
    env.ingest("invoice payment budget")
    _, _, b = env.ingest("server deploy database")
    ans = await q.ask("invoice payment", "acme", attachment_id=str(b.attachment_id))
    assert ans.sources == [str(b.attachment_id)]
    assert all("invoice" not in c for _, c in q.doc.calls)


async def test_tenant_boundary_hides_other_tenants_attachments(env, q):
    _, _, req = env.ingest("secret invoice budget", tenant_id="globex")
    ans = await q.ask("invoice?", "acme", attachment_id=str(req.attachment_id))
    assert ans.status == "not_found"
    assert (await q.ask("invoice budget", "acme")).status == "no_match"


async def test_spreadsheet_fast_path_runs_both_specialists_in_parallel(env, q):
    _, _, req = env.ingest("region,total\nnorth,10", mime_type="text/csv", filename="s.csv")
    ans = await q.ask("total?", "acme", attachment_id=str(req.attachment_id))
    assert ans.via == "spreadsheet+document"
    assert q.events[:2] == ["sheet:start", "doc:start"]
    assert env.prov.embedder.calls == 1  # only the paid index call; no retrieval was needed


async def test_empty_workspace_file_is_a_hard_stop_not_another_document(env, q):
    env.ingest("invoice payment budget")  # a tempting different document
    _, _, empty = env.ingest("   ")
    ans = await q.ask("invoice payment budget?", "acme", attachment_id=str(empty.attachment_id))
    assert ans.status == "no_content" and ans.text == NO_CONTENT
    assert q.doc.calls == [] and q.sheet.calls == []


async def test_empty_chat_file_is_a_hard_stop_not_another_document(env, q):
    env.ingest("invoice payment budget")
    env.prov.storage.put("s3://c/e.txt", b" ")
    ChatAttachmentProcessor(env.store, env.prov).process("acme", "c-1", "s3://c/e.txt", "e", "text/plain")
    ans = await q.ask("invoice payment budget?", "acme", attachment_id="c-1")
    assert ans.status == "no_content" and q.doc.calls == []


async def test_chat_attachment_is_answered_from_the_table_without_download(env, q):
    env.prov.storage.put("s3://c/n.txt", b"offsite is on friday")
    ChatAttachmentProcessor(env.store, env.prov).process("acme", "c-2", "s3://c/n.txt", "n", "text/plain")
    ans = await q.ask("when?", "acme", attachment_id="c-2")
    assert ans.status == "ok" and "offsite" in ans.text and env.prov.storage.downloads == 1


async def test_unfinished_attachment_reports_not_ready(env, q):
    from conftest import RecordingRunner

    env.service.runner = RecordingRunner()
    _, _, req = env.ingest("invoice")
    assert (await q.ask("x", "acme", attachment_id=str(req.attachment_id))).status == "not_ready"


async def test_parallel_queries_each_read_their_own_attachment_via_the_envelope(env, q):
    _, _, a = env.ingest("alpha invoice payment")
    _, _, b = env.ingest("beta server deploy")

    async def run(req):
        _envelope.set(CallEnvelope(str(req.attachment_id), "acme"))
        return await q.ask("what is in it?")  # no id passed: the envelope supplies it

    ra, rb = await asyncio.gather(run(a), run(b))
    assert ra.sources == [str(a.attachment_id)] and rb.sources == [str(b.attachment_id)]
    assert "alpha" in ra.text and "beta" in rb.text
