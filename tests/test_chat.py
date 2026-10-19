from __future__ import annotations

import pytest

from async_doc_ingestion.chat import ChatAttachmentProcessor, FakeFetcher, fetch_webpage
from async_doc_ingestion.models import NO_CONTENT
from async_doc_ingestion.specialists import DocumentRouter, FakeSpecialist


@pytest.fixture
def chat(env):
    return ChatAttachmentProcessor(env.store, env.prov)


def put(env, url, data):
    env.prov.storage.put(url, data.encode() if isinstance(data, str) else data)


def test_audio_is_transcribed_and_returned_inline_not_stored(env, chat):
    put(env, "s3://c/a.mp3", "please book the room")
    r = chat.process("acme", "att-1", "s3://c/a.mp3", "a.mp3", "audio/mpeg")
    assert r.status == "inline" and r.inline_text == "please book the room"
    assert env.store.get_chat("acme", "att-1") is None


def test_file_is_extracted_once_and_never_downloaded_again(env, chat):
    put(env, "s3://c/n.pdf", "meeting notes about the budget")
    assert chat.process("acme", "att-2", "s3://c/n.pdf", "n.pdf", "application/pdf").status == "stored"
    again = chat.process("acme", "att-2", "s3://c/n.pdf", "n.pdf", "application/pdf")
    assert again.status == "already_stored"
    assert env.prov.ocr.starts == 1 and env.prov.storage.downloads == 1
    assert env.store.get_chat("acme", "att-2")["text"] == "meeting notes about the budget"


def test_chat_extraction_is_tenant_scoped(env, chat):
    put(env, "s3://c/n.txt", "private")
    chat.process("acme", "att-3", "s3://c/n.txt", "n.txt", "text/plain")
    assert env.store.get_chat("globex", "att-3") is None


def test_rtf_chat_attachment_is_converted_to_text(env, chat):
    put(env, "s3://c/r.rtf", r"{\rtf1 Quarterly {\b plan}\par}")
    chat.process("acme", "att-4", "s3://c/r.rtf", "r.rtf", "application/rtf")
    assert env.store.get_chat("acme", "att-4")["text"] == "Quarterly plan"


def test_empty_attachment_is_a_hard_stop_and_is_not_indexed(env, chat):
    put(env, "s3://c/e.txt", "   ")
    r = chat.process("acme", "att-5", "s3://c/e.txt", "e.txt", "text/plain")
    assert r.status == "no_content" and r.note == NO_CONTENT
    assert env.prov.embedder.calls == 0


def test_unsupported_type_is_reported_not_guessed(env, chat):
    put(env, "s3://c/x.bin", "zzz")
    assert chat.process("acme", "att-6", "s3://c/x.bin", "x", "application/zip").status == "unsupported"


def test_url_fetch_tool_strips_markup_and_rejects_non_http():
    f = FakeFetcher({"https://example.com/p": "<html><style>x{}</style><h1>Hi</h1><script>1</script>"
                                              "<p>there  world</p></html>"})
    assert fetch_webpage(f, "https://example.com/p") == "Hi there world"
    with pytest.raises(ValueError):
        fetch_webpage(f, "file:///etc/passwd")
    assert f.calls == 1


async def test_spreadsheet_runs_both_specialists_in_parallel():
    events: list[str] = []
    router = DocumentRouter(FakeSpecialist("sheet", events), FakeSpecialist("doc", events))
    ans = await router.ask("total?", "text/csv", "q.csv", "a,b\n1,2")
    assert events[:2] == ["sheet:start", "doc:start"]  # both started before either finished
    assert ans.via == "spreadsheet+document"


async def test_non_spreadsheet_goes_only_to_document_specialist():
    sheet, doc = FakeSpecialist("sheet"), FakeSpecialist("doc")
    ans = await DocumentRouter(sheet, doc).ask("hi?", "application/pdf", "a.pdf", "text")
    assert ans.via == "document" and sheet.calls == [] and len(doc.calls) == 1


async def test_router_never_needs_the_caller_to_declare_a_type():
    sheet, doc = FakeSpecialist("sheet"), FakeSpecialist("doc")
    ans = await DocumentRouter(sheet, doc).ask("q", "application/octet-stream", "Budget.XLSX", "x")
    assert ans.via == "spreadsheet+document"  # decided from the stored filename and mime


async def test_empty_content_stops_before_any_specialist_runs():
    sheet, doc = FakeSpecialist("sheet"), FakeSpecialist("doc")
    ans = await DocumentRouter(sheet, doc).ask("q", "text/csv", "a.csv", "  ")
    assert ans.status == "no_content" and ans.text == NO_CONTENT
    assert sheet.calls == [] and doc.calls == []
