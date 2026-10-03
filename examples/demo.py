"""End-to-end walkthrough with fakes: no network, no keys. Run: python examples/demo.py"""
from __future__ import annotations

import asyncio
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from async_doc_ingestion.chat import ChatAttachmentProcessor
from async_doc_ingestion.models import SubmitRequest
from async_doc_ingestion.pipeline import Pipeline
from async_doc_ingestion.providers import Providers, WorkerCrash
from async_doc_ingestion.query import AttachmentQuery
from async_doc_ingestion.retention import cleanup_chat_attachments
from async_doc_ingestion.service import IngestService
from async_doc_ingestion.specialists import DocumentRouter, FakeSpecialist
from async_doc_ingestion.store import Store
from async_doc_ingestion.worker import InlineRunner


class Deferred:
    """A runner that only queues, so we can play the part of the worker ourselves."""

    def dispatch(self, task: str, job_id: str) -> None:
        pass


def show(title: str) -> None:
    print(f"\n== {title}")


def progress(store: Store, job_id: str) -> str:
    j = store.get_job(job_id)
    stages = " ".join(f"{s}:{v}" for s, v in j.progress.items())
    return f"[{j.status}{' indexed' if j.indexed else ''}] {stages}"


def main() -> None:
    store = Store(str(Path(tempfile.mkdtemp()) / "demo.sqlite"))
    prov = Providers.fakes()
    pipeline = Pipeline(store, prov)
    service = IngestService(store, prov, InlineRunner(pipeline))

    def submit(data: bytes, filename: str, mime: str, tenant: str = "acme-a", **kw):
        req = SubmitRequest(tenant_id=tenant, attachment_id=kw.pop("attachment_id", uuid.uuid4()),
                            source_url=f"s3://acme-workspace/{filename}", filename=filename,
                            mime_type=mime, surface=kw.pop("surface", "task"),
                            surface_id=kw.pop("surface_id", "T-7"), **kw)
        prov.storage.put(req.source_url, data)
        job_id, unchanged = service.submit(req)
        return job_id, unchanged, req

    show("1. Tenant A submits a PDF, an audio recording and a text note")
    pdf_id, _, pdf_req = submit(b"Q3 invoice and payment summary. Budget approved for the roadmap.",
                                "q3-budget.pdf", "application/pdf", labels=["finance"])
    audio_id, _, _ = submit(b"Standup recording: sprint deadline moved, release on Friday.",
                            "standup.mp3", "audio/mpeg", surface="meeting", surface_id="M-3")
    note_id, _, note_req = submit(b"Reminder: review the invoice payment budget before the roadmap meeting.",
                                  "note.txt", "text/plain")
    for name, jid in (("pdf  ", pdf_id), ("audio", audio_id), ("note ", note_id)):
        print(f"{name} {progress(store, jid)}")
    print("links for the pdf:", {k[:8]: v for k, v in store.get_links(str(pdf_req.attachment_id)).items()})

    show("2. Re-submit the PDF unchanged: a hash no-op")
    _, unchanged = service.submit(pdf_req)
    print(f"unchanged={unchanged}  OCR starts={prov.ocr.starts}  embed calls={prov.embedder.calls}")

    show("3. Worker crashes right after the OCR job started; the message is redelivered")
    service.runner = Deferred()
    crash_id, _, _ = submit(b"Contract clause on liability and compliance.",
                                    "contract.pdf", "application/pdf")
    prov.ocr.crash_on_next_poll = True
    starts_before = prov.ocr.starts
    try:
        pipeline.process(crash_id)
    except WorkerCrash as e:
        print(f"worker died: {e}; persisted ocr_job_id={store.get_job(crash_id).ocr_job_id}")
    pipeline.process(crash_id)  # redelivery
    pipeline.index(crash_id)
    print(f"after resume: {progress(store, crash_id)}")
    print(f"OCR started {prov.ocr.starts - starts_before} time(s) for this document")

    show("4. Search tenant A's vectors; tenant B sees nothing")
    q = prov.embedder.embed(["invoice payment budget"])[0]
    for hit in prov.vectors.search("tenant_acme-a", q, k=2):
        print(f"  A hit {hit['score']:.2f}  {hit['text'][:50]!r}")
    print("  B hits:", prov.vectors.search("tenant_acme-b", q, k=2))

    show("5. Delete the text note")
    result = service.delete(str(note_req.attachment_id))
    print("deleted:", result)
    print("note still searchable?", any("Reminder" in h["text"]
                                         for h in prov.vectors.search("tenant_acme-a", q, k=10)))

    show("6. Chat attachments: extracted once, then read from the table")
    chat = ChatAttachmentProcessor(store, prov)
    prov.storage.put("s3://acme-chat/offsite.txt", b"The team offsite is on Friday at the lake house.")
    prov.storage.put("s3://acme-chat/empty.txt", b"  ")
    prov.storage.put("s3://acme-chat/sales.csv", b"region,total\nnorth,10\nsouth,20")
    for att, name, mime in (("c-1", "offsite.txt", "text/plain"), ("c-2", "empty.txt", "text/plain"),
                            ("c-3", "sales.csv", "text/csv")):
        r = chat.process("acme-a", att, f"s3://acme-chat/{name}", name, mime)
        print(f"  {name}: {r.status}")
    downloads = prov.storage.downloads
    query = AttachmentQuery(store, prov, DocumentRouter(FakeSpecialist("sheet"), FakeSpecialist("doc")))

    async def ask_all() -> None:
        for question, att in (("When is the offsite?", "c-1"), ("Anything here?", "c-2"),
                              ("Total sales?", "c-3")):
            a = await query.ask(question, "acme-a", attachment_id=att)
            print(f"  {att}: {a.status} via {a.via or '-'}: {a.text.splitlines()[0]}")
        a = await query.ask("Which document talks about liability?", "acme-a")
        print(f"  workspace search: {a.status} via {a.via}, sources={[s[:8] for s in a.sources]}")

    asyncio.run(ask_all())
    print(f"  files downloaded while answering: {prov.storage.downloads - downloads}")

    show("7. Retention: chat extractions older than 90 days are removed with their vectors")
    store.put_chat("acme-a", "c-1", "offsite.txt", "text/plain", "x",
                   created_at=datetime.now(UTC) - timedelta(days=91))
    print("cleanup:", cleanup_chat_attachments(store, prov.vectors))


if __name__ == "__main__":
    main()
