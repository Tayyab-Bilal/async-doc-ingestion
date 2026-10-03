# Usage guide

Every Python block in this file runs as written. They share one namespace, top to bottom, and
`tests/test_docs.py` executes them. Nothing touches a network: all providers are fakes.

## 1. Wire the service

```python
import asyncio
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from async_doc_ingestion.models import SubmitRequest
from async_doc_ingestion.pipeline import Pipeline
from async_doc_ingestion.providers import Providers
from async_doc_ingestion.service import IngestService
from async_doc_ingestion.store import Store
from async_doc_ingestion.worker import InlineRunner

store = Store(str(Path(tempfile.mkdtemp()) / "ingest.sqlite"))
prov = Providers.fakes()          # fake storage, OCR, transcriber, tagger, embedder, vector store
pipeline = Pipeline(store, prov)
service = IngestService(store, prov, InlineRunner(pipeline))  # inline: runs the job on submit


def submit(text: str, name: str = "note.txt", mime: str = "text/plain", **over):
    """Put `text` in fake storage and submit it. Returns (job_id, unchanged, request)."""
    req = SubmitRequest(
        **{"tenant_id": "acme", "attachment_id": uuid.uuid4(), "source_url": f"s3://docs/{name}",
           "filename": name, "mime_type": mime, "surface": "task", "surface_id": "T-1", **over})
    prov.storage.put(req.source_url, text.encode())
    job_id, unchanged = service.submit(req)
    return job_id, unchanged, req
```

## 2. Submit, poll, re-submit

```python
job_id, unchanged, req = submit("Invoice payment and budget for the Q3 roadmap", "q3.txt",
                                labels=["finance"])
job = service.status(job_id)
assert job.status == "completed" and job.indexed
print(job.progress)  # per-stage progress: download ... link, index

_, unchanged = service.submit(req)  # same bytes, same URL, same labels: nothing re-runs
assert unchanged is True
```

An unchanged re-upload returns `unchanged=True` (HTTP 200) and no stage runs. See
[api.md](api.md) for the hash and for what a changed URL does.

## 3. Source surfaces

Seven surfaces are accepted: `task`, `project`, `meeting`, `communication_hub`, `compliance`,
`space`, `email`. The entity-context fake returns different fields for each.

```python
from async_doc_ingestion.models import Surface

for surface in Surface.__args__:
    _, _, r = submit(f"note about {surface}", f"{surface}.txt", surface=surface)
    ctx = store.get_meta(str(r.attachment_id), "entity")
    assert ctx["surface"] == surface
    print(surface, sorted(set(ctx) - {"surface", "title", "tenant"}))
```

`chat` is not a surface. Chat files use the chat processor (section 6).

## 4. Links: how the cascade decides

For each other attachment of the same tenant, three signals are tried in order. The first one that
applies and reaches 0.4 creates the link, and its score is stored.

1. Keyword Jaccard (needs keywords on both sides).
2. Label Jaccard (needs labels on both sides).
3. Exact metadata match (needs non-empty metadata on both sides; score 1.0).

A signal below 0.4 falls through to the next one. Links are written in both directions.

```python
from async_doc_ingestion.linking import cascade_score

assert cascade_score({"a", "b"}, {"a", "b"}, set(), set(), None, None) == ("keyword", 1.0)
assert cascade_score({"a"}, {"b"}, {"q1"}, {"q1"}, None, None) == ("label", 1.0)
assert cascade_score({"a"}, {"b"}, set(), set(), {"id": 7}, {"id": 7}) == ("metadata", 1.0)
assert cascade_score({"a"}, {"b"}, set(), set(), None, None) is None

_, _, a = submit("alpha beta gamma delta", "a.txt", metadata={"contract": "C-9"})
_, _, b = submit("omicron sigma tau upsilon", "b.txt", metadata={"contract": "C-9"})
assert str(b.attachment_id) in store.get_links(str(a.attachment_id))
assert str(a.attachment_id) in store.get_links(str(b.attachment_id))
```

## 5. Domain and subdomain

```python
from async_doc_ingestion.pipeline import lookup_domain, normalize

assert lookup_domain(set(normalize("invoice and release deadline"))) == [
    ("engineering", "delivery"), ("finance", "invoicing"), ("planning", "scheduling")]
```

The stage stores two tag kinds: `domain` (`finance`) and `subdomain` (`finance/invoicing`).

## 6. Chat attachments

Files dropped into a chat message use `ChatAttachmentProcessor`. Audio is transcribed and returned
inline. Other files are extracted once into a table; later questions read that table.

```python
from async_doc_ingestion.chat import ChatAttachmentProcessor, FakeFetcher, fetch_webpage

chat = ChatAttachmentProcessor(store, prov)
prov.storage.put("s3://chat/memo.rtf", rb"{\rtf1 Offsite is on {\b Friday}.\par}")
prov.storage.put("s3://chat/voice.mp3", b"please move the standup")
prov.storage.put("s3://chat/blank.txt", b"   ")

assert chat.process("acme", "c-1", "s3://chat/memo.rtf", "memo.rtf", "application/rtf").status == "stored"
voice = chat.process("acme", "c-2", "s3://chat/voice.mp3", "voice.mp3", "audio/mpeg")
assert voice.status == "inline" and voice.inline_text == "please move the standup"
blank = chat.process("acme", "c-3", "s3://chat/blank.txt", "blank.txt", "text/plain")
assert blank.status == "no_content"          # "no readable content", never indexed
assert store.get_chat("acme", "c-1")["text"] == "Offsite is on Friday."

pages = FakeFetcher({"https://example.com/news": "<h1>Launch</h1><script>x()</script><p>Friday</p>"})
assert fetch_webpage(pages, "https://example.com/news") == "Launch Friday"
```

## 7. Ask questions

`AttachmentQuery` answers over one attachment (workspace or chat) or over the whole tenant.

```python
from async_doc_ingestion.query import AttachmentQuery
from async_doc_ingestion.specialists import DocumentRouter, FakeSpecialist

query = AttachmentQuery(store, prov, DocumentRouter(FakeSpecialist("sheet"), FakeSpecialist("doc")))


async def demo():
    a = await query.ask("When is the offsite?", "acme", attachment_id="c-1")   # chat file
    assert a.status == "ok" and "Offsite" in a.text
    a = await query.ask("anything?", "acme", attachment_id="c-3")              # empty chat file
    assert a.status == "no_content" and a.text == "no readable content"
    a = await query.ask("budget for the roadmap?", "acme")                     # hybrid search
    assert a.status == "ok" and a.via == "hybrid" and a.sources
    return a


print(asyncio.run(demo()).text)
```

A spreadsheet goes to the spreadsheet specialist and the document specialist at the same time:

```python
_, _, sheet = submit("region,total\nnorth,10", "sales.csv", "text/csv")
answer = asyncio.run(query.ask("total?", "acme", attachment_id=str(sheet.attachment_id)))
assert answer.via == "spreadsheet+document"
```

### Parallel queries and the context envelope

If you run several queries at once and do not pass an attachment id, each task sets its own
envelope, so tasks never read each other's file.

```python
from async_doc_ingestion.context import CallEnvelope, _envelope


async def in_envelope(attachment_id: str):
    _envelope.set(CallEnvelope(attachment_id, "acme"))   # task-local
    return await query.ask("what is this about?")


async def both():
    return await asyncio.gather(in_envelope("c-1"), in_envelope(str(sheet.attachment_id)))


first, second = asyncio.run(both())
assert first.via == "document" and second.via == "spreadsheet+document"
```

## 8. Self-healing sweep

```python
from async_doc_ingestion.sweep import sweep

assert sweep(store, service.runner, datetime.now(UTC) + timedelta(minutes=20)) == []  # all indexed
```

The sweep only touches jobs that are `completed` but not `indexed`, are at least 15 minutes old,
have fewer than 5 attempts, in batches of 50. In production it runs from Celery beat every 10
minutes (`ingest.sweep`).

## 9. Retention

```python
from async_doc_ingestion.retention import cleanup_chat_attachments

store.put_chat("acme", "c-1", "memo.rtf", "application/rtf", "old text",
               created_at=datetime.now(UTC) - timedelta(days=91))
print(cleanup_chat_attachments(store, prov.vectors))   # {'extractions': 1, 'vectors': 1}
assert store.get_chat("acme", "c-1") is None
```

Workspace attachments are never expired. Beat runs `ingest.cleanup` daily.

## 10. Delete

```python
result = service.delete(str(req.attachment_id))
assert result["vectors"] == 1 and result["rows"]["jobs"] == 1
```

## 11. Metering the embedder

A wrapper around the embedder must subclass the base class the vector-store library checks.

```python
from async_doc_ingestion.metering import MeteredEmbedder, StrictVectorStore

metered = MeteredEmbedder(prov.embedder)
assert StrictVectorStore(metered).add_texts(["a", "b"]) == 2 and metered.credits == 2
```

## 12. Real Celery

```bash
pip install -e ".[celery]"
# at worker start-up, call async_doc_ingestion.celery_app.configure(Pipeline(store, real_providers))
celery -A async_doc_ingestion.celery_app worker --loglevel=info
celery -A async_doc_ingestion.celery_app beat --loglevel=info   # sweep every 10 min, cleanup daily
```
