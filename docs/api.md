# API reference

Public classes and functions, then the HTTP routes. Python blocks here run in `tests/test_docs.py`.

## Contract rules

- Submit never blocks: it returns a job id (HTTP 202) and the work happens on a worker.
- Status vocabulary is closed: `queued`, `running`, `completed`, `failed`. There is no
  "degraded" state: a job that finished but was not indexed stays `completed` with
  `indexed=false`, and the sweep finishes it. Adding a state would break clients that only know
  these four.
- Stage progress values: `pending`, `running`, `done`, `skipped`.
- Stages, in order: `download`, `extract`, `tag`, `normalize`, `classify`, `metadata`, `link`,
  `index`. (`normalize` is a toy suffix stemmer standing in for a lemmatizer; `classify` is the
  domain and subdomain lookup.)
- Empty documents finish as `completed` with `note="no readable content"`. They are never
  indexed and never swept.
- Re-ingest hash: SHA-256 over metadata, sorted labels and the content hash. Here "source" means
  the file bytes; production compares the object ETag or version instead.

## Models (`models.py`)

| Name | Description |
|---|---|
| `Surface` | `task`, `project`, `meeting`, `communication_hub`, `compliance`, `space`, `email` |
| `Status` | `queued`, `running`, `completed`, `failed` |
| `STAGES` | the eight stage names above |
| `NO_CONTENT` | the string `"no readable content"` |
| `SubmitRequest` | `tenant_id: str`, `attachment_id: UUID`, `source_url: str`, `filename: str`, `mime_type: str`, `surface: Surface`, `surface_id: str`, `labels: list[str] = []`, `metadata: dict = {}` |
| `Job` | `job_id`, `attachment_id`, `tenant_id`, `status`, `stage`, `progress`, `payload_hash`, `ocr_job_id`, `attempts`, `indexed`, `note`, `created_at`, `updated_at` |

## Service (`service.py`)

| Signature | Description |
|---|---|
| `IngestService(store, providers, runner)` | Submit/status/delete logic, no HTTP. |
| `submit(req: SubmitRequest) -> tuple[str, bool]` | `(job_id, unchanged)`. Unchanged means nothing was dispatched. A changed URL or changed bytes wipes derived state and vectors but keeps job history. |
| `status(job_id: str) -> Job \| None` | Current job row. |
| `delete(attachment_id: str) -> dict \| None` | `{"rows": {table: count}, "vectors": n}`, or `None` if unknown. |

## Pipeline (`pipeline.py`)

| Signature | Description |
|---|---|
| `Pipeline(store, providers)` | Runs the stages. Each stage is resumable from the job row. |
| `process(job_id: str) -> None` | Run unfinished stages. A crash leaves the job `running` so redelivery resumes. |
| `index(job_id: str) -> bool` | Chunk, embed and upsert. `True` if it did the paid work. |
| `lookup_domain(keywords: set[str]) -> list[tuple[str, str]]` | `(domain, subdomain)` pairs. |
| `normalize(text: str) -> list[str]` | Lowercase, stopwords out, toy suffix stemming. |

Supported types: audio (`audio/*`, transcribed), PDF, PPTX and images (OCR), RTF, `text/*`.
Anything else fails with `unsupported mime type`.

## Linking (`linking.py`)

| Signature | Description |
|---|---|
| `THRESHOLD = 0.4` | Minimum score for any signal. |
| `jaccard(a: set, b: set) -> float` | Set overlap. |
| `cascade_score(kw_a, kw_b, labels_a, labels_b, meta_a, meta_b) -> tuple[str, float] \| None` | First applicable signal at or above the threshold: `("keyword" \| "label" \| "metadata", score)`. |
| `compute_links(store, attachment_id, tenant_id) -> list[tuple[str, float]]` | Same-tenant links, best first. |

```python
from async_doc_ingestion.linking import THRESHOLD, cascade_score, jaccard

assert THRESHOLD == 0.4 and jaccard({"a", "b"}, {"b", "c"}) == 1 / 3
assert cascade_score({"a"}, {"b"}, {"x"}, {"y"}, {"k": 1}, {"k": 1}) == ("metadata", 1.0)
```

## Chat attachments (`chat.py`)

| Signature | Description |
|---|---|
| `ChatAttachmentProcessor(store, providers)` | Phase 1 processor. |
| `process(tenant_id, attachment_id, url, filename, mime) -> ChatResult` | `status` is `inline` (audio; text in `inline_text`), `stored`, `already_stored`, `no_content` or `unsupported`. |
| `fetch_webpage(fetcher, url) -> str` | URL tool: http(s) only, tags and scripts removed. |
| `FakeFetcher(pages: dict[str, str])` | Test fetcher with a `calls` counter. |

## Routing (`specialists.py`)

| Signature | Description |
|---|---|
| `DocumentRouter(spreadsheet, document)` | One entry point; the caller never declares a type. |
| `await ask(question, mime, filename, content) -> Answer` | Empty content: hard stop. Spreadsheet: both specialists via `asyncio.gather`. Else: document specialist. |
| `Answer(status, text, via="", sources=[])` | `status`: `ok`, `no_content`, `not_found`, `not_ready`, `no_match`. |
| `FakeSpecialist(name, events=None)` | Deterministic specialist; `events` records start and end. |
| `is_spreadsheet(mime, filename) -> bool` | Type check from stored metadata. |

## Query (`query.py`)

| Signature | Description |
|---|---|
| `AttachmentQuery(store, providers, router, top_k=3)` | Phase 3. |
| `await ask(question, tenant=None, *, attachment_id=None, shared_default=None) -> Answer` | Resolves the attachment (explicit, then envelope, then shared default). Fast path first; then hybrid retrieval. |
| `hybrid_search(question, tenant, att=None) -> list[dict]` | `0.5 * dense + 0.5 * keyword overlap`, workspace chunks only. |

## Context (`context.py`)

`CallEnvelope(attachment_id, tenant)`, `current_envelope()`, `resolve(explicit, envelope, shared)`
(first value that is not `None`), `query_attachments(store, queries)` (parallel demo helper).

## Retention, sweep, metering

| Signature | Description |
|---|---|
| `cleanup_chat_attachments(store, vectors, now=None, days=90) -> dict` | `{"extractions": n, "vectors": m}`. |
| `sweep(store, runner, now=None, *, min_age=15min, max_attempts=5, batch=50) -> list[str]` | Atomically claims completed-but-unindexed jobs, dispatches `index`, returns the job ids. |
| `MeteredEmbedder(inner)` | Counts credits; subclasses `EmbeddingsBase`. |
| `StrictVectorStore(embedding)` | Library stand-in; raises `TypeError` unless `embedding` is an `EmbeddingsBase`. |

## Providers (`providers.py`)

Protocols: `Storage`, `OCRProvider`, `Transcriber`, `Tagger`, `EntityContext`, `Embedder`,
`VectorStore`. Fakes: `FakeStorage` (`downloads`), `FakeOCR` (`starts`, `crash_on_next_poll`),
`FakeTranscriber`, `FakeTagger`, `FakeEntityContext`, `FakeEmbedder` (`calls`),
`InMemoryVectorStore`. `Providers.fakes()` builds the set. `collection_for(tenant)` returns
`tenant_{id}`.

## Celery (`celery_app.py`, optional extra)

Tasks `ingest.process`, `ingest.index`, `ingest.sweep`, `ingest.cleanup`. Beat: sweep every 600
seconds, cleanup every 86400. Settings: `task_acks_late`, `task_reject_on_worker_lost`,
`max_retries=0`.

## HTTP routes (`api.py`)

Build the app with `create_app(service)` (needs the `api` extra).

| Route | Success | Errors |
|---|---|---|
| `POST /ingest` (body: `SubmitRequest`) | `202 {"job_id"}`; `200 {"job_id", "unchanged": true}` for a no-op | `422` invalid body or surface |
| `GET /ingest/{job_id}` | `200` the `Job` | `404` unknown job; `422` id is not a UUID |
| `DELETE /attachments/{attachment_id}` | `200 {"rows": {...}, "vectors": n}` | `404` unknown; `422` not a UUID |

```bash
curl -X POST localhost:8000/ingest -H 'content-type: application/json' -d '{
  "tenant_id": "acme", "attachment_id": "0b0f7f0e-8c1d-4c55-9a53-0c3d5c8f6a11",
  "source_url": "s3://docs/a.pdf", "filename": "a.pdf", "mime_type": "application/pdf",
  "surface": "task", "surface_id": "T-1"}'
```

```python
import tempfile
import uuid
from pathlib import Path

from fastapi.testclient import TestClient

from async_doc_ingestion.api import create_app
from async_doc_ingestion.pipeline import Pipeline
from async_doc_ingestion.providers import Providers
from async_doc_ingestion.service import IngestService
from async_doc_ingestion.store import Store
from async_doc_ingestion.worker import InlineRunner

store, prov = Store(str(Path(tempfile.mkdtemp()) / "api.sqlite")), Providers.fakes()
client = TestClient(create_app(IngestService(store, prov, InlineRunner(Pipeline(store, prov)))))
prov.storage.put("s3://docs/a.txt", b"invoice payment budget")
body = {"tenant_id": "acme", "attachment_id": str(uuid.uuid4()), "source_url": "s3://docs/a.txt",
        "filename": "a.txt", "mime_type": "text/plain", "surface": "email", "surface_id": "E-1"}

first = client.post("/ingest", json=body)
assert first.status_code == 202
job_id = first.json()["job_id"]
assert client.get(f"/ingest/{job_id}").json()["status"] == "completed"
again = client.post("/ingest", json=body)
assert again.status_code == 200 and again.json()["unchanged"] is True
assert client.get("/ingest/not-a-uuid").status_code == 422
assert client.get(f"/ingest/{uuid.uuid4()}").status_code == 404
assert client.post("/ingest", json={**body, "surface": "chat"}).status_code == 422
assert client.delete(f"/attachments/{body['attachment_id']}").json()["vectors"] == 1
assert client.delete(f"/attachments/{body['attachment_id']}").status_code == 404
```
