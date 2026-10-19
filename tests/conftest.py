from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import pytest

from async_doc_ingestion.models import SubmitRequest
from async_doc_ingestion.pipeline import Pipeline
from async_doc_ingestion.providers import Providers
from async_doc_ingestion.service import IngestService
from async_doc_ingestion.store import Store
from async_doc_ingestion.worker import InlineRunner


class RecordingRunner:
    """Queues instead of running, so tests control exactly when a worker 'picks up' a job."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def dispatch(self, task: str, job_id: str) -> None:
        self.calls.append((task, job_id))


@dataclass
class Env:
    store: Store
    prov: Providers
    pipeline: Pipeline
    inline: InlineRunner
    service: IngestService
    _n: int = field(default=0)

    def req(self, **over) -> SubmitRequest:
        base = {"tenant_id": "acme", "attachment_id": uuid.uuid4(), "source_url": "s3://docs/a.pdf",
                "filename": "a.pdf", "mime_type": "application/pdf", "surface": "task",
                "surface_id": "T-1", "labels": [], "metadata": {}}
        return SubmitRequest(**{**base, **over})

    def ingest(self, data: bytes | str, **over) -> tuple[str, bool, SubmitRequest]:
        """Store `data` at a fresh URL (unless given) and submit it."""
        self._n += 1
        over.setdefault("source_url", f"s3://docs/f{self._n}")
        over.setdefault("mime_type", "text/plain")
        req = self.req(**over)
        self.prov.storage.put(req.source_url, data.encode() if isinstance(data, str) else data)
        job_id, unchanged = self.service.submit(req)
        return job_id, unchanged, req


@pytest.fixture
def env(tmp_path) -> Env:
    store, prov = Store(str(tmp_path / "ingest.sqlite")), Providers.fakes()
    pipeline = Pipeline(store, prov)
    inline = InlineRunner(pipeline)
    return Env(store, prov, pipeline, inline, IngestService(store, prov, inline))


@pytest.fixture
def queued(env: Env) -> Env:
    """Same env, but submit only enqueues; tests drive `pipeline.process` by hand."""
    env.service.runner = RecordingRunner()
    return env
