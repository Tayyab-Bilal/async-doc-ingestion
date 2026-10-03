"""Real Celery wiring, shown for reference. Not imported by tests; Celery is an optional extra.

Beat runs the sweep every 10 minutes and the 90-day cleanup daily.
The three settings below are the whole crash-safety story at the broker level:
  - acks_late: the message is only acknowledged after the task finishes,
  - reject_on_worker_lost: a killed worker puts it back on the queue,
  - max_retries=0: no blind automatic retries; redelivery re-attaches via the persisted OCR job id.
"""
from __future__ import annotations

import os

from celery import Celery

from .pipeline import Pipeline
from .retention import cleanup_chat_attachments
from .sweep import sweep as run_sweep

app = Celery("async_doc_ingestion", broker=os.environ.get("BROKER_URL", "redis://localhost:6379/0"))
app.conf.update(task_acks_late=True, task_reject_on_worker_lost=True)
app.conf.beat_schedule = {
    "sweep": {"task": "ingest.sweep", "schedule": 600.0},  # every 10 minutes
    "retention": {"task": "ingest.cleanup", "schedule": 86400.0},  # daily
}

_pipeline: Pipeline | None = None


def configure(pipeline: Pipeline) -> None:
    """Call once at worker start-up with a pipeline wired to real providers."""
    global _pipeline
    _pipeline = pipeline


@app.task(name="ingest.process", max_retries=0)
def process(job_id: str) -> None:
    assert _pipeline is not None, "call configure() first"
    _pipeline.process(job_id)
    index.delay(job_id)


@app.task(name="ingest.index", max_retries=0)
def index(job_id: str) -> None:
    assert _pipeline is not None, "call configure() first"
    _pipeline.index(job_id)


@app.task(name="ingest.sweep", max_retries=0)
def sweep() -> list[str]:
    """Beat entry: re-dispatch jobs that finished but were never indexed."""
    assert _pipeline is not None, "call configure() first"
    return run_sweep(_pipeline.store, CeleryRunner())


@app.task(name="ingest.cleanup", max_retries=0)
def cleanup() -> dict[str, int]:
    assert _pipeline is not None, "call configure() first"
    return cleanup_chat_attachments(_pipeline.store, _pipeline.p.vectors)


class CeleryRunner:
    def dispatch(self, task: str, job_id: str) -> None:
        {"process": process, "index": index}[task].delay(job_id)
