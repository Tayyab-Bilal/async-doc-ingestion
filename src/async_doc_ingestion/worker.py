"""Dispatch seam. Pipeline code never knows whether it runs inline, in Celery, or elsewhere."""
from __future__ import annotations

from typing import Literal, Protocol

from .pipeline import Pipeline

Task = Literal["process", "index"]


class Runner(Protocol):
    def dispatch(self, task: Task, job_id: str) -> None: ...


class InlineRunner:
    """Runs everything synchronously; used by tests and the demo."""

    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline

    def dispatch(self, task: Task, job_id: str) -> None:
        if task == "process":
            self.pipeline.process(job_id)
            self.dispatch("index", job_id)
        else:
            self.pipeline.index(job_id)
