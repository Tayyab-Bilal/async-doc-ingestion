"""Wire and persistence models. Progress is per stage so clients can poll and workers can resume."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

# The seven places a file can be attached. Chat is not one: chat attachments have their own
# processor (chat.py) and are never submitted through the async API.
Surface = Literal[
    "task", "project", "meeting", "communication_hub", "compliance", "space", "email"
]
Status = Literal["queued", "running", "completed", "failed"]

# `index` is a separate step on purpose: the sweep can re-run it without redoing OCR.
STAGES = ("download", "extract", "tag", "normalize", "classify", "metadata", "link", "index")
NO_CONTENT = "no readable content"


class SubmitRequest(BaseModel):
    tenant_id: str
    attachment_id: UUID
    source_url: str
    filename: str
    mime_type: str
    surface: Surface
    surface_id: str
    labels: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class Job(BaseModel):
    job_id: str
    attachment_id: str
    tenant_id: str
    status: Status
    stage: str | None = None
    progress: dict[str, str]  # stage -> pending | running | done | skipped
    payload_hash: str
    ocr_job_id: str | None = None
    attempts: int = 0
    indexed: bool = False
    note: str | None = None
    created_at: datetime
    updated_at: datetime
