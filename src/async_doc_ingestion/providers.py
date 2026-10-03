"""Provider seams and deterministic fakes. Nothing here touches a network.

Fakes carry call counters: `FakeOCR.starts` and `FakeEmbedder.calls` are the "money" counters the
tests use to prove we never pay twice.
"""
from __future__ import annotations

import hashlib
import math
import re
import threading
import time
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class OCRResult:
    text: str
    pages: int


@dataclass
class Point:
    id: str
    vector: list[float]
    payload: dict[str, Any]


class Storage(Protocol):
    def download(self, url: str) -> bytes: ...


class OCRProvider(Protocol):
    def start(self, content: bytes, mime: str) -> str: ...
    def poll(self, job_id: str) -> OCRResult | None: ...


class Transcriber(Protocol):
    def transcribe(self, content: bytes) -> str: ...


class Tagger(Protocol):
    def tag(self, text: str, entity_context: dict) -> list[str]: ...


class EntityContext(Protocol):
    def fetch(self, surface: str, surface_id: str, tenant: str) -> dict: ...


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class EmbeddingsBase(ABC):
    """Stands in for a third-party embeddings base class (the kind a vector-store library checks
    with `isinstance`). Any wrapper around an embedder must subclass it; see metering.py."""

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class VectorStore(Protocol):
    def upsert(self, collection: str, points: list[Point]) -> None: ...
    def delete(self, collection: str, attachment_id: str) -> int: ...
    def search(self, collection: str, vector: list[float], k: int) -> list[dict]: ...


class WorkerCrash(Exception):
    """Raised by fakes to simulate a worker dying mid-stage."""


class FakeStorage:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.downloads = 0

    def put(self, url: str, data: bytes) -> None:
        self.files[url] = data

    def download(self, url: str) -> bytes:
        self.downloads += 1
        return self.files[url]


class FakeOCR:
    """The 'PDF' bytes are just UTF-8 text; `starts` is the per-page-billed call."""

    def __init__(self) -> None:
        self.starts = 0
        self.crash_on_next_poll = False
        self._jobs: dict[str, OCRResult] = {}

    def start(self, content: bytes, mime: str) -> str:
        self.starts += 1
        job_id = f"ocr-{self.starts}"
        text = content.decode("utf-8", errors="ignore")
        self._jobs[job_id] = OCRResult(text=text, pages=text.count("\f") + 1)
        return job_id

    def poll(self, job_id: str) -> OCRResult | None:
        if self.crash_on_next_poll:
            self.crash_on_next_poll = False
            raise WorkerCrash("worker died after OCR job was started")
        return self._jobs[job_id]


class FakeTranscriber:
    def __init__(self) -> None:
        self.calls = 0

    def transcribe(self, content: bytes) -> str:
        self.calls += 1
        return content.decode("utf-8", errors="ignore")


class FakeTagger:
    """Stands in for an LLM: surface name plus the longest words, deterministically."""

    def tag(self, text: str, entity_context: dict) -> list[str]:
        words = Counter(w for w in re.findall(r"[a-z]{5,}", text.lower()))
        top = [w for w, _ in sorted(words.items(), key=lambda kv: (-kv[1], kv[0]))[:3]]
        return [f"surface:{entity_context.get('surface', 'unknown')}", *top]


# Each surface exposes different business fields, as the backend does for each kind of entity.
SURFACE_FIELDS: dict[str, dict[str, object]] = {
    "task": {"status": "open", "assignee": "sam", "due_date": "2026-01-31"},
    "project": {"owner": "priya", "phase": "build", "budget_code": "PRJ-204"},
    "meeting": {"organizer": "lee", "attendees": ["sam", "priya"], "agenda": "Q1 planning"},
    "communication_hub": {"channel": "announcements", "audience": "all-staff", "pinned": True},
    "compliance": {"policy": "data retention", "control_id": "C-17", "review_cycle": "annual"},
    "space": {"space_name": "Design", "members": 12, "visibility": "private"},
    "email": {"sender": "ana@example.com", "recipients": ["sam@example.com"], "subject": "Invoice"},
}


class FakeEntityContext:
    """Returns a different set of fields for each of the seven surfaces."""

    def fetch(self, surface: str, surface_id: str, tenant: str) -> dict:
        return {"surface": surface, "title": f"{surface.title()} {surface_id}", "tenant": tenant,
                **SURFACE_FIELDS[surface]}


class FakeEmbedder(EmbeddingsBase):
    """Hashed bag-of-words vectors: similar texts land close, so search results make sense."""

    DIM = 32

    def __init__(self, delay: float = 0.0) -> None:
        self.calls = 0
        self.delay = delay  # widens race windows in concurrency tests
        self._lock = threading.Lock()

    def embed(self, texts: list[str]) -> list[list[float]]:
        with self._lock:
            self.calls += 1
        if self.delay:
            time.sleep(self.delay)
        return [self._vec(t) for t in texts]

    def _vec(self, text: str) -> list[float]:
        v = [0.0] * self.DIM
        for w in re.findall(r"\w+", text.lower()):
            v[int(hashlib.md5(w.encode()).hexdigest(), 16) % self.DIM] += 1.0
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]


class InMemoryVectorStore:
    """One collection per tenant (`tenant_{id}`): isolation by construction, not by filter."""

    def __init__(self) -> None:
        self.collections: dict[str, dict[str, Point]] = {}
        self._lock = threading.Lock()

    def upsert(self, collection: str, points: list[Point]) -> None:
        with self._lock:
            self.collections.setdefault(collection, {}).update({p.id: p for p in points})

    def delete(self, collection: str, attachment_id: str) -> int:
        with self._lock:
            col = self.collections.get(collection, {})
            gone = [i for i, p in col.items() if p.payload["attachment_id"] == attachment_id]
            for i in gone:
                del col[i]
            return len(gone)

    def search(self, collection: str, vector: list[float], k: int) -> list[dict]:
        with self._lock:
            points = list(self.collections.get(collection, {}).values())
        scored = [(sum(a * b for a, b in zip(vector, p.vector)), p) for p in points]
        scored.sort(key=lambda sp: -sp[0])
        return [{"score": round(s, 4), **p.payload} for s, p in scored[:k]]

    def count(self, collection: str) -> int:
        return len(self.collections.get(collection, {}))


def collection_for(tenant_id: str) -> str:
    return f"tenant_{tenant_id}"


@dataclass
class Providers:
    storage: Storage
    ocr: OCRProvider
    transcriber: Transcriber
    tagger: Tagger
    entities: EntityContext
    embedder: Embedder
    vectors: VectorStore

    @classmethod
    def fakes(cls) -> Providers:
        return cls(FakeStorage(), FakeOCR(), FakeTranscriber(), FakeTagger(), FakeEntityContext(),
                   FakeEmbedder(), InMemoryVectorStore())


