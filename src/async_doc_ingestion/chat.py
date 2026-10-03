"""Chat attachments (phase 1): files dropped into a chat message, not submitted through the API.

Audio is transcribed and handed back inline. Every other file is extracted once into a table, so
later questions read the database and never download the file again. An empty file is stored as
empty and reported as "no readable content".
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from .models import NO_CONTENT
from .pipeline import CHUNK_CHARS, OCR_MIMES, RTF_MIMES
from .providers import Point, Providers, collection_for
from .rtf import rtf_to_text
from .store import Store


@dataclass
class ChatResult:
    status: str  # inline | stored | already_stored | no_content | unsupported
    inline_text: str | None = None  # set for audio only
    note: str | None = None


class ChatAttachmentProcessor:
    def __init__(self, store: Store, providers: Providers) -> None:
        self.store, self.p = store, providers

    def process(self, tenant_id: str, attachment_id: str, url: str, filename: str,
                mime: str) -> ChatResult:
        if mime.startswith("audio/"):  # inline: the transcript goes into the message itself
            return ChatResult("inline", self.p.transcriber.transcribe(self.p.storage.download(url)))
        if self.store.get_chat(tenant_id, attachment_id):  # extract once, whatever the retries
            return ChatResult("already_stored")
        content = self.p.storage.download(url)
        text = self._extract(content, mime)
        if text is None:
            return ChatResult("unsupported", note=f"unsupported mime type {mime}")
        self.store.put_chat(tenant_id, attachment_id, filename, mime, text)
        if not text.strip():
            return ChatResult("no_content", note=NO_CONTENT)  # kept, but never indexed
        self._index(tenant_id, attachment_id, text)
        return ChatResult("stored")

    def _extract(self, content: bytes, mime: str) -> str | None:
        if mime in OCR_MIMES or mime.startswith("image/"):
            job_id = self.p.ocr.start(content, mime)
            result = self.p.ocr.poll(job_id)
            if result is None:
                raise RuntimeError("OCR not finished")  # Simplification: no re-attach for chat
            return result.text
        if mime in RTF_MIMES:
            return rtf_to_text(content.decode("latin-1"))
        if mime.startswith("text/") or "spreadsheet" in mime or "ms-excel" in mime:
            # Simplification: spreadsheets are read as text here; production uses a sheet reader.
            return content.decode("utf-8", errors="replace")
        return None

    def _index(self, tenant_id: str, attachment_id: str, text: str) -> None:
        chunks = [text[i:i + CHUNK_CHARS] for i in range(0, len(text), CHUNK_CHARS)]
        vectors = self.p.embedder.embed(chunks)
        self.p.vectors.upsert(collection_for(tenant_id), [
            Point(f"{attachment_id}:{i}", vec, {"attachment_id": attachment_id, "chunk": i,
                                                "text": chunk, "tags": [], "source": "chat"})
            for i, (chunk, vec) in enumerate(zip(chunks, vectors))
        ])


class UrlFetcher(Protocol):
    def get(self, url: str) -> str: ...


class FakeFetcher:
    """Serves canned pages; counts calls. No network."""

    def __init__(self, pages: dict[str, str]) -> None:
        self.pages, self.calls = pages, 0

    def get(self, url: str) -> str:
        self.calls += 1
        return self.pages[url]


def fetch_webpage(fetcher: UrlFetcher, url: str) -> str:
    """The chat 'read this link' tool: fetch, drop scripts and tags, collapse whitespace."""
    if not url.startswith(("http://", "https://")):
        raise ValueError("only http(s) urls can be fetched")
    html = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", fetcher.get(url))
    return re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", html)).strip()
