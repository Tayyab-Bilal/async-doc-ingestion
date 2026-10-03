"""The seven stages plus `index`. Each stage is idempotent and resumable from the job row.

A worker may die anywhere. Because progress, the OCR job id and every derived artefact live in the
store, a re-queued message simply skips finished stages and continues. We never catch generic
exceptions: a crash leaves the job `running` so the redelivered message resumes it.
"""
from __future__ import annotations

import re

from .linking import compute_links
from .models import NO_CONTENT, STAGES, Job, SubmitRequest
from .providers import Point, Providers, collection_for
from .rtf import rtf_to_text
from .store import Store

# Two-level lookup: domain -> subdomain -> trigger words.
DOMAINS = {
    "finance": {"invoicing": {"invoice", "payment"}, "budgeting": {"budget", "revenue", "expense"}},
    "legal": {"contracts": {"contract", "clause", "agreement"},
              "risk": {"liability", "compliance"}},
    "engineering": {"infrastructure": {"server", "database"},
                    "delivery": {"deploy", "release", "bug"}},
    "planning": {"roadmap": {"roadmap", "milestone"},
                 "scheduling": {"sprint", "deadline", "agenda"}},
}
STOPWORDS = {"the", "and", "for", "with", "that", "this", "are", "was", "from", "have", "has"}
CHUNK_CHARS = 800
OCR_MIMES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",  # pptx
}
RTF_MIMES = {"application/rtf", "text/rtf"}


def normalize(text: str) -> list[str]:
    """Lowercase, strip punctuation, drop stopwords, crude suffix stripping (ing/ed/s)."""
    out = []
    for word in re.sub(r"[^\w\s]", " ", text.lower()).split():
        for suffix in ("ing", "ed", "s"):
            if word.endswith(suffix) and len(word) - len(suffix) >= 3:
                word = word[: -len(suffix)]
                break
        if len(word) > 2 and word not in STOPWORDS:
            out.append(word)
    return out


_SUBDOMAIN_KEYWORDS = {(d, s): set(normalize(" ".join(words)))
                       for d, subs in DOMAINS.items() for s, words in subs.items()}


def lookup_domain(keywords: set[str]) -> list[tuple[str, str]]:
    """(domain, subdomain) pairs whose trigger words appear in the document keywords."""
    return sorted(pair for pair, words in _SUBDOMAIN_KEYWORDS.items() if keywords & words)


class Halt(Exception):
    """A stage decided the job is finished early (empty document, unsupported type)."""

    def __init__(self, status: str, note: str) -> None:
        self.status, self.note = status, note


class Pipeline:
    def __init__(self, store: Store, providers: Providers) -> None:
        self.store, self.p = store, providers

    def process(self, job_id: str) -> None:
        job = self.store.get_job(job_id)
        if job is None or job.status in ("completed", "failed"):
            return
        req = self.store.get_request(job.attachment_id)
        self.store.update_job(job_id, status="running")
        try:
            for stage in STAGES[:-1]:
                if job.progress[stage] == "done":
                    continue
                self.store.set_stage(job_id, stage, "running")
                getattr(self, f"_{stage}")(job, req)
                self.store.set_stage(job_id, stage, "done")
        except Halt as h:
            self.store.finish(job_id, h.status, h.note)
            return
        self.store.update_job(job_id, status="completed")

    # ---- stages -----------------------------------------------------------------------------
    def _download(self, job: Job, req: SubmitRequest) -> None:
        self.store.put_blob(job.attachment_id, self.p.storage.download(req.source_url))

    def _extract(self, job: Job, req: SubmitRequest) -> None:
        content, mime = self.store.get_blob(job.attachment_id), req.mime_type
        if mime.startswith("audio/"):
            text, pages = self.p.transcriber.transcribe(content), 0
        elif mime in OCR_MIMES or mime.startswith("image/"):
            text, pages = self._ocr(job, content, mime)
        elif mime in RTF_MIMES:
            text, pages = rtf_to_text(content.decode("latin-1")), 1
        elif mime.startswith("text/"):
            text, pages = content.decode("utf-8", errors="replace"), 1
        else:
            raise Halt("failed", f"unsupported mime type {mime}")
        if not text.strip():
            self.store.set_stage(job.job_id, "extract", "done")
            raise Halt("completed", NO_CONTENT)  # never indexed, never swept
        self.store.put_extracted(job.attachment_id, text, pages)

    def _ocr(self, job: Job, content: bytes, mime: str) -> tuple[str, int]:
        # Re-read the row: a previous attempt may have persisted the OCR job id already.
        ocr_job_id = self.store.get_job(job.job_id).ocr_job_id  # type: ignore[union-attr]
        if ocr_job_id is None:
            ocr_job_id = self.p.ocr.start(content, mime)
            # Simplification: a crash between start() and this write still pays once more; production
            # narrows it by passing our job id as the provider's idempotency key.
            self.store.update_job(job.job_id, ocr_job_id=ocr_job_id)
        result = self.p.ocr.poll(ocr_job_id)  # persisted BEFORE polling, so a crash here is safe
        if result is None:
            raise RuntimeError(f"OCR job {ocr_job_id} not finished; message will be re-queued")
        return result.text, result.pages

    def _tag(self, job: Job, req: SubmitRequest) -> None:
        ctx = self.p.entities.fetch(req.surface, req.surface_id, req.tenant_id)
        self.store.put_meta(job.attachment_id, "entity", ctx)
        text = self.store.get_extracted(job.attachment_id)["text"]  # type: ignore[index]
        self.store.replace_tags(job.attachment_id, "tag", self.p.tagger.tag(text, ctx))

    def _normalize(self, job: Job, req: SubmitRequest) -> None:
        text = self.store.get_extracted(job.attachment_id)["text"]  # type: ignore[index]
        self.store.replace_tags(job.attachment_id, "keyword", sorted(set(normalize(text))))

    def _classify(self, job: Job, req: SubmitRequest) -> None:
        keywords = self.store.get_tags(job.attachment_id, "keyword")
        pairs = lookup_domain(keywords)
        self.store.replace_tags(job.attachment_id, "domain", sorted({d for d, _ in pairs}))
        self.store.replace_tags(job.attachment_id, "subdomain", [f"{d}/{s}" for d, s in pairs])

    def _metadata(self, job: Job, req: SubmitRequest) -> None:
        self.store.replace_tags(job.attachment_id, "label", req.labels)
        self.store.put_meta(job.attachment_id, "metadata", req.metadata)
        self.store.put_meta(job.attachment_id, "document", {
            "filename": req.filename, "mime_type": req.mime_type, "surface": req.surface,
            "surface_id": req.surface_id,
            "pages": self.store.get_extracted(job.attachment_id)["pages"],  # type: ignore[index]
        })

    def _link(self, job: Job, req: SubmitRequest) -> None:
        self.store.replace_links(job.attachment_id,
                                 compute_links(self.store, job.attachment_id, req.tenant_id))

    # ---- index (separate so the sweep can redo only this) -----------------------------------
    def index(self, job_id: str) -> bool:
        job = self.store.get_job(job_id)
        if job is None or job.status != "completed" or job.indexed or job.note == NO_CONTENT:
            return False
        att = job.attachment_id
        text = self.store.get_extracted(att)["text"]  # type: ignore[index]
        chunks = [text[i:i + CHUNK_CHARS] for i in range(0, len(text), CHUNK_CHARS)]
        vectors = self.p.embedder.embed(chunks)  # the paid call; reached at most once per claim
        collection = collection_for(job.tenant_id)
        self.p.vectors.delete(collection, att)  # replace, so shrunk documents leave no stale chunks
        self.p.vectors.upsert(collection, [
            Point(f"{att}:{i}", vec, {"attachment_id": att, "chunk": i, "text": chunk,
                                      "tags": sorted(self.store.get_tags(att, "tag"))})
            for i, (chunk, vec) in enumerate(zip(chunks, vectors))
        ])
        self.store.set_stage(job_id, "index", "done")
        self.store.update_job(job_id, indexed=True)
        return True
