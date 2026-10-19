"""Attachment query (phase 3): answer a question over one attachment, or over the whole tenant.

Order of work, cheapest first:
  1. Resolve which attachment (explicit argument, then the per-call envelope, then a shared
     default) so parallel queries never read each other's file.
  2. Fast path, before any retrieval or model loop: empty file -> hard stop; spreadsheet ->
     spreadsheet and document specialists in parallel on the stored text.
  3. Otherwise hybrid retrieval over the tenant's vectors (dense score + keyword overlap),
     then the document specialist answers from the best chunks.

Simplification: there is no agent loop here, one specialist call follows retrieval. The original
used a ReAct specialist; the fast path and the retrieval step are the parts modelled.
"""
from __future__ import annotations

from .context import current_envelope, resolve
from .models import NO_CONTENT
from .pipeline import normalize
from .providers import Providers, collection_for
from .specialists import Answer, DocumentRouter, is_spreadsheet
from .store import Store


class AttachmentQuery:
    def __init__(self, store: Store, providers: Providers, router: DocumentRouter,
                 top_k: int = 3) -> None:
        self.store, self.p, self.router, self.top_k = store, providers, router, top_k

    async def ask(self, question: str, tenant: str | None = None, *,
                  attachment_id: str | None = None, shared_default: str | None = None) -> Answer:
        env = current_envelope()
        att = resolve(attachment_id, env.attachment_id if env else None, shared_default)
        tenant = resolve(tenant, env.tenant if env else None, None)
        if tenant is None:
            raise ValueError("no tenant: pass one or run inside a call envelope")
        if att is None:
            return await self._retrieve(question, tenant, None, "", "")

        chat = self.store.get_chat(tenant, att)
        if chat:  # read from the table: no download, no retrieval
            return await self.router.ask(question, chat["mime_type"], chat["filename"],
                                         chat["text"])
        job = self.store.get_job_by_attachment(att)
        if job is None or job.tenant_id != tenant:
            return Answer("not_found", "attachment not found")
        doc = self.store.get_extracted(att)
        if job.note == NO_CONTENT or (doc and not doc["text"].strip()):
            return Answer("no_content", NO_CONTENT)  # hard stop, never another document
        if doc is None:
            return Answer("not_ready", "attachment is still being processed")
        req = self.store.get_request(att)
        if is_spreadsheet(req.mime_type, req.filename):
            return await self.router.ask(question, req.mime_type, req.filename, doc["text"])
        return await self._retrieve(question, tenant, att, req.mime_type, req.filename,
                                    fallback=doc["text"])

    async def _retrieve(self, question: str, tenant: str, att: str | None, mime: str,
                        filename: str, fallback: str = "") -> Answer:
        hits = self.hybrid_search(question, tenant, att)
        if not hits and not fallback:
            return Answer("no_match", "no matching content")
        text = "\n".join(h["text"] for h in hits) or fallback
        answer = await self.router.ask(question, mime or "text/plain", filename, text)
        answer.via = "hybrid"
        answer.sources = sorted({h["attachment_id"] for h in hits} | ({att} if att else set()))
        return answer

    def hybrid_search(self, question: str, tenant: str, att: str | None = None) -> list[dict]:
        """Workspace chunks only (chat chunks share the collection but are queried by id).
        Score = 0.5 * dense similarity + 0.5 * share of question keywords found in the chunk."""
        vec = self.p.embedder.embed([question])[0]
        words = set(normalize(question))
        scored = []
        for hit in self.p.vectors.search(collection_for(tenant), vec, k=200):
            if hit.get("source") == "chat" or (att and hit["attachment_id"] != att):
                continue
            keyword = len(words & set(normalize(hit["text"]))) / len(words) if words else 0.0
            scored.append((0.5 * hit["score"] + 0.5 * keyword, hit))
        scored.sort(key=lambda s: -s[0])
        return [h for score, h in scored[: self.top_k] if score > 0]
