"""Submit/status/delete logic behind the API, kept free of HTTP so it is easy to test."""
from __future__ import annotations

from .hashing import content_hash, payload_hash
from .models import Job, SubmitRequest
from .providers import Providers, collection_for
from .store import Store
from .worker import Runner


class IngestService:
    def __init__(self, store: Store, providers: Providers, runner: Runner) -> None:
        self.store, self.p, self.runner = store, providers, runner

    def submit(self, req: SubmitRequest) -> tuple[str, bool]:
        """Returns (job_id, unchanged). Unchanged re-uploads cost nothing: no stage re-runs."""
        # Simplification: downloads to hash; production compares the object's ETag/version instead.
        content = self.p.storage.download(req.source_url)
        h, ch = payload_hash(req, content), content_hash(content)
        att = str(req.attachment_id)
        job = self.store.get_job_by_attachment(att)
        if job is None:
            job = self.store.create_job(req, h, ch)
        else:
            old = self.store.get_request(att)
            if job.payload_hash == h and old.source_url == req.source_url and job.status != "failed":
                return job.job_id, True
            # A moved file or changed bytes invalidates derived state (and the OCR job id).
            wipe = old.source_url != req.source_url or self.store.get_content_hash(att) != ch
            if wipe:
                self.p.vectors.delete(collection_for(req.tenant_id), att)
            self.store.reset_job(job.job_id, req, h, ch, wipe)
        self.runner.dispatch("process", job.job_id)
        return job.job_id, False

    def status(self, job_id: str) -> Job | None:
        return self.store.get_job(job_id)

    def delete(self, attachment_id: str) -> dict | None:
        job = self.store.get_job_by_attachment(attachment_id)
        if job is None:
            return None
        vectors = self.p.vectors.delete(collection_for(job.tenant_id), attachment_id)
        return {"rows": self.store.delete_attachment(attachment_id), "vectors": vectors}
