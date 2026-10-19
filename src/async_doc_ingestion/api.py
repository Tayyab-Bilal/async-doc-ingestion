"""HTTP surface. Submit returns 202 at once; clients poll GET for per-stage progress."""
from __future__ import annotations

from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .models import SubmitRequest
from .service import IngestService


def create_app(service: IngestService) -> FastAPI:
    app = FastAPI(title="async-doc-ingestion")

    @app.post("/ingest", status_code=202)
    def ingest(req: SubmitRequest):
        job_id, unchanged = service.submit(req)
        if unchanged:
            return JSONResponse({"job_id": job_id, "unchanged": True}, status_code=200)
        return {"job_id": job_id}

    @app.get("/ingest/{job_id}")
    def status(job_id: UUID):  # UUID type: garbage ids are a 422, not a 500
        job = service.status(str(job_id))
        if job is None:
            raise HTTPException(404, "unknown job")
        return job.model_dump(mode="json")

    @app.delete("/attachments/{attachment_id}")
    def delete(attachment_id: UUID):
        result = service.delete(str(attachment_id))
        if result is None:
            raise HTTPException(404, "unknown attachment")
        return result

    return app
