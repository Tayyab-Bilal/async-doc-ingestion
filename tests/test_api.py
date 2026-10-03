from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from async_doc_ingestion.api import create_app
from async_doc_ingestion.models import STAGES


@pytest.fixture
def post(env):
    client = TestClient(create_app(env.service))

    def _post(req, data=b"quarterly invoice payment budget"):
        env.prov.storage.put(req.source_url, data)
        return client.post("/ingest", json=req.model_dump(mode="json"))

    _post.client = client
    return _post


def test_submit_returns_202_and_job_id(env, post):
    r = post(env.req(mime_type="text/plain"))
    assert r.status_code == 202
    assert r.json()["job_id"]


def test_status_shows_stage_progress(queued, post):
    r = post(queued.req(mime_type="text/plain"))
    job_id = r.json()["job_id"]
    before = post.client.get(f"/ingest/{job_id}").json()
    assert before["status"] == "queued"
    assert set(before["progress"]) == set(STAGES)
    assert set(before["progress"].values()) == {"pending"}

    queued.pipeline.process(job_id)
    mid = post.client.get(f"/ingest/{job_id}").json()
    assert mid["status"] == "completed" and mid["indexed"] is False
    assert mid["progress"]["link"] == "done" and mid["progress"]["index"] == "pending"

    queued.pipeline.index(job_id)
    after = post.client.get(f"/ingest/{job_id}").json()
    assert set(after["progress"].values()) == {"done"} and after["indexed"] is True


def test_invalid_uuid_is_422(env, post):
    assert post.client.get("/ingest/not-a-uuid").status_code == 422
    assert post.client.delete("/attachments/not-a-uuid").status_code == 422


def test_unchanged_resubmit_over_http_is_200(env, post):
    req = env.req(mime_type="text/plain")
    assert post(req).status_code == 202
    again = post(req)
    assert again.status_code == 200 and again.json()["unchanged"] is True


def test_delete_unknown_attachment_is_404(env, post):
    assert post.client.delete(f"/attachments/{env.req().attachment_id}").status_code == 404
