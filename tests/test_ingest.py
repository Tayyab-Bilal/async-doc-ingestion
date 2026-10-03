from __future__ import annotations

import pytest

from async_doc_ingestion.models import NO_CONTENT
from async_doc_ingestion.providers import WorkerCrash

PDF = b"Quarterly invoice and payment report for the budget review"


def test_unchanged_resubmit_is_noop(env):
    job_id, _, req = env.ingest(PDF, mime_type="application/pdf")
    assert env.prov.ocr.starts == 1
    downloads_before = env.prov.storage.downloads

    job_again, unchanged = env.service.submit(req)

    assert (job_again, unchanged) == (job_id, True)
    assert env.prov.ocr.starts == 1  # the money counter: no second OCR job
    assert env.prov.embedder.calls == 1  # and no second embedding
    assert env.prov.storage.downloads == downloads_before + 1  # only the hashing download


def test_changed_labels_reingests(env):
    job_id, _, req = env.ingest(PDF, mime_type="application/pdf", labels=["finance"])
    again, unchanged = env.service.submit(req.model_copy(update={"labels": ["finance", "q3"]}))
    assert again == job_id and unchanged is False
    assert env.store.get_tags(str(req.attachment_id), "label") == {
        "finance", "q3"}
    assert env.prov.ocr.starts == 1  # same bytes: re-attached to the existing OCR job
    assert env.store.get_job(job_id).indexed is True


def test_label_order_is_not_a_change(env):
    _, _, req = env.ingest(PDF, labels=["a", "b"])
    _, unchanged = env.service.submit(req.model_copy(update={"labels": ["b", "a"]}))
    assert unchanged is True


def test_new_storage_url_wipes_derived_keeps_history(queued):
    job_id, _, req = queued.ingest(PDF, mime_type="application/pdf")
    queued.pipeline.process(job_id)
    queued.pipeline.index(job_id)
    att = str(req.attachment_id)
    assert queued.store.get_extracted(att) and queued.store.get_tags(att, "keyword")
    assert queued.prov.vectors.count("tenant_acme") == 1

    queued.prov.storage.put("s3://docs/moved.pdf", b"Completely different contract text clause")
    new_req = req.model_copy(update={"source_url": "s3://docs/moved.pdf"})
    again, unchanged = queued.service.submit(new_req)

    assert again == job_id and unchanged is False
    assert queued.store.get_extracted(att) is None  # derived state gone
    assert queued.store.get_tags(att, "keyword") == set()
    assert queued.prov.vectors.count("tenant_acme") == 0
    job = queued.store.get_job(job_id)
    assert job.ocr_job_id is None and job.indexed is False
    history = queued.store.history(att)
    assert [h["event"] for h in history] == ["created", "reingest_wipe"]  # history survived
    assert history[0]["source_url"] != history[1]["source_url"]


def test_crash_after_ocr_start_resumes_same_ocr_job(queued):
    job_id, _, req = queued.ingest(PDF, mime_type="application/pdf")
    queued.prov.ocr.crash_on_next_poll = True

    with pytest.raises(WorkerCrash):
        queued.pipeline.process(job_id)  # worker dies right after OCR start was persisted
    crashed = queued.store.get_job(job_id)
    assert queued.prov.ocr.starts == 1
    assert crashed.status == "running" and crashed.ocr_job_id == "ocr-1"

    queued.pipeline.process(job_id)  # message re-queued, new worker picks it up

    assert queued.prov.ocr.starts == 1  # re-attached, not paid again
    done = queued.store.get_job(job_id)
    assert done.status == "completed" and done.ocr_job_id == "ocr-1"
    assert queued.store.get_extracted(str(req.attachment_id))["text"].startswith("Quarterly")


def test_resume_skips_finished_stages(queued):
    job_id, _, _ = queued.ingest(PDF, mime_type="application/pdf")
    queued.prov.ocr.crash_on_next_poll = True
    with pytest.raises(WorkerCrash):
        queued.pipeline.process(job_id)
    downloads = queued.prov.storage.downloads
    queued.pipeline.process(job_id)
    assert queued.prov.storage.downloads == downloads  # download stage was already done


def test_empty_document_never_indexes(env):
    job_id, _, _ = env.ingest("   \n  ")
    job = env.store.get_job(job_id)
    assert job.status == "completed" and job.note == NO_CONTENT
    assert job.indexed is False
    assert job.progress["extract"] == "done" and job.progress["tag"] == "skipped"
    assert env.pipeline.index(job_id) is False
    assert env.prov.embedder.calls == 0
    assert env.prov.vectors.count("tenant_acme") == 0


def test_audio_goes_to_transcriber(env):
    job_id, _, _ = env.ingest("standup notes about the sprint deadline", mime_type="audio/mpeg")
    assert env.prov.transcriber.calls == 1 and env.prov.ocr.starts == 0
    assert env.store.get_job(job_id).status == "completed"


def test_text_read_directly(env):
    _, _, req = env.ingest("plain note about the roadmap", mime_type="text/plain")
    assert env.prov.transcriber.calls == 0 and env.prov.ocr.starts == 0
    assert env.store.get_extracted(str(req.attachment_id))["text"] == "plain note about the roadmap"


def test_unsupported_mime_fails_without_paying(env):
    job_id, _, _ = env.ingest("x", mime_type="application/x-weird")
    job = env.store.get_job(job_id)
    assert job.status == "failed" and "unsupported" in job.note
    assert env.prov.embedder.calls == 0


def test_stages_enrich_with_context_and_domain(env):
    _, _, req = env.ingest("Invoice payment due; budgets approved.", surface="meeting",
                           surface_id="M-9", metadata={"owner": "sam"})
    att = str(req.attachment_id)
    assert "surface:meeting" in env.store.get_tags(att, "tag")  # tagger saw entity context
    assert env.store.get_tags(att, "domain") == {"finance"}
    assert {"invoice", "payment", "budget"} <= env.store.get_tags(att, "keyword")  # normalised
    assert env.store.get_meta(att, "entity")["title"] == "Meeting M-9"
