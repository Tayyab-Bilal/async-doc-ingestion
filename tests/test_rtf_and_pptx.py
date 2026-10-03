from __future__ import annotations

from async_doc_ingestion.rtf import rtf_to_text

RTF = (r"{\rtf1\ansi{\fonttbl{\f0 Arial;}}{\*\generator Fake 1.0;}\f0 Hello \b world\b0\par "
       r"Second line with caf\'e9 and \{braces\}\par}")


def test_rtf_to_text_strips_markup_and_keeps_text():
    assert rtf_to_text(RTF) == "Hello world\nSecond line with café and {braces}"


def test_pipeline_extracts_rtf(env):
    job_id, _, req = env.ingest(RTF, mime_type="application/rtf")
    assert env.store.get_job(job_id).status == "completed"
    assert env.store.get_extracted(str(req.attachment_id))["text"].startswith("Hello world")


def test_pptx_goes_through_ocr(env):
    mime = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    job_id, _, _ = env.ingest("slide one text", mime_type=mime)
    assert env.store.get_job(job_id).status == "completed" and env.prov.ocr.starts == 1
