from __future__ import annotations

from datetime import UTC, datetime, timedelta

from async_doc_ingestion.chat import ChatAttachmentProcessor
from async_doc_ingestion.retention import cleanup_chat_attachments


def test_cleanup_removes_only_chat_extractions_older_than_90_days_and_their_vectors(env):
    chat = ChatAttachmentProcessor(env.store, env.prov)
    for att, text in (("old", "old chat file"), ("new", "new chat file")):
        env.prov.storage.put(f"s3://c/{att}", text.encode())
        chat.process("acme", att, f"s3://c/{att}", f"{att}.txt", "text/plain")
    env.store.put_chat("acme", "old", "old.txt", "text/plain", "old chat file",
                       created_at=datetime.now(UTC) - timedelta(days=91))
    env.ingest("workspace doc survives cleanup")  # a workspace attachment, same tenant
    before = env.prov.vectors.count("tenant_acme")

    counts = cleanup_chat_attachments(env.store, env.prov.vectors)

    assert counts == {"extractions": 1, "vectors": 1}
    assert env.store.get_chat("acme", "old") is None and env.store.get_chat("acme", "new")
    assert env.prov.vectors.count("tenant_acme") == before - 1
    assert cleanup_chat_attachments(env.store, env.prov.vectors) == {"extractions": 0, "vectors": 0}


def test_cutoff_is_inclusive_of_the_boundary_day(env):
    env.store.put_chat("acme", "edge", "e.txt", "text/plain", "x",
                       created_at=datetime.now(UTC) - timedelta(days=89))
    assert cleanup_chat_attachments(env.store, env.prov.vectors)["extractions"] == 0
