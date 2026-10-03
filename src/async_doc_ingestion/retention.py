"""Retention: chat attachments are throw-away context, so they expire after 90 days.

Workspace attachments are not touched; they live until their owner deletes them.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .providers import VectorStore, collection_for
from .store import Store

RETENTION_DAYS = 90


def cleanup_chat_attachments(store: Store, vectors: VectorStore, now: datetime | None = None,
                             days: int = RETENTION_DAYS) -> dict[str, int]:
    """Delete expired chat extractions and their vectors. Returns what was removed."""
    cutoff = (now or datetime.now(UTC)) - timedelta(days=days)
    counts = {"extractions": 0, "vectors": 0}
    for row in store.expired_chat(cutoff):
        counts["vectors"] += vectors.delete(collection_for(row["tenant_id"]), row["attachment_id"])
        counts["extractions"] += store.delete_chat(row["attachment_id"])
    return counts
