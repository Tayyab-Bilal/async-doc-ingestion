"""Re-ingest detection: one stable hash over everything that should trigger reprocessing."""
from __future__ import annotations

import hashlib
import json

from .models import SubmitRequest


def content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def payload_hash(req: SubmitRequest, content: bytes) -> str:
    """SHA-256 over canonical JSON of (metadata, labels, content hash).

    Labels are sorted so ordering noise from clients is not a change. The URL is deliberately not
    in the hash; the service compares it separately because a moved file wipes derived state.
    """
    canonical = json.dumps(
        {"metadata": req.metadata, "labels": sorted(req.labels), "content": content_hash(content)},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
