"""Self-healing: finish jobs that completed but never got indexed (e.g. worker died after the
last stage, or the vector store was down)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .store import Store
from .worker import Runner

MIN_AGE = timedelta(minutes=15)  # younger jobs may simply still be in flight
MAX_ATTEMPTS = 5  # then a human looks; we stop paying for embeddings
BATCH = 50


def sweep(store: Store, runner: Runner, now: datetime | None = None, *,
          min_age: timedelta = MIN_AGE, max_attempts: int = MAX_ATTEMPTS,
          batch: int = BATCH) -> list[str]:
    """Claim atomically, then dispatch `index`. Returns the job ids this sweep owned."""
    now = now or datetime.now(UTC)
    claimed = store.claim_unindexed(now, min_age, max_attempts, batch)
    try:
        for job_id in claimed:
            runner.dispatch("index", job_id)
    finally:
        store.release_claims(claimed)  # failures stay retryable, bounded by the attempt cap
    return claimed
