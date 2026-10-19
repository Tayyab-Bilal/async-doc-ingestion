"""Type-contract tests: the ingestion outage, and the Celery settings nobody runs in tests."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from async_doc_ingestion.metering import MeteredEmbedder, StrictVectorStore
from async_doc_ingestion.providers import EmbeddingsBase, FakeEmbedder

CELERY_SRC = (Path(__file__).parents[1] / "src/async_doc_ingestion/celery_app.py").read_text()


class DelegatingWrapper:
    """The outage: same methods through __getattr__, but not an EmbeddingsBase."""

    def __init__(self, inner) -> None:
        self.inner, self.credits = inner, 0

    def __getattr__(self, name):
        return getattr(self.inner, name)


def test_metered_wrapper_subclasses_the_third_party_base_class():
    wrapper = MeteredEmbedder(FakeEmbedder())
    assert isinstance(wrapper, EmbeddingsBase)
    store = StrictVectorStore(wrapper)  # the library's isinstance check passes
    assert store.add_texts(["a", "b"]) == 2 and wrapper.credits == 2


def test_delegating_wrapper_without_the_base_class_is_rejected_by_the_library():
    with pytest.raises(TypeError, match="EmbeddingsBase"):
        StrictVectorStore(DelegatingWrapper(FakeEmbedder()))
    # It even looks right by duck typing, which is why the outage was easy to miss:
    assert DelegatingWrapper(FakeEmbedder()).embed(["x"])


def test_fake_embedder_satisfies_the_base_class_too():
    assert isinstance(FakeEmbedder(), EmbeddingsBase)


def test_celery_crash_safety_settings_and_beat_schedule_are_declared():
    # celery_app.py needs the optional extra, so it is read as text, never imported.
    assert "task_acks_late=True" in CELERY_SRC and "task_reject_on_worker_lost=True" in CELERY_SRC
    assert CELERY_SRC.count("max_retries=0") >= 3
    assert re.search(r'"task": "ingest\.sweep", "schedule": 600\.0', CELERY_SRC)  # 10 minutes
    assert re.search(r'"task": "ingest\.cleanup"', CELERY_SRC)
