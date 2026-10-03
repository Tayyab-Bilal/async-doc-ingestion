from __future__ import annotations

import pytest
from pydantic import ValidationError

from async_doc_ingestion.models import Surface
from async_doc_ingestion.providers import FakeEntityContext

SEVEN = ["task", "project", "meeting", "communication_hub", "compliance", "space", "email"]


def test_exactly_seven_source_surfaces():
    assert list(Surface.__args__) == SEVEN


def test_entity_context_is_distinct_per_surface():
    contexts = {s: FakeEntityContext().fetch(s, "X-1", "acme") for s in SEVEN}
    field_sets = {frozenset(c) for c in contexts.values()}
    assert len(field_sets) == 7  # every surface exposes its own fields
    assert all(c["surface"] == s for s, c in contexts.items())


@pytest.mark.parametrize("surface", SEVEN)
def test_each_surface_is_ingested_and_tagged_with_its_context(env, surface):
    job_id, _, req = env.ingest("invoice payment budget", surface=surface)
    att = str(req.attachment_id)
    assert env.store.get_job(job_id).status == "completed"
    assert env.store.get_meta(att, "entity")["surface"] == surface
    assert f"surface:{surface}" in env.store.get_tags(att, "tag")


def test_chat_is_not_a_source_surface(env):
    with pytest.raises(ValidationError):
        env.req(surface="chat")
