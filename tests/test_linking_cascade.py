from __future__ import annotations

from async_doc_ingestion.linking import cascade_score


def test_keyword_signal_wins_first():
    assert cascade_score({"a", "b"}, {"a", "b"}, {"x"}, {"x"}, {"k": 1}, {"k": 1}) == ("keyword", 1.0)


def test_label_signal_used_when_keywords_are_below_threshold():
    sig, score = cascade_score({"a"}, {"b"}, {"x", "y"}, {"x", "y"}, {}, {})
    assert (sig, score) == ("label", 1.0)


def test_metadata_signal_is_last_and_needs_exact_match():
    assert cascade_score({"a"}, {"b"}, {"x"}, {"y"}, {"k": 1}, {"k": 1}) == ("metadata", 1.0)
    assert cascade_score({"a"}, {"b"}, {"x"}, {"y"}, {"k": 1}, {"k": 2}) is None


def test_signal_does_not_apply_when_one_side_has_no_data():
    assert cascade_score({"a"}, {"b"}, set(), {"x"}, {}, {"k": 1}) is None
    assert cascade_score({"a"}, {"b"}, set(), set(), {}, {}) is None  # empty == empty is no signal


def test_score_below_threshold_creates_no_link():
    assert cascade_score({"a", "b", "c"}, {"a", "d", "e"}, set(), set(), None, None) is None  # 0.2


def test_label_link_created_end_to_end_and_bidirectional(env):
    _, _, a = env.ingest("alpha beta gamma delta", labels=["q1", "launch"])
    _, _, b = env.ingest("omicron sigma tau upsilon", labels=["q1", "launch"])
    a, b = str(a.attachment_id), str(b.attachment_id)
    assert env.store.get_links(a) == {b: 1.0} and env.store.get_links(b) == {a: 1.0}


def test_metadata_link_created_end_to_end(env):
    _, _, a = env.ingest("alpha beta gamma delta", metadata={"contract": "C-9"})
    _, _, b = env.ingest("omicron sigma tau upsilon", metadata={"contract": "C-9"})
    _, _, c = env.ingest("phi chi psi omega", metadata={"contract": "C-10"})
    a, b, c = (str(r.attachment_id) for r in (a, b, c))
    assert set(env.store.get_links(a)) == {b} and env.store.get_links(c) == {}
