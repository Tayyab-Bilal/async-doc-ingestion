from __future__ import annotations

from async_doc_ingestion.linking import THRESHOLD, jaccard


def test_links_bidirectional_threshold(env):
    _, _, a = env.ingest("alpha beta gamma delta epsilon")
    _, _, b = env.ingest("alpha beta gamma zeta eta")  # jaccard 3/7 = 0.43 >= 0.4
    _, _, c = env.ingest("alpha beta omicron sigma tau")  # vs a: 2/8 = 0.25 < 0.4
    _, _, other_tenant = env.ingest("alpha beta gamma delta epsilon", tenant_id="globex")
    a, b, c, o = (str(r.attachment_id) for r in (a, b, c, other_tenant))

    assert THRESHOLD == 0.4 and round(jaccard({"x"}, {"x", "y"}), 2) == 0.5
    assert b in env.store.get_links(a) and a in env.store.get_links(b)  # both directions
    assert env.store.get_links(a)[b] == env.store.get_links(b)[a]
    assert c not in env.store.get_links(a) and a not in env.store.get_links(c)
    assert o not in env.store.get_links(a) and env.store.get_links(o) == {}  # tenant boundary


def test_vectors_isolated_per_tenant(env):
    env.ingest("invoice payment budget", tenant_id="acme")
    env.ingest("contract clause liability", tenant_id="globex")
    q = env.prov.embedder.embed(["invoice payment budget"])[0]

    acme = env.prov.vectors.search("tenant_acme", q, k=10)
    globex = env.prov.vectors.search("tenant_globex", q, k=10)

    assert [h["text"] for h in acme] == ["invoice payment budget"]
    assert [h["text"] for h in globex] == ["contract clause liability"]  # never acme's doc
    assert env.prov.vectors.search("tenant_nobody", q, k=10) == []


def test_delete_removes_rows_and_vectors_with_counts(env):
    _, _, a = env.ingest("alpha beta gamma delta epsilon")
    _, _, b = env.ingest("alpha beta gamma zeta eta")
    att_a, att_b = str(a.attachment_id), str(b.attachment_id)
    q = env.prov.embedder.embed(["alpha"])[0]

    result = env.service.delete(att_a)

    assert result["vectors"] == 1
    rows = result["rows"]
    assert rows["jobs"] == 1 and rows["extracted"] == 1 and rows["blobs"] == 1
    assert rows["tags"] > 0 and rows["links"] == 2 and rows["job_history"] == 1
    assert env.store.get_job_by_attachment(att_a) is None
    assert env.store.get_links(att_b) == {}  # the partner's back-link went too
    assert [h["attachment_id"] for h in env.prov.vectors.search("tenant_acme", q, 10)] == [att_b]
    assert env.service.delete(att_a) is None  # second delete: nothing left
