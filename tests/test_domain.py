from __future__ import annotations

from async_doc_ingestion.pipeline import lookup_domain, normalize


def test_lookup_returns_domain_and_subdomain():
    kws = set(normalize("The invoice and the release deadline"))
    assert lookup_domain(kws) == [("engineering", "delivery"), ("finance", "invoicing"),
                                  ("planning", "scheduling")]


def test_no_trigger_words_means_no_domain():
    assert lookup_domain(set(normalize("lunch menu"))) == []


def test_pipeline_stores_both_levels(env):
    _, _, req = env.ingest("Invoice payment and budget review")
    att = str(req.attachment_id)
    assert env.store.get_tags(att, "domain") == {"finance"}
    assert env.store.get_tags(att, "subdomain") == {"finance/invoicing", "finance/budgeting"}
