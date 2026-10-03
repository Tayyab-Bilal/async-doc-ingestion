"""Bidirectional links between attachments of one tenant, decided by a three-signal cascade."""
from __future__ import annotations

from .store import Store

THRESHOLD = 0.4


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a | b else 0.0


def cascade_score(kw_a: set, kw_b: set, labels_a: set, labels_b: set,
                  meta_a: object, meta_b: object) -> tuple[str, float] | None:
    """Return (signal, score) for the first signal that reaches THRESHOLD, else None.

    Signals are tried in order: keyword Jaccard, label Jaccard, exact metadata match. A signal only
    *applies* when both attachments have data for it (a pair of empty label sets is not "similar").
    An applicable signal below the threshold falls through to the next one. Exact metadata match
    scores 1.0. The score stored on the link is the score of the signal that created it.
    """
    if kw_a and kw_b and (s := jaccard(kw_a, kw_b)) >= THRESHOLD:
        return "keyword", s
    if labels_a and labels_b and (s := jaccard(labels_a, labels_b)) >= THRESHOLD:
        return "label", s
    if meta_a and meta_b and meta_a == meta_b:
        return "metadata", 1.0
    return None


def compute_links(store: Store, attachment_id: str, tenant_id: str) -> list[tuple[str, float]]:
    kw, labels = store.get_tags(attachment_id, "keyword"), store.get_tags(attachment_id, "label")
    meta = store.get_meta(attachment_id, "metadata")
    found = []
    for other in store.tenant_attachments(tenant_id, exclude=attachment_id):
        hit = cascade_score(kw, store.get_tags(other, "keyword"), labels,
                            store.get_tags(other, "label"), meta,
                            store.get_meta(other, "metadata"))
        if hit:
            found.append((other, hit[1]))
    return sorted(found, key=lambda t: t[1], reverse=True)
