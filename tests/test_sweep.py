from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from async_doc_ingestion.sweep import sweep


def unindexed_env(env, n: int):
    from conftest import RecordingRunner

    env.service.runner = RecordingRunner()  # submit only enqueues
    ids = []
    for i in range(n):
        job_id, _, _ = env.ingest(f"report number {i} about invoice payments")
        env.pipeline.process(job_id)
        ids.append(job_id)
    return ids


def later(minutes: int) -> datetime:
    return datetime.now(UTC) + timedelta(minutes=minutes)


def test_sweep_reindexes_complete_but_unindexed(env):
    ids = unindexed_env(env, 3)
    assert all(not env.store.get_job(i).indexed for i in ids)

    claimed = sweep(env.store, env.inline, later(20))

    assert sorted(claimed) == sorted(ids)
    assert all(env.store.get_job(i).indexed for i in ids)
    assert env.prov.vectors.count("tenant_acme") == 3
    assert env.prov.embedder.calls == 3
    assert sweep(env.store, env.inline, later(20)) == []  # nothing left, nothing re-paid
    assert env.prov.embedder.calls == 3


def test_sweep_ignores_empty_documents(env):
    from conftest import RecordingRunner

    env.service.runner = RecordingRunner()
    job_id, _, _ = env.ingest("   ")
    env.pipeline.process(job_id)
    assert sweep(env.store, env.inline, later(60)) == []
    assert env.prov.embedder.calls == 0


def test_sweep_respects_min_age_attempt_cap_batch(env):
    ids = unindexed_env(env, 7)

    # min age: fresh jobs are left alone, they may still be in flight
    assert sweep(env.store, env.inline, later(14)) == []
    assert env.prov.embedder.calls == 0

    # batch cap: 7 eligible, 3 per sweep
    first = sweep(env.store, env.inline, later(20), batch=3)
    second = sweep(env.store, env.inline, later(20), batch=3)
    third = sweep(env.store, env.inline, later(20), batch=3)
    assert (len(first), len(second), len(third)) == (3, 3, 1)
    assert sorted(first + second + third) == sorted(ids)

    # attempt cap: a runner that never indexes burns attempts, then the job is left for humans
    from conftest import RecordingRunner

    stuck, _, _ = env.ingest("one more stuck report on invoices")
    env.pipeline.process(stuck)
    dead_runner = RecordingRunner()
    sizes = [len(sweep(env.store, dead_runner, later(20))) for _ in range(7)]
    assert sizes == [1, 1, 1, 1, 1, 0, 0]  # max_attempts = 5
    assert env.store.get_job(stuck).attempts == 5
    assert env.store.get_job(stuck).indexed is False


def test_two_concurrent_sweeps_never_double_index(env):
    ids = unindexed_env(env, 12)
    env.prov.embedder.delay = 0.01  # keep each index call in flight so the sweeps overlap
    barrier = threading.Barrier(2)

    def drain() -> list[str]:
        barrier.wait()  # both sweepers start at the same instant, against the same rows
        mine: list[str] = []
        while batch := sweep(env.store, env.inline, later(20), batch=3):
            mine += batch
        return mine

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda _: drain(), range(2)))

    claimed = results[0] + results[1]
    assert len(claimed) == len(set(claimed)) == 12  # no job claimed twice
    assert sorted(claimed) == sorted(ids)
    assert env.prov.embedder.calls == 12  # one paid embed per job, not 24
    assert all(env.store.get_job(i).indexed for i in ids)
