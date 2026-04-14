"""Tests for Sections 4 & 5: Executor, Retry Engine, and full Workflow Runner."""

import os
import tempfile
import time
import threading

import pytest

from microflow import (
    create_workflow,
    add_task,
    run,
    run_async,
    get_status,
)


# ── fixtures ──────────────────────────────────────────────────────

@pytest.fixture()
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    os.unlink(path)


def silent_sink(event):
    pass  # suppress stdout during tests


# ── success path ──────────────────────────────────────────────────

def test_single_task_success(db):
    def work(ctx, res):
        return {"done": True}

    wf = create_workflow("single", context={})
    add_task(wf, "work", work, retry_limit=0)
    results = run(wf, db_path=db, sink=silent_sink)
    assert all(r.status == "success" for r in results.values())


def test_linear_chain_passes_results(db):
    def step1(ctx, res):
        return 10

    def step2(ctx, res):
        assert res["step1"] == 10
        return res["step1"] * 2

    wf = create_workflow("chain")
    add_task(wf, "step1", step1, retry_limit=0)
    add_task(wf, "step2", step2, depends_on=["step1"], retry_limit=0)
    results = run(wf, db_path=db, sink=silent_sink)
    step2_result = next(r for r in results.values() if r.output == 20)
    assert step2_result.status == "success"


def test_parallel_tasks_run_concurrently(db):
    barrier = threading.Barrier(2, timeout=5)

    def slow_a(ctx, res):
        barrier.wait()
        return "a"

    def slow_b(ctx, res):
        barrier.wait()
        return "b"

    wf = create_workflow("parallel")
    add_task(wf, "a", slow_a, retry_limit=0)
    add_task(wf, "b", slow_b, retry_limit=0)

    start = time.time()
    results = run(wf, db_path=db, sink=silent_sink)
    elapsed = time.time() - start

    assert all(r.status == "success" for r in results.values())
    # Both tasks ran concurrently, so total time should be roughly 1x not 2x
    assert elapsed < 3.0


def test_context_shared_across_tasks(db):
    def writer(ctx, res):
        ctx["written"] = 99
        return ctx["written"]

    def reader(ctx, res):
        return ctx["written"]

    wf = create_workflow("ctx-share", context={})
    add_task(wf, "writer", writer, retry_limit=0)
    add_task(wf, "reader", reader, depends_on=["writer"], retry_limit=0)
    results = run(wf, db_path=db, sink=silent_sink)
    reader_result = next(r for r in results.values() if r.output == 99)
    assert reader_result.status == "success"


# ── retry path ────────────────────────────────────────────────────

def test_retry_then_succeed(db):
    attempts = {"n": 0}

    def flaky(ctx, res):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("not yet")
        return "ok"

    wf = create_workflow("retry-succeed")
    add_task(wf, "flaky", flaky, retry_limit=3, retry_delay_s=0.01)
    results = run(wf, db_path=db, sink=silent_sink)
    r = list(results.values())[0]
    assert r.status == "success"
    assert r.output == "ok"


def test_exhaust_retries_marks_failed(db):
    def always_fail(ctx, res):
        raise RuntimeError("always")

    wf = create_workflow("exhaust")
    add_task(wf, "bad", always_fail, retry_limit=2, retry_delay_s=0.01)
    results = run(wf, db_path=db, sink=silent_sink)
    r = list(results.values())[0]
    assert r.status == "failed"
    assert "always" in r.error


def test_timeout_causes_failure(db):
    def slow(ctx, res):
        time.sleep(5)
        return "never"

    wf = create_workflow("timeout")
    add_task(wf, "slow", slow, retry_limit=0, timeout_s=0.2)
    results = run(wf, db_path=db, sink=silent_sink)
    r = list(results.values())[0]
    assert r.status == "failed"
    assert "timed out" in r.error.lower()


def test_failed_task_cancels_downstream(db):
    def fail(ctx, res):
        raise RuntimeError("boom")

    def should_not_run(ctx, res):
        return "ran"

    wf = create_workflow("cancel-downstream")
    t1 = add_task(wf, "fail", fail, retry_limit=0)
    add_task(wf, "downstream", should_not_run, depends_on=["fail"], retry_limit=0)

    results = run(wf, db_path=db, sink=silent_sink)
    statuses = {r.status for r in results.values()}
    assert "failed" in statuses
    assert "cancelled" in statuses
    assert "success" not in statuses


# ── run_async ─────────────────────────────────────────────────────

def test_run_async_returns_thread(db):
    def work(ctx, res):
        time.sleep(0.05)
        return True

    wf = create_workflow("async")
    add_task(wf, "work", work, retry_limit=0)
    t = run_async(wf, db_path=db, sink=silent_sink)
    assert isinstance(t, threading.Thread)
    t.join(timeout=5)
    assert not t.is_alive()

    status = get_status(wf.id, db_path=db)
    assert all(r.status == "success" for r in status.values())


# ── cycle detection at run() time ─────────────────────────────────

def test_run_raises_on_cycle(db):
    from microflow import CyclicDependencyError

    wf = create_workflow("cycle")
    add_task(wf, "a", lambda c, r: None, depends_on=["b"])
    add_task(wf, "b", lambda c, r: None, depends_on=["a"])

    with pytest.raises(CyclicDependencyError):
        run(wf, db_path=db, sink=silent_sink)
