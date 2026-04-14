"""Tests for Section 6: HITL Gate (approve / reject / override paths)."""

import os
import tempfile
import threading
import time

import pytest

from microflow import (
    create_workflow,
    add_task,
    run_async,
    send_signal,
    get_status,
    hitl_required,
)


# ── fixtures ──────────────────────────────────────────────────────

@pytest.fixture()
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    os.unlink(path)


def silent_sink(event):
    pass


# ── helpers ──────────────────────────────────────────────────────

def _wait_for_status(workflow_id, task_id, target_status, db, timeout=5.0):
    """Poll until the given task reaches *target_status*."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        statuses = get_status(workflow_id, db_path=db)
        r = statuses.get(task_id)
        if r and r.status == target_status:
            return r
        time.sleep(0.05)
    raise TimeoutError(f"task {task_id} never reached {target_status!r}")


# ── approve path ──────────────────────────────────────────────────

def test_hitl_approve(db):
    @hitl_required
    def review_step(ctx, res):
        return {"approved": True, "reviewer_note": ctx.get("note")}

    wf = create_workflow("hitl-approve", context={})
    task = add_task(wf, "review_step", review_step, retry_limit=0)

    t = run_async(wf, db_path=db, sink=silent_sink)

    # Wait for waiting_hitl status
    _wait_for_status(wf.id, task.id, "waiting_hitl", db)

    # Approve with extra payload
    send_signal(wf.id, task.id, "approve", payload={"note": "LGTM"}, db_path=db)

    t.join(timeout=10)
    assert not t.is_alive()

    results = get_status(wf.id, db_path=db)
    assert results[task.id].status == "success"
    assert results[task.id].output["reviewer_note"] == "LGTM"


# ── reject path ───────────────────────────────────────────────────

def test_hitl_reject(db):
    @hitl_required
    def gated(ctx, res):
        return "should not run"

    def downstream(ctx, res):
        return "ran"

    wf = create_workflow("hitl-reject", context={})
    gate = add_task(wf, "gated", gated, retry_limit=0)
    down = add_task(wf, "downstream", downstream, depends_on=["gated"], retry_limit=0)

    t = run_async(wf, db_path=db, sink=silent_sink)
    _wait_for_status(wf.id, gate.id, "waiting_hitl", db)

    send_signal(wf.id, gate.id, "reject", db_path=db)

    t.join(timeout=10)
    assert not t.is_alive()

    results = get_status(wf.id, db_path=db)
    assert results[gate.id].status == "failed"
    assert results[gate.id].error == "rejected by reviewer"
    assert results[down.id].status == "cancelled"


# ── override path ─────────────────────────────────────────────────

def test_hitl_override(db):
    @hitl_required
    def guarded(ctx, res):
        return {"override_value": ctx.get("injected", "none")}

    wf = create_workflow("hitl-override", context={})
    task = add_task(wf, "guarded", guarded, retry_limit=0)

    t = run_async(wf, db_path=db, sink=silent_sink)
    _wait_for_status(wf.id, task.id, "waiting_hitl", db)

    send_signal(wf.id, task.id, "override", payload={"injected": "force-value"}, db_path=db)

    t.join(timeout=10)
    assert not t.is_alive()

    results = get_status(wf.id, db_path=db)
    assert results[task.id].status == "success"
    assert results[task.id].output["override_value"] == "force-value"


# ── invalid signal ────────────────────────────────────────────────

def test_invalid_signal_raises(db):
    with pytest.raises(ValueError, match="Invalid signal"):
        send_signal("some-wf", "some-task", "banana", db_path=db)


# ── hitl_required decorator preserves function ────────────────────

def test_hitl_required_marks_function():
    from microflow import _HITL_ATTR

    @hitl_required
    def fn(ctx, res):
        return 42

    assert getattr(fn, _HITL_ATTR, False) is True
    assert fn({}, {}) == 42  # still callable


# ── sequential hitl tasks ─────────────────────────────────────────

def test_multiple_hitl_tasks_in_sequence(db):
    @hitl_required
    def step1(ctx, res):
        return "step1-done"

    @hitl_required
    def step2(ctx, res):
        return "step2-done"

    wf = create_workflow("hitl-sequential", context={})
    t1 = add_task(wf, "step1", step1, retry_limit=0)
    t2 = add_task(wf, "step2", step2, depends_on=["step1"], retry_limit=0)

    t = run_async(wf, db_path=db, sink=silent_sink)

    # Approve first gate
    _wait_for_status(wf.id, t1.id, "waiting_hitl", db)
    send_signal(wf.id, t1.id, "approve", db_path=db)

    # Approve second gate
    _wait_for_status(wf.id, t2.id, "waiting_hitl", db)
    send_signal(wf.id, t2.id, "approve", db_path=db)

    t.join(timeout=10)
    assert not t.is_alive()

    results = get_status(wf.id, db_path=db)
    assert results[t1.id].status == "success"
    assert results[t2.id].status == "success"
