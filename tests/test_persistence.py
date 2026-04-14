"""Tests for Section 2: Persistence (SQLite read/write round-trip)."""

import tempfile
import time
import os

import pytest

from microflow import (
    Workflow,
    Task,
    TaskResult,
    HITLSignal,
    _persist_workflow,
    _persist_result,
    _load_results,
    _persist_signal,
    _load_workflow_row,
    create_workflow,
    add_task,
)


def noop(context, results):
    return None


@pytest.fixture()
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    os.unlink(path)


def test_persist_and_load_workflow(db):
    wf = create_workflow("test-wf", context={"x": 1})
    _persist_workflow(wf, db)
    row = _load_workflow_row(wf.id, db)
    assert row is not None
    assert row["name"] == "test-wf"


def test_persist_and_load_task_result(db):
    wf = create_workflow("wf", context={})
    task = add_task(wf, "step1", noop)
    _persist_workflow(wf, db)

    now = time.time()
    result = TaskResult(
        task_id=task.id,
        status="success",
        output={"value": 42},
        error=None,
        attempt=0,
        started_at=now,
        finished_at=now + 1.0,
    )
    _persist_result(result, wf.id, db)

    loaded = _load_results(wf.id, db)
    assert task.id in loaded
    r = loaded[task.id]
    assert r.status == "success"
    assert r.output == {"value": 42}
    assert r.error is None
    assert r.attempt == 0


def test_load_results_returns_latest(db):
    """Multiple rows for same task_id → load_results returns the last one."""
    wf = create_workflow("wf", context={})
    task = add_task(wf, "step1", noop)
    _persist_workflow(wf, db)

    now = time.time()
    r1 = TaskResult(task_id=task.id, status="running", output=None,
                    error=None, attempt=0, started_at=now, finished_at=None)
    r2 = TaskResult(task_id=task.id, status="success", output="done",
                    error=None, attempt=0, started_at=now, finished_at=now + 2)

    _persist_result(r1, wf.id, db)
    _persist_result(r2, wf.id, db)

    loaded = _load_results(wf.id, db)
    assert loaded[task.id].status == "success"


def test_persist_hitl_signal(db):
    wf = create_workflow("wf", context={})
    task = add_task(wf, "review", noop)
    _persist_workflow(wf, db)

    sig = HITLSignal(workflow_id=wf.id, task_id=task.id,
                     signal="approve", payload={"note": "looks good"})
    _persist_signal(sig, db)

    from microflow import _get_conn
    conn = _get_conn(db)
    rows = conn.execute("SELECT * FROM hitl_signals").fetchall()
    assert len(rows) == 1
    assert rows[0]["signal"] == "approve"


def test_load_results_empty(db):
    wf = create_workflow("wf", context={})
    _persist_workflow(wf, db)
    loaded = _load_results(wf.id, db)
    assert loaded == {}


def test_missing_workflow_row(db):
    row = _load_workflow_row("nonexistent-id", db)
    assert row is None
