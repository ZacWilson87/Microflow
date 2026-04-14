"""Tests for Section 3: DAG Resolver (topological sort + cycle detection)."""

import pytest

from microflow import (
    CyclicDependencyError,
    create_workflow,
    add_task,
    _topological_sort,
    _ready_tasks,
    TaskResult,
)
import time


def noop(context, results):
    return None


# ── helpers ──────────────────────────────────────────────────────

def _names_in_order(wf, order_ids):
    return [wf.tasks[tid].name for tid in order_ids]


def _make_success(task_id):
    now = time.time()
    return TaskResult(task_id=task_id, status="success", output=None,
                      error=None, attempt=0, started_at=now, finished_at=now)


# ── topological sort tests ────────────────────────────────────────

def test_linear_chain():
    wf = create_workflow("linear")
    t1 = add_task(wf, "a", noop)
    t2 = add_task(wf, "b", noop, depends_on=["a"])
    t3 = add_task(wf, "c", noop, depends_on=["b"])

    order = _topological_sort(wf.tasks)
    names = _names_in_order(wf, order)
    assert names.index("a") < names.index("b") < names.index("c")


def test_parallel_fan_out():
    wf = create_workflow("fan")
    root = add_task(wf, "root", noop)
    b = add_task(wf, "b", noop, depends_on=["root"])
    c = add_task(wf, "c", noop, depends_on=["root"])
    d = add_task(wf, "d", noop, depends_on=["root"])

    order = _topological_sort(wf.tasks)
    names = _names_in_order(wf, order)
    assert names.index("root") < names.index("b")
    assert names.index("root") < names.index("c")
    assert names.index("root") < names.index("d")


def test_diamond():
    #   A
    #  / \
    # B   C
    #  \ /
    #   D
    wf = create_workflow("diamond")
    a = add_task(wf, "a", noop)
    b = add_task(wf, "b", noop, depends_on=["a"])
    c = add_task(wf, "c", noop, depends_on=["a"])
    d = add_task(wf, "d", noop, depends_on=["b", "c"])

    order = _topological_sort(wf.tasks)
    names = _names_in_order(wf, order)
    assert names.index("a") < names.index("b")
    assert names.index("a") < names.index("c")
    assert names.index("b") < names.index("d")
    assert names.index("c") < names.index("d")


def test_single_task():
    wf = create_workflow("single")
    add_task(wf, "only", noop)
    order = _topological_sort(wf.tasks)
    assert len(order) == 1


def test_cycle_detection_simple():
    wf = create_workflow("cycle")
    add_task(wf, "a", noop, depends_on=["b"])
    add_task(wf, "b", noop, depends_on=["a"])
    with pytest.raises(CyclicDependencyError):
        _topological_sort(wf.tasks)


def test_cycle_detection_self_loop():
    wf = create_workflow("self-loop")
    add_task(wf, "a", noop, depends_on=["a"])
    with pytest.raises(CyclicDependencyError):
        _topological_sort(wf.tasks)


def test_cycle_detection_three_node():
    wf = create_workflow("three-cycle")
    add_task(wf, "a", noop, depends_on=["c"])
    add_task(wf, "b", noop, depends_on=["a"])
    add_task(wf, "c", noop, depends_on=["b"])
    with pytest.raises(CyclicDependencyError):
        _topological_sort(wf.tasks)


def test_unknown_dependency_raises():
    wf = create_workflow("bad-dep")
    add_task(wf, "a", noop, depends_on=["nonexistent"])
    with pytest.raises(ValueError, match="unknown task"):
        _topological_sort(wf.tasks)


# ── ready_tasks tests ─────────────────────────────────────────────

def test_ready_tasks_no_deps():
    wf = create_workflow("ready")
    t1 = add_task(wf, "a", noop)
    t2 = add_task(wf, "b", noop)
    ready = _ready_tasks(wf.tasks, {})
    ready_names = {t.name for t in ready}
    assert ready_names == {"a", "b"}


def test_ready_tasks_blocked_until_dep_done():
    wf = create_workflow("blocked")
    t1 = add_task(wf, "a", noop)
    t2 = add_task(wf, "b", noop, depends_on=["a"])

    # Nothing done yet: only "a" is ready
    ready = _ready_tasks(wf.tasks, {})
    assert {t.name for t in ready} == {"a"}

    # "a" succeeds: now "b" should be ready
    results = {t1.id: _make_success(t1.id)}
    ready = _ready_tasks(wf.tasks, results)
    assert {t.name for t in ready} == {"b"}


def test_ready_tasks_excludes_started():
    wf = create_workflow("started")
    t1 = add_task(wf, "a", noop)

    # Mark t1 as running (present in results)
    now = time.time()
    results = {t1.id: TaskResult(task_id=t1.id, status="running", output=None,
                                  error=None, attempt=0, started_at=now, finished_at=None)}
    ready = _ready_tasks(wf.tasks, results)
    assert ready == []


def test_ready_tasks_fan_in():
    wf = create_workflow("fan-in")
    a = add_task(wf, "a", noop)
    b = add_task(wf, "b", noop)
    c = add_task(wf, "c", noop, depends_on=["a", "b"])

    # Neither a nor b done: c not ready
    ready = _ready_tasks(wf.tasks, {})
    assert "c" not in {t.name for t in ready}

    # Only a done: c still not ready
    results = {a.id: _make_success(a.id)}
    ready = _ready_tasks(wf.tasks, results)
    assert "c" not in {t.name for t in ready}

    # Both done: c ready
    results[b.id] = _make_success(b.id)
    ready = _ready_tasks(wf.tasks, results)
    assert "c" in {t.name for t in ready}
