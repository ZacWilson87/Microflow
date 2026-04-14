# microflow.py — The Irreducible AI Agent Workflow Engine
# Zero stdlib-external dependencies. Python 3.11+.
# Read top-to-bottom: every design decision is visible, nothing is magic.

# ── 1. TYPES & CONSTANTS ─────────────────────────────────────────

from __future__ import annotations

import json
import queue as _queue_module
import sqlite3
import threading
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Literal

TICK_INTERVAL = 0.1        # seconds between scheduler ticks
_HITL_ATTR    = "_hitl_required"


@dataclass
class Task:
    id: str
    name: str
    fn: Callable
    depends_on: list[str] = field(default_factory=list)
    retry_limit: int = 3
    retry_delay_s: float = 1.0
    timeout_s: float | None = None


@dataclass
class TaskResult:
    task_id: str
    status: Literal["pending", "running", "success", "failed", "waiting_hitl", "cancelled"]
    output: Any
    error: str | None
    attempt: int
    started_at: float
    finished_at: float | None


@dataclass
class Workflow:
    id: str
    name: str
    tasks: dict[str, Task]   # task_id -> Task
    context: dict            # shared mutable state passed to all tasks
    created_at: float


@dataclass
class HITLSignal:
    workflow_id: str
    task_id: str
    signal: Literal["approve", "reject", "override"]
    payload: dict


class CyclicDependencyError(Exception):
    """Raised when the task graph contains a cycle."""

# ── 2. PERSISTENCE (SQLite) ──────────────────────────────────────
# Every state transition writes a row; replay from DB recovers in-flight
# workflows after a crash. No ORM — raw sqlite3 throughout.

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, context JSON NOT NULL, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS task_results (
    id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, task_id TEXT NOT NULL,
    status TEXT NOT NULL, output JSON, error TEXT, attempt INTEGER NOT NULL DEFAULT 0,
    started_at REAL NOT NULL, finished_at REAL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id)
);
CREATE TABLE IF NOT EXISTS hitl_signals (
    id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, task_id TEXT NOT NULL,
    signal TEXT NOT NULL, payload JSON NOT NULL, created_at REAL NOT NULL
);
"""

_conn_local = threading.local()  # one connection per thread per db_path


def _get_conn(db_path: str) -> sqlite3.Connection:
    key = f"conn_{db_path}"
    if not hasattr(_conn_local, key):
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA)
        conn.commit()
        setattr(_conn_local, key, conn)
    return getattr(_conn_local, key)


def _persist_workflow(wf: Workflow, db_path: str) -> None:
    conn = _get_conn(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO workflows (id,name,context,created_at) VALUES (?,?,?,?)",
        (wf.id, wf.name, json.dumps(wf.context), wf.created_at),
    )
    conn.commit()


def _persist_result(r: TaskResult, workflow_id: str, db_path: str) -> None:
    conn = _get_conn(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO task_results "
        "(id,workflow_id,task_id,status,output,error,attempt,started_at,finished_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (str(uuid.uuid4()), workflow_id, r.task_id, r.status,
         json.dumps(r.output), r.error, r.attempt, r.started_at, r.finished_at),
    )
    conn.commit()


def _load_results(workflow_id: str, db_path: str) -> dict[str, TaskResult]:
    """Load the *latest* TaskResult per task_id for a workflow."""
    rows = _get_conn(db_path).execute(
        "SELECT task_id,status,output,error,attempt,started_at,finished_at "
        "FROM task_results WHERE workflow_id=? ORDER BY rowid ASC", (workflow_id,)
    ).fetchall()
    latest: dict[str, TaskResult] = {}
    for row in rows:
        latest[row["task_id"]] = TaskResult(
            task_id=row["task_id"], status=row["status"],
            output=json.loads(row["output"]) if row["output"] is not None else None,
            error=row["error"], attempt=row["attempt"],
            started_at=row["started_at"], finished_at=row["finished_at"],
        )
    return latest


def _persist_signal(sig: HITLSignal, db_path: str) -> None:
    conn = _get_conn(db_path)
    conn.execute(
        "INSERT INTO hitl_signals (id,workflow_id,task_id,signal,payload,created_at) VALUES (?,?,?,?,?,?)",
        (str(uuid.uuid4()), sig.workflow_id, sig.task_id, sig.signal, json.dumps(sig.payload), time.time()),
    )
    conn.commit()


def _load_workflow_row(workflow_id: str, db_path: str) -> sqlite3.Row | None:
    return _get_conn(db_path).execute(
        "SELECT * FROM workflows WHERE id=?", (workflow_id,)
    ).fetchone()


# ── 3. DAG RESOLVER ──────────────────────────────────────────────
# Kahn's algorithm: maintain in-degree counts; emit nodes whose
# in-degree drops to zero. O(V+E). Cycle detection is free —
# if the sorted list is shorter than node count, a cycle exists.

def _topological_sort(tasks: dict[str, Task]) -> list[str]:
    """Return task IDs in topological order. Raises CyclicDependencyError on cycle."""
    name_to_id = {t.name: t.id for t in tasks.values()}
    in_degree: dict[str, int] = {tid: 0 for tid in tasks}
    dependents: dict[str, list[str]] = defaultdict(list)
    for task in tasks.values():
        for dep_name in task.depends_on:
            dep_id = name_to_id.get(dep_name)
            if dep_id is None:
                raise ValueError(f"Task '{task.name}' depends on unknown task '{dep_name}'")
            in_degree[task.id] += 1
            dependents[dep_id].append(task.id)
    queue: deque[str] = deque(tid for tid, deg in in_degree.items() if deg == 0)
    order: list[str] = []
    while queue:
        tid = queue.popleft()
        order.append(tid)
        for child_id in dependents[tid]:
            in_degree[child_id] -= 1
            if in_degree[child_id] == 0:
                queue.append(child_id)
    if len(order) != len(tasks):
        cycle_nodes = [tasks[tid].name for tid in tasks if tid not in set(order)]
        raise CyclicDependencyError(f"Cycle detected among tasks: {cycle_nodes}")
    return order


def _ready_tasks(tasks: dict[str, Task], results: dict[str, TaskResult]) -> list[Task]:
    """Return tasks whose dependencies are all 'success' and which haven't started."""
    name_to_result = {tasks[tid].name: results.get(tid) for tid in tasks}
    ready = []
    for task in tasks.values():
        if task.id in results:
            continue
        if all(
            name_to_result.get(dep) is not None and name_to_result[dep].status == "success"
            for dep in task.depends_on
        ):
            ready.append(task)
    return ready


def _all_success(tasks: dict[str, Task], results: dict[str, TaskResult]) -> bool:
    return all(results.get(tid) is not None and results[tid].status == "success" for tid in tasks)


def _any_failed(tasks: dict[str, Task], results: dict[str, TaskResult]) -> bool:
    return any(r.status == "failed" for r in results.values())


def _cancel_downstream(
    failed_task: Task,
    tasks: dict[str, Task],
    results: dict[str, TaskResult],
    workflow_id: str,
    db_path: str,
    emitter: _Emitter,
) -> None:
    """Mark tasks transitively downstream of *failed_task* as cancelled (BFS)."""
    name_to_id = {t.name: t.id for t in tasks.values()}
    to_cancel: set[str] = set()
    frontier: deque[str] = deque([failed_task.name])
    while frontier:
        src = frontier.popleft()
        for task in tasks.values():
            if src in task.depends_on and task.id not in results and task.name not in to_cancel:
                to_cancel.add(task.name)
                frontier.append(task.name)
    now = time.time()
    for name in to_cancel:
        tid = name_to_id[name]
        r = TaskResult(tid, "cancelled", None, "upstream task failed", 0, now, now)
        results[tid] = r
        _persist_result(r, workflow_id, db_path)
        emitter.emit(workflow_id, tid, "task_cancelled", {"reason": "upstream_failure"})


# ── 4. EXECUTOR ──────────────────────────────────────────────────
# Each task lifecycle function runs in a ThreadPoolExecutor thread.
# Signature: fn(context: dict, result_store: dict) -> Any
# result_store maps task name -> output of completed tasks.

def _build_result_store(tasks: dict[str, Task], results: dict[str, TaskResult]) -> dict:
    return {tasks[tid].name: results[tid].output for tid in results if results[tid].status == "success"}

# ── 5. RETRY ENGINE ──────────────────────────────────────────────
# Exponential backoff: delay = retry_delay_s * (2 ** attempt)
# Timeout via daemon Thread.join(timeout) — avoids nested executor deadlock.

def _call_with_timeout(fn: Callable, args: tuple, timeout_s: float | None) -> Any:
    """Run fn(*args) in a daemon thread; raise TimeoutError or re-raise fn's exception."""
    holder: dict = {}
    def _target():
        try:
            holder["output"] = fn(*args)
        except Exception as exc:
            holder["error"] = exc
    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        raise TimeoutError(f"Task timed out after {timeout_s}s")
    if "error" in holder:
        raise holder["error"]
    return holder.get("output")


def _run_with_retry(
    task: Task, context: dict, result_store: dict,
    workflow_id: str, db_path: str, emitter: _Emitter,
) -> TaskResult:
    """Execute *task* with retries + timeout. Returns final TaskResult."""
    started_at = time.time()
    attempt = 0
    while True:
        _persist_result(TaskResult(task.id, "running", None, None, attempt, started_at, None),
                        workflow_id, db_path)
        emitter.emit(workflow_id, task.id, "task_started", {"attempt": attempt})
        try:
            output = _call_with_timeout(task.fn, (context, result_store), task.timeout_s)
            r = TaskResult(task.id, "success", output, None, attempt, started_at, time.time())
            _persist_result(r, workflow_id, db_path)
            emitter.emit(workflow_id, task.id, "task_succeeded", {"attempt": attempt, "output": output})
            return r
        except (TimeoutError, Exception) as exc:
            err = str(exc)
        attempt += 1
        if attempt > task.retry_limit:
            r = TaskResult(task.id, "failed", None, err, attempt - 1, started_at, time.time())
            _persist_result(r, workflow_id, db_path)
            emitter.emit(workflow_id, task.id, "task_failed", {"error": err, "attempt": attempt - 1})
            return r
        delay = task.retry_delay_s * (2 ** attempt)
        emitter.emit(workflow_id, task.id, "task_retrying", {"attempt": attempt, "delay_s": delay})
        time.sleep(delay)


# ── 6. HITL GATE ─────────────────────────────────────────────────
# @hitl_required marks a fn. Runner sets status=waiting_hitl and blocks.
# approve/override → payload merged into context, task proceeds.
# reject           → status=failed, downstream tasks cancelled.

_hitl_queues: dict[str, _queue_module.Queue] = {}
_hitl_queues_lock = threading.Lock()


def hitl_required(fn: Callable) -> Callable:
    """Decorator that marks a task function as requiring human approval."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    setattr(wrapper, _HITL_ATTR, True)
    return wrapper


def _get_hitl_queue(workflow_id: str) -> _queue_module.Queue:
    with _hitl_queues_lock:
        if workflow_id not in _hitl_queues:
            _hitl_queues[workflow_id] = _queue_module.Queue()
        return _hitl_queues[workflow_id]


def _run_hitl_task(
    task: Task, context: dict, result_store: dict,
    workflow_id: str, db_path: str, emitter: _Emitter,
) -> TaskResult:
    """Handle a HITL task: block until signal arrives, then approve/reject."""
    started_at = time.time()
    _persist_result(TaskResult(task.id, "waiting_hitl", None, None, 0, started_at, None),
                    workflow_id, db_path)
    emitter.emit(workflow_id, task.id, "hitl_waiting", {})

    q = _get_hitl_queue(workflow_id)
    while True:
        sig: HITLSignal = q.get()
        if sig.task_id != task.id:
            q.put(sig)
            time.sleep(0.05)
            continue
        break

    _persist_signal(sig, db_path)
    emitter.emit(workflow_id, task.id, "hitl_resolved", {"signal": sig.signal, "payload": sig.payload})

    if sig.signal == "reject":
        r = TaskResult(task.id, "failed", None, "rejected by reviewer", 0, started_at, time.time())
        _persist_result(r, workflow_id, db_path)
        return r

    # approve or override: merge payload into context, run normally
    context.update(sig.payload)
    return _run_with_retry(task, context, result_store, workflow_id, db_path, emitter)


# ── 7. OBSERVABILITY EMITTER ─────────────────────────────────────
# All transitions emit a structured event dict to a pluggable sink.
# Default sink: print() as JSON lines. Replace via register_sink().

def print_json_sink(event: dict) -> None:
    """Default observability sink — writes JSON lines to stdout."""
    print(json.dumps(event))


_current_sink: Callable[[dict], None] = print_json_sink


class _Emitter:
    def __init__(self, sink: Callable[[dict], None]) -> None:
        self._sink = sink

    def emit(self, workflow_id: str, task_id: str, event: str, payload: dict) -> None:
        self._sink({"ts": time.time(), "workflow_id": workflow_id,
                    "task_id": task_id, "event": event, "payload": payload})


# ── 8. WORKFLOW RUNNER ───────────────────────────────────────────
# Main loop ticks every TICK_INTERVAL seconds:
#   resolve ready tasks → submit to executor → collect futures → persist → emit
# run()       blocks until workflow finishes or fails.
# run_async() starts run() in a daemon thread and returns it.

def _run_workflow(wf: Workflow, db_path: str, sink: Callable[[dict], None]) -> dict[str, TaskResult]:
    """Core blocking runner. Returns final results dict keyed by task_id."""
    _topological_sort(wf.tasks)   # validate DAG; raises CyclicDependencyError if bad
    emitter = _Emitter(sink)
    _persist_workflow(wf, db_path)
    results: dict[str, TaskResult] = _load_results(wf.id, db_path)   # crash-resume
    in_flight: dict[str, Future] = {}
    executor = ThreadPoolExecutor(max_workers=max(1, len(wf.tasks)))
    _get_hitl_queue(wf.id)  # ensure queue exists before tasks start

    try:
        while True:
            # Collect finished in-flight tasks
            for tid in [tid for tid, fut in in_flight.items() if fut.done()]:
                task_result: TaskResult = in_flight.pop(tid).result()
                results[tid] = task_result
                if task_result.status == "failed":
                    _cancel_downstream(wf.tasks[tid], wf.tasks, results, wf.id, db_path, emitter)

            # Submit ready tasks first so new work is queued before exit check.
            result_store = _build_result_store(wf.tasks, results)
            for task in _ready_tasks(wf.tasks, results):
                if task.id in in_flight:
                    continue
                results[task.id] = TaskResult(task.id, "running", None, None, 0, time.time(), None)
                runner = _run_hitl_task if getattr(task.fn, _HITL_ATTR, False) else _run_with_retry
                in_flight[task.id] = executor.submit(
                    runner, task, wf.context, result_store, wf.id, db_path, emitter
                )

            if _all_success(wf.tasks, results):
                emitter.emit(wf.id, "", "workflow_completed", {"task_count": len(wf.tasks)})
                return results

            if _any_failed(wf.tasks, results) and not in_flight:
                emitter.emit(wf.id, "", "workflow_failed",
                             {"failed_tasks": [tid for tid, r in results.items() if r.status == "failed"]})
                return results

            time.sleep(TICK_INTERVAL)
    finally:
        executor.shutdown(wait=False)


# ── 9. PUBLIC API ────────────────────────────────────────────────
# These 8 functions are the entire public surface of microflow.

def create_workflow(name: str, context: dict | None = None) -> Workflow:
    """Create a new Workflow object. Does not persist or run it yet."""
    return Workflow(id=str(uuid.uuid4()), name=name, tasks={},
                    context=context or {}, created_at=time.time())


def add_task(
    workflow: Workflow, name: str, fn: Callable,
    depends_on: list[str] | None = None,
    retry_limit: int = 3, retry_delay_s: float = 1.0, timeout_s: float | None = None,
) -> Task:
    """Add a task to *workflow* and return the Task object."""
    task = Task(id=str(uuid.uuid4()), name=name, fn=fn,
                depends_on=depends_on or [], retry_limit=retry_limit,
                retry_delay_s=retry_delay_s, timeout_s=timeout_s)
    workflow.tasks[task.id] = task
    return task


def run(
    workflow: Workflow, db_path: str = "microflow.db",
    sink: Callable[[dict], None] | None = None,
) -> dict[str, TaskResult]:
    """Run *workflow* synchronously. Returns dict[task_id -> TaskResult]."""
    return _run_workflow(workflow, db_path, sink or _current_sink)


def run_async(
    workflow: Workflow, db_path: str = "microflow.db",
    sink: Callable[[dict], None] | None = None,
) -> threading.Thread:
    """Run *workflow* in a background thread. Returns the Thread (already started)."""
    t = threading.Thread(target=_run_workflow,
                         args=(workflow, db_path, sink or _current_sink),
                         daemon=True, name=f"microflow-{workflow.id[:8]}")
    t.start()
    return t


def send_signal(
    workflow_id: str, task_id: str, signal: str,
    payload: dict | None = None, db_path: str = "microflow.db",
) -> None:
    """Send a HITL signal to a waiting task. signal must be approve/reject/override."""
    if signal not in ("approve", "reject", "override"):
        raise ValueError(f"Invalid signal '{signal}'. Must be approve, reject, or override.")
    sig = HITLSignal(workflow_id=workflow_id, task_id=task_id,
                     signal=signal, payload=payload or {})  # type: ignore[arg-type]
    _get_hitl_queue(workflow_id).put(sig)


def get_status(workflow_id: str, db_path: str = "microflow.db") -> dict[str, TaskResult]:
    """Load and return the latest TaskResult for every task in *workflow_id*."""
    return _load_results(workflow_id, db_path)


def replay(workflow_id: str, db_path: str = "microflow.db") -> Workflow:
    """Reconstruct a Workflow shell from persisted data (tasks not re-attached).

    # TODO: full replay requires serialising task *functions*, which is not
    # trivially safe. Returns a Workflow with empty tasks dict so callers can
    # inspect context/identity. Re-register tasks before calling run() again.
    """
    row = _load_workflow_row(workflow_id, db_path)
    if row is None:
        raise KeyError(f"No workflow with id={workflow_id!r} in {db_path!r}")
    return Workflow(id=row["id"], name=row["name"], tasks={},
                    context=json.loads(row["context"]), created_at=row["created_at"])


def register_sink(sink: Callable[[dict], None]) -> None:
    """Replace the global default observability sink."""
    global _current_sink
    _current_sink = sink
