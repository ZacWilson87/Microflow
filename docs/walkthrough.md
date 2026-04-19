# microflow — An Annotated Walkthrough

> *"I cannot simplify this further."*

This document walks through `microflow.py` line by line, explaining every design decision. If you've ever wondered what the irreducible core of an AI agent workflow engine looks like — stripped of frameworks, decorators, and magic — this is it.

The inspiration is Andrej Karpathy's microGPT: ~200 lines that contain the full algorithmic content of a large language model. microflow does the same for agent orchestration. ~500 lines of pure Python stdlib, nothing hidden.

---

## The Philosophy: Why One File?

Most orchestration frameworks — Temporal, Prefect, Celery, Airflow — are genuinely useful. But they come at a cost: they conceal the mechanics. You call `@flow` or `@task` and something magical happens. The mental model you build is shallow because the seams are invisible.

microflow makes every seam visible. When you need durable execution, you see exactly what "durable" means: a SQLite `INSERT` after every state transition. When you need retry logic, you see the loop and the sleep. When you need concurrency, you see the `ThreadPoolExecutor`. There is no magic.

This is the right tool for:
- Learning how orchestration engines work
- AI agent pipelines where you want full control
- Projects where adding a framework is overkill

---

## Section 1 — Types & Constants

```python
TICK_INTERVAL = 0.1
_HITL_ATTR    = "_hitl_required"
```

Two constants. `TICK_INTERVAL` is how often the main loop wakes up to check for newly-ready tasks. 100ms is fast enough for human-scale workflows and negligible CPU overhead. `_HITL_ATTR` is the string attribute name `@hitl_required` stamps onto task functions — we'll see this in Section 6.

### The Four Core Dataclasses

**`Task`** is a unit of work: a function, a name, a list of dependency names, and tuning parameters for retry and timeout. The `id` is a `uuid4` generated at `add_task()` time — tasks are identified by UUID internally, by name externally (so you can write `depends_on=["fetch_data"]` instead of an opaque ID).

**`TaskResult`** is the state machine for a task's execution. The `status` field cycles through `pending → running → (retrying →)* success | failed | waiting_hitl | cancelled`. This is the record that gets persisted after every transition.

**`Workflow`** is the container: a tasks dict, a shared context (a mutable `dict` all tasks read and write), and a UUID. The `context` is intentionally mutable — tasks can coordinate by writing into it, and HITL signals can inject data into it via payload merging.

**`HITLSignal`** carries a reviewer's decision: approve, reject, or override. The `payload` dict is merged into `context` on approve/override, which is how a reviewer's annotations flow into subsequent tasks.

**`CyclicDependencyError`** is raised at workflow submission time, not at `run()` time. Fail fast.

---

## Section 2 — Persistence (SQLite)

```python
_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (...);
CREATE TABLE IF NOT EXISTS task_results (...);
CREATE TABLE IF NOT EXISTS hitl_signals (...);
"""
```

Three tables. No ORM. The schema is a string constant executed via `conn.executescript()` on first connection. `IF NOT EXISTS` means it's idempotent — safe to run every time.

The most important design decision in this section is **thread-local connections**:

```python
_conn_local = threading.local()

def _get_conn(db_path: str) -> sqlite3.Connection:
    key = f"conn_{db_path}"
    if not hasattr(_conn_local, key):
        conn = sqlite3.connect(db_path, check_same_thread=False)
        ...
    return getattr(_conn_local, key)
```

SQLite connections are not thread-safe by default. Rather than adding a mutex around every DB call (which would serialize all I/O), we create one connection per thread. Each task runs in its own thread from the executor pool; each gets its own connection. `check_same_thread=False` is set as a safeguard because we might read from a different thread than we wrote from, but the actual concurrent writes are handled by SQLite's built-in WAL mode behavior.

**`_persist_result`** is called on every state transition: `running`, `success`, `failed`, `waiting_hitl`, `cancelled`. This is the durability guarantee. If the process crashes mid-workflow, the next run will call `_load_results()` and pick up from where it left off.

**`task_results` has no `UNIQUE` constraint on `(workflow_id, task_id)`** — multiple rows can exist for the same task (running → success). `_load_results()` uses `ORDER BY rowid ASC` and overwrites the dict, so the last write wins.

---

## Section 3 — DAG Resolver

Kahn's algorithm in ~30 lines:

```python
queue = deque(tid for tid, deg in in_degree.items() if deg == 0)
order = []
while queue:
    tid = queue.popleft()
    order.append(tid)
    for child_id in dependents[tid]:
        in_degree[child_id] -= 1
        if in_degree[child_id] == 0:
            queue.append(child_id)
if len(order) != len(tasks):
    raise CyclicDependencyError(...)
```

Start with all tasks that have no dependencies (in-degree = 0). Emit them. For each emitted task, decrement the in-degree of its children. When a child's in-degree hits zero, it's ready. If the final list is shorter than the task count, some tasks were never emitted — meaning they're in a cycle.

Note that dependencies are declared by **name** (`depends_on=["fetch_data"]`) but stored internally by UUID. The `name_to_id` dict in `_topological_sort` bridges them. This lets users write readable task graphs without worrying about UUID bookkeeping.

**`_ready_tasks`** is called every tick. It returns tasks not yet in `results` whose every dependency is `success`. This is O(tasks × deps) but workflows are small; the simplicity is worth it.

The tick loop reorders operations deliberately: **submit before checking terminal conditions**. This ensures that if a task just succeeded and its downstream is now ready, that downstream gets queued in the same tick — even if an unrelated task has already failed. Without this ordering, a race between a failure and a newly-ready task could cause the runner to exit before the ready task ever runs.

---

## Section 4 — Executor

```python
executor = ThreadPoolExecutor(max_workers=max(1, len(wf.tasks)))
```

One thread per task in the worst case (all tasks run in parallel). The executor is created per workflow and shut down (non-blocking) when the runner exits.

**The key insight**: the executor submits *lifecycle management functions* (`_run_with_retry`, `_run_hitl_task`), not the task functions themselves. Each lifecycle function blocks until the task completes (including retries). This means the executor threads are long-lived during task execution, not just for individual function calls.

**Why not `asyncio`?** Two reasons. First, task functions are arbitrary Python — they might block on file I/O, network calls, or CPU-bound work. Async requires your callees to be async-aware. Second, threads are simpler to explain. The point of microflow is clarity; `asyncio` adds a whole new execution model to explain.

---

## Section 5 — Retry Engine

```python
def _call_with_timeout(fn, args, timeout_s):
    holder = {}
    def _target():
        try: holder["output"] = fn(*args)
        except Exception as exc: holder["error"] = exc
    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive(): raise TimeoutError(...)
    if "error" in holder: raise holder["error"]
    return holder.get("output")
```

Why not `Future.result(timeout=...)`? Because `_run_with_retry` is already running *inside* the executor. Submitting `task.fn` to the same executor would deadlock if `max_workers` is fully occupied — each waiting thread holds a slot, and the task function can never get one. The fix is a fresh daemon thread for each invocation, completely outside the executor pool.

The trade-off: a timed-out task's thread keeps running (Python can't kill threads). The result is marked `failed` immediately; the dangling thread will eventually terminate when its I/O or sleep completes. For most AI agent tasks (LLM calls, HTTP requests) this is acceptable.

**Retry loop**:

```python
while True:
    try:
        output = _call_with_timeout(...)
        return success_result
    except (TimeoutError, Exception) as exc:
        err = str(exc)
    attempt += 1
    if attempt > task.retry_limit:
        return failed_result
    delay = task.retry_delay_s * (2 ** attempt)
    time.sleep(delay)
```

Exponential backoff: `delay_s * 2^attempt`. For `retry_delay_s=1.0`: 2s, 4s, 8s, 16s. The first failure triggers attempt 1, not attempt 0 — so `retry_limit=3` means up to 4 total executions (initial + 3 retries).

---

## Section 6 — HITL Gate

The human-in-the-loop gate is the most novel part of microflow for an AI agent context. AI outputs need human review before consequential actions (sending emails, deploying code, publishing decisions).

```python
def hitl_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    setattr(wrapper, _HITL_ATTR, True)
    return wrapper
```

The decorator stamps a marker attribute onto the function. In the runner, `getattr(task.fn, _HITL_ATTR, False)` routes to `_run_hitl_task` instead of `_run_with_retry`. The wrapper calls through to the original function — it only adds the marker.

**The signal queue**:

```python
_hitl_queues: dict[str, Queue] = {}

def _get_hitl_queue(workflow_id):
    with _hitl_queues_lock:
        if workflow_id not in _hitl_queues:
            _hitl_queues[workflow_id] = Queue()
        return _hitl_queues[workflow_id]
```

One `Queue` per workflow. `send_signal()` puts a `HITLSignal` onto the queue. `_run_hitl_task` blocks on `q.get()` until the signal arrives.

**Why not an `Event`?** Because we need to carry data (the `payload`) and distinguish between signals targeting different tasks in the same workflow. A `Queue` is the right primitive.

The signal matching loop handles the multi-task case:
```python
while True:
    sig = q.get()
    if sig.task_id != task.id:
        q.put(sig)  # return it for other tasks
        time.sleep(0.05)
        continue
    break
```

If a workflow has two parallel HITL tasks, signals for each get routed correctly.

---

## Section 7 — Observability Emitter

```python
class _Emitter:
    def emit(self, workflow_id, task_id, event, payload):
        self._sink({"ts": time.time(), "workflow_id": workflow_id,
                    "task_id": task_id, "event": event, "payload": payload})
```

Every state transition calls `emitter.emit()`. The sink is a `Callable[[dict], None]`. The default writes JSON lines to stdout. `register_sink()` swaps it globally — used by `server.py` to fan out events to SSE and WebSocket subscribers.

This is the observability model from production systems like Jaeger / OpenTelemetry, reduced to its essence. Each event is self-contained: timestamp, which workflow, which task, what happened, with what data. You can pipe stdout to `jq` for live filtering, or write a custom sink to push to any monitoring system.

---

## Section 8 — Workflow Runner

The tick loop is the heart of microflow:

```python
while True:
    # 1. Collect finished futures
    for tid in [tid for tid, fut in in_flight.items() if fut.done()]:
        task_result = in_flight.pop(tid).result()
        results[tid] = task_result
        if task_result.status == "failed":
            _cancel_downstream(...)

    # 2. Submit ready tasks (before terminal check!)
    result_store = _build_result_store(wf.tasks, results)
    for task in _ready_tasks(wf.tasks, results):
        results[task.id] = placeholder   # reserve slot
        in_flight[task.id] = executor.submit(runner, task, ...)

    # 3. Check terminal conditions
    if _all_success(...): return results
    if _any_failed(...) and not in_flight: return results

    time.sleep(TICK_INTERVAL)
```

Three phases per tick, in this order:

**Phase 1**: Poll `in_flight` futures with `.done()` — non-blocking. Collect results. On failure, BFS-cancel all downstream tasks immediately so they don't start.

**Phase 2**: Compute `result_store` (name → output of successful tasks) and find ready tasks. Stamp a `running` placeholder into `results` before submitting — this prevents the tick loop from double-submitting the same task on the next tick.

**Phase 3**: Exit if done. Submit before checking because a task that became ready this tick (its deps just completed in Phase 1) should be queued before we decide to exit — otherwise a workflow where the last dependency completes in the same tick as an unrelated failure could terminate prematurely.

**Crash recovery**: `_load_results()` at startup loads all persisted results. Any tasks already `success` are skipped by `_ready_tasks`. This is "at-least-once" semantics — a task that was `running` when the crash happened will be re-submitted (its thread is gone). Idempotent task functions are recommended.

---

## Section 9 — Public API

Eight functions. That's it.

```python
create_workflow(name, context={})        -> Workflow
add_task(workflow, name, fn, ...)        -> Task
run(workflow, db_path, sink)             -> dict[task_id, TaskResult]
run_async(workflow, ...)                 -> Thread
send_signal(workflow_id, task_id, ...)   -> None
get_status(workflow_id, ...)             -> dict[task_id, TaskResult]
replay(workflow_id, ...)                 -> Workflow
register_sink(sink)                      -> None
```

This is the entire interface. You don't need to know about `_Emitter`, `_run_with_retry`, `_topological_sort`, or any of the internals to use microflow. But they're all there, readable, one file, 500 lines.

---

## What's Not Here (by design)

**No async/await in the core.** Threading is simpler to explain and works with any blocking Python code.

**No distributed execution.** Tasks run in threads in the same process. For distributed workloads, use Temporal or Celery — and then use microflow to understand what they're doing.

**No task function serialization.** `replay()` reconstructs the `Workflow` object but leaves `tasks` empty. Serializing Python callables (pickle, cloudpickle) introduces security and versioning complexity. microflow asks you to re-register functions explicitly.

**No automatic deduplication.** If your process crashes mid-retry, the task runs again. Design your task functions to be idempotent.

---

## Closing Thought

The best software systems are the ones you can hold in your head. microflow is ~500 lines because that's how many lines it takes — no fewer, no more. Every line earns its place.

Build on top of it, fork it, learn from it. The algorithm is the product.

---

## Appendix: The Go Port

`microflow.go` implements the same engine in Go. Reading both files side-by-side is the fastest way to see how the same concepts map onto two different language paradigms.

### Key differences

**Persistence** — Python uses SQLite (it's in the stdlib). Go's stdlib has no SQLite driver, so `microflow.go` writes the workflow state as a JSON file after every transition. The pedagogical point is identical: "durable execution means writing state after every step." A plain-text JSON file is arguably *more* transparent — you can `cat` it and read the state machine directly.

**Concurrency** — Python uses `ThreadPoolExecutor` (a thread pool with a work queue). Go uses goroutines launched directly with `go runTask(...)`. Both run tasks concurrently; Go's scheduler is lighter and doesn't need a pool abstraction because goroutines are cheap enough to create on demand.

**HITL** — Python uses a `queue.Queue` per workflow (a blocking FIFO). Go uses a `chan HITLSignal` with buffer size 1. Both block the task goroutine/thread until a reviewer sends a signal. The Go channel is more idiomatic: select-on-channel is the natural way to express "wait for one of several events."

**Timeout** — Python spawns a daemon thread for the task function and uses `thread.join(timeout)` to bound the wait. Go uses `context.WithTimeout` and a `select` on the done channel vs. the context's Done channel — cleaner because the context cancellation propagates into any downstream calls the task function makes.

**Error handling** — Python raises exceptions; Go returns `(value, error)` pairs. The retry loop structure is identical — the `for attempt := 0; attempt < maxAttempts; attempt++` loop in Go directly mirrors the Python `for attempt in range(max_attempts)` loop.

**Options pattern** — Python uses keyword arguments (`add_task(wf, "name", fn, retries=3, timeout=10.0)`). Go uses the functional options pattern (`AddTask(wf, "name", fn, WithRetries(3), WithTimeout(10.0))`). Both are zero-magic: you can trace exactly what each option does.

### Same section structure

Both files use the same numbered section headers so you can jump between them:

| Section | Python lines | Go lines |
|---------|-------------|---------|
| 1. Types & Constants | 1–68 | 1–87 |
| 2. Persistence | 70–156 | 89–133 |
| 3. DAG Resolver | 159–246 | 135–241 |
| 4–5. Executor & Retry | 248–303 | 243–334 |
| 6. HITL Gate | 305–356 | 253–334 |
| 7. Observability | 358–378 | 416–433 |
| 8. Workflow Runner | 380–425 | 336–414 |
| 9. Public API | 427–500 | 435–510 |

The algorithm is the same. The idioms are different. That's the lesson.
