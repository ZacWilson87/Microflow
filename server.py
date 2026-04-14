"""server.py — Optional FastAPI REST + WebSocket server for microflow.

Install extras:  pip install microflow[server]
Run:             uvicorn server:app --reload

Endpoints
---------
POST   /workflows              Create and run a workflow from a JSON task graph
GET    /workflows              List all known workflows
GET    /workflows/{id}         Get current task statuses
POST   /workflows/{id}/signal  Send a HITL signal (approve/reject/override)
GET    /workflows/{id}/events  SSE stream of observability events (text/event-stream)
WS     /ws/{id}                WebSocket stream of observability events
"""

from __future__ import annotations

import asyncio
import json
import queue
import threading
import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import microflow
from microflow import (
    TaskResult, Workflow,
    create_workflow, add_task, run_async, send_signal, get_status,
    print_json_sink, register_sink,
)

app = FastAPI(title="microflow", description="The irreducible AI agent workflow engine.")

# ── In-memory state ───────────────────────────────────────────────
# workflow_id -> Workflow object (kept alive so tasks dict is accessible)
_workflows: dict[str, Workflow] = {}

# workflow_id -> list of subscriber queues (one per SSE/WS client)
_event_subscribers: dict[str, list[queue.Queue]] = {}
_subs_lock = threading.Lock()


# ── Event routing ─────────────────────────────────────────────────

def _fanout_sink(event: dict) -> None:
    """Observability sink that fans out events to all subscribers + stdout."""
    print_json_sink(event)
    wid = event.get("workflow_id", "")
    with _subs_lock:
        for q in _event_subscribers.get(wid, []):
            q.put(event)


register_sink(_fanout_sink)


def _subscribe(workflow_id: str) -> queue.Queue:
    q: queue.Queue = queue.Queue()
    with _subs_lock:
        _event_subscribers.setdefault(workflow_id, []).append(q)
    return q


def _unsubscribe(workflow_id: str, q: queue.Queue) -> None:
    with _subs_lock:
        subs = _event_subscribers.get(workflow_id, [])
        if q in subs:
            subs.remove(q)


# ── Request / response models ─────────────────────────────────────

class TaskSpec(BaseModel):
    name: str
    code: str        # Python source of the task function (exec'd at runtime)
    depends_on: list[str] = []
    retry_limit: int = 3
    retry_delay_s: float = 1.0
    timeout_s: float | None = None


class WorkflowRequest(BaseModel):
    name: str
    context: dict = {}
    tasks: list[TaskSpec]
    db_path: str = "microflow.db"


class SignalRequest(BaseModel):
    task_id: str
    signal: str        # approve | reject | override
    payload: dict = {}


def _result_to_dict(r: TaskResult) -> dict:
    return {
        "task_id": r.task_id,
        "status": r.status,
        "output": r.output,
        "error": r.error,
        "attempt": r.attempt,
        "started_at": r.started_at,
        "finished_at": r.finished_at,
    }


# ── Routes ────────────────────────────────────────────────────────

@app.post("/workflows", status_code=202)
def create_and_run_workflow(req: WorkflowRequest) -> dict:
    """Create a workflow from a JSON task graph and start it asynchronously.

    Task functions are supplied as Python source strings executed in a
    restricted namespace. Each function must be named ``fn`` and accept
    ``(context, results)`` arguments.

    Example task spec:
        {"name": "greet", "code": "def fn(ctx, res): return 'hello'"}
    """
    wf = create_workflow(req.name, context=req.context)
    _workflows[wf.id] = wf

    for spec in req.tasks:
        ns: dict[str, Any] = {}
        try:
            exec(compile(spec.code, f"<task:{spec.name}>", "exec"), ns)  # noqa: S102
        except SyntaxError as exc:
            raise HTTPException(status_code=400, detail=f"Syntax error in task '{spec.name}': {exc}")
        fn = ns.get("fn")
        if fn is None:
            raise HTTPException(status_code=400,
                                detail=f"Task '{spec.name}' code must define a function named 'fn'")
        add_task(wf, spec.name, fn,
                 depends_on=spec.depends_on,
                 retry_limit=spec.retry_limit,
                 retry_delay_s=spec.retry_delay_s,
                 timeout_s=spec.timeout_s)

    run_async(wf, db_path=req.db_path)
    return {"workflow_id": wf.id, "name": wf.name, "status": "running"}


@app.get("/workflows")
def list_workflows() -> list[dict]:
    """List all workflows started in this server process."""
    return [{"workflow_id": wid, "name": wf.name, "created_at": wf.created_at}
            for wid, wf in _workflows.items()]


@app.get("/workflows/{workflow_id}")
def workflow_status(workflow_id: str, db_path: str = "microflow.db") -> dict:
    """Get the current status of all tasks in a workflow."""
    results = get_status(workflow_id, db_path=db_path)
    if not results and workflow_id not in _workflows:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {
        "workflow_id": workflow_id,
        "tasks": {tid: _result_to_dict(r) for tid, r in results.items()},
    }


@app.post("/workflows/{workflow_id}/signal")
def post_signal(workflow_id: str, req: SignalRequest,
                db_path: str = "microflow.db") -> dict:
    """Send a HITL signal (approve/reject/override) to a waiting task."""
    try:
        send_signal(workflow_id, req.task_id, req.signal,
                    payload=req.payload, db_path=db_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"workflow_id": workflow_id, "task_id": req.task_id, "signal": req.signal}


@app.get("/workflows/{workflow_id}/events")
def event_stream(workflow_id: str) -> StreamingResponse:
    """Server-Sent Events stream of observability events for a workflow."""
    q = _subscribe(workflow_id)

    def generate():
        try:
            yield f"data: {json.dumps({'event': 'connected', 'workflow_id': workflow_id})}\n\n"
            while True:
                try:
                    event = q.get(timeout=15)
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("event") in ("workflow_completed", "workflow_failed"):
                        break
                except queue.Empty:
                    yield ": keepalive\n\n"  # SSE comment keeps connection alive
        finally:
            _unsubscribe(workflow_id, q)

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.websocket("/ws/{workflow_id}")
async def websocket_events(websocket: WebSocket, workflow_id: str) -> None:
    """WebSocket stream of observability events for a workflow."""
    await websocket.accept()
    q = _subscribe(workflow_id)
    loop = asyncio.get_event_loop()
    try:
        while True:
            # Run blocking queue.get in a thread pool to keep the event loop free
            try:
                event = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: q.get(timeout=15)),
                    timeout=16,
                )
                await websocket.send_text(json.dumps(event))
                if event.get("event") in ("workflow_completed", "workflow_failed"):
                    break
            except (asyncio.TimeoutError, queue.Empty):
                await websocket.send_text(json.dumps({"event": "keepalive"}))
    except WebSocketDisconnect:
        pass
    finally:
        _unsubscribe(workflow_id, q)
        await websocket.close()
