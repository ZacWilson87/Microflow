# microflow

> The irreducible AI agent workflow engine.

microflow.py is a single file of ~500 lines of pure Python with no
dependencies that implements a complete AI agent orchestration engine:
DAG scheduling, durable execution, retry/backoff, human-in-the-loop
gates, and structured observability.

I cannot simplify this further.

## Quickstart

```python
from microflow import create_workflow, add_task, run

def fetch_data(context, results):
    return {"rows": 42}

def process(context, results):
    rows = results["fetch_data"]["rows"]
    return {"processed": rows * 2}

wf = create_workflow("demo", context={"env": "prod"})
t1 = add_task(wf, "fetch_data", fetch_data)
t2 = add_task(wf, "process", process, depends_on=["fetch_data"])
results = run(wf)
```

## Philosophy

microflow is pedagogical clarity applied to AI agent orchestration.
A developer should be able to read `microflow.py` top-to-bottom and
understand exactly how durable execution, DAG scheduling, retry logic,
HITL signals, and structured observability work — without any framework
magic obscuring the seams.

The canonical constraint: the engine core fits in ≤ 500 lines of pure
Python stdlib. No Temporal. No Prefect. No Celery. No dependencies in
the core file.

Inspiration: Karpathy's microGPT — 200 lines that contain the full
algorithmic content of an LLM. microflow does the same for agent
orchestration.

## Features

- **DAG scheduling** — declare task dependencies; microflow resolves execution order via Kahn's algorithm
- **Durable execution** — every state transition persisted to SQLite; resume after crash
- **Retry / backoff** — configurable retry limits with exponential backoff per task
- **Human-in-the-loop** — `@hitl_required` pauses execution until a reviewer approves or rejects
- **Structured observability** — JSON-line events emitted on every transition; pluggable sink
- **Optional REST + WebSocket server** — `server.py` wraps the engine in FastAPI with SSE streaming

## Installation

```bash
# core (zero deps)
pip install microflow

# with optional server layer
pip install microflow[server]
```

## Examples

```bash
python examples/hello_world.py
python examples/parallel_fan.py
python examples/hitl_review.py
python examples/retry_chaos.py
```

## Running the server

```bash
uvicorn server:app --reload
```

## License

MIT
