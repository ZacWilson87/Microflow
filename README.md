# microflow

> The irreducible AI agent workflow engine.

Two implementations of the same engine, same philosophy, two languages:

| | Python | Go |
|---|---|---|
| File | `microflow.py` | `microflow.go` |
| Lines | ≤ 500 | ~510 |
| Dependencies | none (stdlib only) | none (stdlib only) |
| Persistence | SQLite | JSON file |

Read either file top-to-bottom and you will understand exactly how durable
execution, DAG scheduling, retry logic, HITL signals, and structured
observability work — no framework magic, no hidden seams.

## Python quickstart

```python
from microflow import create_workflow, add_task, run

def fetch_data(context, results):
    return {"rows": 42}

def process(context, results):
    rows = results["fetch_data"]["rows"]
    return {"processed": rows * 2}

wf = create_workflow("demo", context={"env": "prod"})
add_task(wf, "fetch_data", fetch_data)
add_task(wf, "process", process, depends_on=["fetch_data"])
results = run(wf)
```

## Go quickstart

```go
import mf "github.com/zacwilson87/microflow"

wf := mf.CreateWorkflow("demo", map[string]any{"env": "prod"})
mf.AddTask(wf, "fetch_data", fetchData)
mf.AddTask(wf, "process", process, mf.WithDependsOn("fetch_data"))
results, err := mf.Run(wf)
```

## Philosophy

microflow is pedagogical clarity applied to AI agent orchestration.
A developer should be able to read either source file top-to-bottom and
understand exactly how the engine works — without any framework magic
obscuring the seams.

Inspiration: Karpathy's microGPT — ~200 lines that contain the full
algorithmic content of an LLM. microflow does the same for agent
orchestration.

## Features

- **DAG scheduling** — declare task dependencies; microflow resolves execution order via Kahn's algorithm
- **Durable execution** — every state transition persisted; resume after crash
- **Retry / backoff** — configurable retry limits with exponential backoff per task
- **Human-in-the-loop** — pause execution until a reviewer approves or rejects
- **Structured observability** — JSON-line events emitted on every transition; pluggable sink

## Python examples

```bash
python examples/hello_world.py
python examples/parallel_fan.py
python examples/hitl_review.py
python examples/retry_chaos.py
```

## Go examples

```bash
go run ./examples/go/hello_world
go run ./examples/go/parallel_fan
go run ./examples/go/hitl_review
go run ./examples/go/retry_chaos
```

## Running the tests

```bash
# Python
pytest tests/

# Go
go test ./...
```

## Optional Python server

```bash
uvicorn server:app --reload
```

## License

MIT
