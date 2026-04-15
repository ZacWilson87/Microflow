# microflow — Agent Context

## What this project is
microflow is a single-file Python implementation of an AI agent workflow engine.
The entire engine lives in `microflow.py` (≤ 500 lines, zero stdlib-external deps).
The optional server layer is in `server.py`.

## Key invariants
- `microflow.py` must remain ≤ 500 lines
- `microflow.py` may only import from the Python stdlib
- All public surface is the 8 functions in Section 9
- SQLite is the only persistence mechanism

## Running tests
```bash
pytest tests/
```

## Running examples (pure Python engine)
```bash
python examples/hello_world.py
python examples/parallel_fan.py
python examples/hitl_review.py
python examples/retry_chaos.py
```

## Hybrid Go/Python mode

New files for the hybrid architecture:
- `bridge.py` — `@task` decorator + `Flow.export()` → `flow.json`
- `worker.py` — subprocess shim; Go calls `python3 worker.py <entrypoint> <args_json>`
- `core/` — Go engine (types, DAG, executor, scheduler, CLI)
- `examples/hybrid_tasks.py` — example task functions
- `examples/hybrid_flow.py` — builds and exports `examples/flow.json`

**Build:**
```bash
cd core && go build -o ../microflow-core . && cd ..
```

**Run hybrid example end-to-end:**
```bash
python examples/hybrid_flow.py
./microflow-core run examples/flow.json --worker worker.py
```

**CLI flags:**
```
microflow-core run <flow.json> [--worker <path>] [--python <bin>]
```

## Branch
Development happens on `claude/microflow-agent-engine-LaNet`.
