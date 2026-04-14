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

## Running examples
```bash
python examples/hello_world.py
python examples/parallel_fan.py
python examples/hitl_review.py
python examples/retry_chaos.py
```

## Branch
Development happens on `claude/microflow-agent-engine-LaNet`.
