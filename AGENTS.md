# microflow — Agent Context

## What this project is
microflow is an AI agent workflow engine implemented twice — once in Python,
once in Go — with identical philosophy: single file, ≤ 500 lines, zero
external dependencies, every seam visible.

- `microflow.py` — Python engine (SQLite persistence)
- `microflow.go` — Go engine (JSON file persistence)
- `server.py` — optional FastAPI server wrapping the Python engine

## Key invariants
- `microflow.py` must remain ≤ 500 lines, stdlib-only imports
- `microflow.go` must remain ≤ 550 lines, no external module dependencies
- Both files expose exactly 8 public functions (same surface, different language idioms)
- No hybrid/subprocess architecture — each engine runs its own language natively

## Running Python tests
```bash
pytest tests/
```

## Running Go tests
```bash
go test ./...
```

## Running Python examples
```bash
python examples/hello_world.py
python examples/parallel_fan.py
python examples/hitl_review.py
python examples/retry_chaos.py
```

## Running Go examples
```bash
go run ./examples/go/hello_world
go run ./examples/go/parallel_fan
go run ./examples/go/hitl_review
go run ./examples/go/retry_chaos
```

## Branch
Development happens on `claude/update-docs-go-xaRpF`.
