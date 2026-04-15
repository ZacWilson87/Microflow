"""hybrid_flow.py — Build and export a flow.json for the Go engine.

Run this script first to generate the spec, then run the Go engine:

    python examples/hybrid_flow.py
    ./microflow-core run examples/flow.json --worker worker.py

The flow defines a three-task DAG:

    fetch_source_data ──┬──► process_data ──► generate_report
                        │                           ▲
                        └───────────────────────────┘

(generate_report depends on both fetch_source_data and process_data)
"""

from __future__ import annotations

import os
import sys

# Allow importing bridge.py and hybrid_tasks.py from the project root / examples/
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bridge import task, Flow

# ── Import actual task functions from hybrid_tasks ────────────────────────────
# We wrap them with @task here so the decorator captures the correct module path
# ("examples.hybrid_tasks") which worker.py uses for importlib.import_module().

import examples.hybrid_tasks as _ht

fetch_source_data = task(task_id="fetch_source_data", retries=2, timeout=15)(_ht.fetch_source_data)
process_data      = task(task_id="process_data",      retries=1, timeout=10)(_ht.process_data)
generate_report   = task(task_id="generate_report",   retries=0, timeout=10)(_ht.generate_report)


# ── Flow assembly ─────────────────────────────────────────────────────────────

def main():
    flow = Flow(
        name="hybrid-data-pipeline",
        context={"source": "analytics-db"},
    )

    flow.register(fetch_source_data)
    flow.register(process_data,    depends_on=["fetch_source_data"])
    flow.register(generate_report, depends_on=["fetch_source_data", "process_data"])

    out_dir = os.path.dirname(os.path.abspath(__file__))
    path = flow.export(os.path.join(out_dir, "flow.json"))

    print(f"\nTo run the workflow with the Go engine:")
    print(f"  cd {project_root}")
    print(f"  ./microflow-core run {path} --worker worker.py")
    print(f"\nOr from the core/ directory after building:")
    print(f"  go run . run {path} --worker ../worker.py")


if __name__ == "__main__":
    main()
