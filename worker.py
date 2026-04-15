"""worker.py — Python subprocess shim invoked by the microflow-core Go engine.

The Go engine calls:
    python3 worker.py <entrypoint> <args_json>

where:
  <entrypoint>  dotted path to the task function, e.g. "examples.hybrid_tasks.fetch_source_data"
  <args_json>   JSON string: {"context": {...}, "results": {...}}

Exit codes
----------
  0  — task succeeded; result JSON written to **stdout**:  {"output": <value>}
  1  — task failed;    error  JSON written to **stderr**:  {"error": "<traceback>"}

The Go engine reads stdout to capture the task output and stderr to capture
error details.  Both channels are plain JSON so the engine can parse them
without shell-parsing heuristics.
"""

from __future__ import annotations

import importlib
import json
import sys
import traceback


def _resolve(entrypoint: str):
    """Import and return the callable at *entrypoint* (a dotted Python path).

    We try progressively shorter module prefixes so that both of these work:
        "examples.hybrid_tasks.fetch_source_data"   (package.module.fn)
        "mymodule.fn"                                (module.fn)
    """
    parts = entrypoint.rsplit(".", 1)
    if len(parts) != 2:
        raise ImportError(f"Invalid entrypoint {entrypoint!r}: expected 'module.function'")
    module_path, func_name = parts
    module = importlib.import_module(module_path)
    fn = getattr(module, func_name, None)
    if fn is None:
        raise AttributeError(f"Module {module_path!r} has no attribute {func_name!r}")
    return fn


def main() -> None:
    if len(sys.argv) < 3:
        print(
            json.dumps({"error": "Usage: worker.py <entrypoint> <args_json>"}),
            file=sys.stderr,
        )
        sys.exit(1)

    entrypoint = sys.argv[1]
    try:
        raw_args = json.loads(sys.argv[2])
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"Invalid args JSON: {exc}"}), file=sys.stderr)
        sys.exit(1)

    context = raw_args.get("context", {})
    results = raw_args.get("results", {})

    try:
        fn = _resolve(entrypoint)
    except (ImportError, AttributeError, ModuleNotFoundError) as exc:
        print(json.dumps({"error": f"Cannot resolve entrypoint: {exc}"}), file=sys.stderr)
        sys.exit(1)

    try:
        # Redirect stdout to stderr during task execution so that any print()
        # calls inside task functions don't corrupt the JSON result channel.
        _real_stdout = sys.stdout
        sys.stdout = sys.stderr
        try:
            output = fn(context, results)
        finally:
            sys.stdout = _real_stdout

        # Ensure the output is JSON-serialisable; fall back to repr() if not.
        try:
            json.dumps(output)
        except (TypeError, ValueError):
            output = repr(output)
        print(json.dumps({"output": output}))
    except Exception:
        tb = traceback.format_exc()
        print(json.dumps({"error": tb}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
