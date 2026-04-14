"""parallel_fan.py — Fan-out / fan-in pattern.

Demonstrates: parallel task execution, diamond DAG, result aggregation.

    fetch
   / | \
  A  B  C   (run concurrently)
   \ | /
  aggregate
"""

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from microflow import create_workflow, add_task, run


def fetch(context, results):
    print("  [fetch] Fetching data...")
    time.sleep(0.1)
    return {"items": [1, 2, 3, 4, 5, 6]}


def process_a(context, results):
    items = results["fetch"]["items"]
    time.sleep(0.15)
    result = sum(items)
    print(f"  [process_a] sum={result}")
    return result


def process_b(context, results):
    items = results["fetch"]["items"]
    time.sleep(0.1)
    result = max(items)
    print(f"  [process_b] max={result}")
    return result


def process_c(context, results):
    items = results["fetch"]["items"]
    time.sleep(0.05)
    result = len(items)
    print(f"  [process_c] count={result}")
    return result


def aggregate(context, results):
    summary = {
        "sum":   results["process_a"],
        "max":   results["process_b"],
        "count": results["process_c"],
        "avg":   results["process_a"] / results["process_c"],
    }
    print(f"  [aggregate] {summary}")
    return summary


if __name__ == "__main__":
    wf = create_workflow("parallel-fan")
    add_task(wf, "fetch",     fetch,     retry_limit=0)
    add_task(wf, "process_a", process_a, depends_on=["fetch"], retry_limit=0)
    add_task(wf, "process_b", process_b, depends_on=["fetch"], retry_limit=0)
    add_task(wf, "process_c", process_c, depends_on=["fetch"], retry_limit=0)
    add_task(wf, "aggregate", aggregate,
             depends_on=["process_a", "process_b", "process_c"], retry_limit=0)

    print("Running parallel_fan workflow (fan-out → fan-in)...")
    start = time.time()
    results = run(wf, db_path="/tmp/parallel_fan.db")
    elapsed = time.time() - start

    print(f"\nCompleted in {elapsed:.2f}s (parallel branches ran concurrently)")
    agg = next(r for r in results.values() if isinstance(r.output, dict) and "avg" in r.output)
    print(f"Aggregate result: {agg.output}")
