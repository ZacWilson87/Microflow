"""retry_chaos.py — Retry/backoff with injected failures.

Demonstrates: retry_limit, retry_delay_s, timeout_s, exponential backoff.

Three tasks show different failure modes:
  - flaky_api: fails twice, succeeds on third attempt
  - slow_task:  always times out (timeout_s=0.3)
  - stable:     always succeeds, runs in parallel with the others
"""

import sys
import os
import time
import random
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from microflow import create_workflow, add_task, run, register_sink
import json


# Custom sink to pretty-print only retry/fail/succeed events
def filtered_sink(event: dict) -> None:
    evt = event["event"]
    if evt in ("task_retrying", "task_failed", "task_succeeded",
               "workflow_completed", "workflow_failed"):
        payload_str = json.dumps(event["payload"])
        print(f"  [{evt}] task={event['task_id'][:8]}... {payload_str}")


_call_count = {"flaky": 0}


def flaky_api(context, results):
    _call_count["flaky"] += 1
    attempt = _call_count["flaky"]
    print(f"  [flaky_api] attempt #{attempt}")
    if attempt < 3:
        raise ConnectionError(f"Service unavailable (attempt {attempt})")
    return {"data": "success after retries", "attempts": attempt}


def slow_task(context, results):
    print("  [slow_task] Starting (will time out)...")
    time.sleep(5)  # much longer than timeout_s=0.3
    return "never reached"


def stable(context, results):
    print("  [stable] Running reliably...")
    time.sleep(0.05)
    return {"status": "always fine"}


def summarize(context, results):
    print("  [summarize] Building summary...")
    return {
        "flaky_result": results.get("flaky_api"),
        "stable_result": results.get("stable"),
        "slow_timed_out": results.get("slow_task") is None,
    }


if __name__ == "__main__":
    register_sink(filtered_sink)

    wf = create_workflow("retry-chaos")

    # flaky_api: will fail twice, then succeed on attempt 3
    add_task(wf, "flaky_api", flaky_api,
             retry_limit=3, retry_delay_s=0.05)

    # slow_task: always times out (no retries after timeout)
    add_task(wf, "slow_task", slow_task,
             retry_limit=0, timeout_s=0.3)

    # stable: always succeeds, no retries needed
    add_task(wf, "stable", stable, retry_limit=0)

    # summarize runs after flaky_api and stable succeed
    # (slow_task is independent; its failure won't block summarize)
    add_task(wf, "summarize", summarize,
             depends_on=["flaky_api", "stable"], retry_limit=0)

    print("Running retry_chaos workflow...")
    print("(flaky_api will retry twice; slow_task will time out)\n")

    results = run(wf, db_path="/tmp/retry_chaos.db")

    print("\nFinal statuses:")
    for task_id, r in results.items():
        name = wf.tasks[task_id].name
        print(f"  {name}: [{r.status}] attempts={r.attempt}  output={r.output!r}")
