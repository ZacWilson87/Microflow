"""hello_world.py — Simplest possible microflow workflow.

Demonstrates: create_workflow, add_task, run.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from microflow import create_workflow, add_task, run


def say_hello(context, results):
    greeting = f"Hello, {context.get('name', 'World')}!"
    print(f"  → {greeting}")
    return greeting


def shout(context, results):
    loud = results["say_hello"].upper()
    print(f"  → {loud}")
    return loud


if __name__ == "__main__":
    wf = create_workflow("hello-world", context={"name": "microflow"})
    add_task(wf, "say_hello", say_hello, retry_limit=0)
    add_task(wf, "shout", shout, depends_on=["say_hello"], retry_limit=0)

    print("Running hello_world workflow...")
    results = run(wf, db_path="/tmp/hello_world.db")

    print("\nFinal results:")
    for task_id, r in results.items():
        print(f"  {wf.tasks[task_id].name}: [{r.status}] output={r.output!r}")
