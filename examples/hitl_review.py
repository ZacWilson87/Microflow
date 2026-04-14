"""hitl_review.py — Human-in-the-loop pause/resume.

Demonstrates: @hitl_required, run_async, send_signal.

The 'review_output' task pauses and waits for a human signal.
This script simulates a reviewer approving after 1 second.
"""

import sys
import os
import time
import threading
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from microflow import (
    create_workflow, add_task, run_async, send_signal, get_status, hitl_required
)


def generate_report(context, results):
    print("  [generate_report] Generating AI analysis report...")
    time.sleep(0.2)
    return {"report": "AI recommends action X with 87% confidence", "risk": "medium"}


@hitl_required
def review_output(context, results):
    """This task will pause until a human approves or rejects."""
    report = results["generate_report"]
    print(f"  [review_output] Task executing with reviewer note: {context.get('reviewer_note', 'none')}")
    return {"approved_report": report, "reviewer": context.get("reviewer_note", "anonymous")}


def publish(context, results):
    approved = results["review_output"]
    print(f"  [publish] Publishing approved report by {approved['reviewer']}")
    return {"status": "published", "report": approved["approved_report"]}


def _simulate_reviewer(workflow_id: str, task_id: str, db_path: str) -> None:
    """Simulate a human reviewer who approves after checking the status."""
    print("\n  [reviewer thread] Waiting for task to reach 'waiting_hitl'...")
    deadline = time.time() + 10
    while time.time() < deadline:
        statuses = get_status(workflow_id, db_path=db_path)
        r = statuses.get(task_id)
        if r and r.status == "waiting_hitl":
            print("  [reviewer thread] Task is waiting — approving with annotation!")
            time.sleep(0.5)  # reviewer reads the pending task
            send_signal(
                workflow_id, task_id, "approve",
                payload={"reviewer_note": "Reviewed and approved by Alice"},
                db_path=db_path,
            )
            return
        time.sleep(0.1)
    print("  [reviewer thread] Timed out waiting for HITL task")


if __name__ == "__main__":
    db_path = "/tmp/hitl_review.db"

    wf = create_workflow("hitl-demo", context={})
    add_task(wf, "generate_report", generate_report, retry_limit=0)
    review_task = add_task(wf, "review_output", review_output,
                           depends_on=["generate_report"], retry_limit=0)
    add_task(wf, "publish", publish, depends_on=["review_output"], retry_limit=0)

    print("Running hitl_review workflow (will pause for human approval)...")

    # Start the workflow in the background
    thread = run_async(wf, db_path=db_path)

    # Simulate a human reviewer in a separate thread
    reviewer = threading.Thread(
        target=_simulate_reviewer,
        args=(wf.id, review_task.id, db_path),
        daemon=True,
    )
    reviewer.start()

    thread.join(timeout=15)
    reviewer.join(timeout=5)

    print("\nFinal results:")
    for task_id, r in get_status(wf.id, db_path=db_path).items():
        print(f"  {wf.tasks[task_id].name}: [{r.status}] output={r.output!r}")
