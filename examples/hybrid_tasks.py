"""hybrid_tasks.py — Task functions for the hybrid Go/Python workflow example.

These functions are called by the Go engine via worker.py.  Each function
receives:
  context  — the flow-level context dict from flow.json
  results  — a dict of {task_id: output} for all successfully completed tasks

Return values must be JSON-serialisable (dict, list, str, int, float, bool, None).
"""

from __future__ import annotations

import time


def fetch_source_data(context: dict, results: dict) -> dict:
    """Simulate fetching rows from an external data source."""
    source = context.get("source", "default-db")
    print(f"  [fetch_source_data] Querying {source}...")
    time.sleep(0.1)  # simulate I/O
    return {
        "source": source,
        "rows": [
            {"id": 1, "value": 42, "label": "alpha"},
            {"id": 2, "value": 17, "label": "beta"},
            {"id": 3, "value": 99, "label": "gamma"},
        ],
        "count": 3,
    }


def process_data(context: dict, results: dict) -> dict:
    """Transform the raw rows into summary statistics."""
    raw = results.get("fetch_source_data", {})
    rows = raw.get("rows", [])
    print(f"  [process_data] Processing {len(rows)} row(s)...")
    time.sleep(0.05)

    values = [r["value"] for r in rows]
    total = sum(values)
    avg = total / len(values) if values else 0.0
    return {
        "total":   total,
        "average": round(avg, 2),
        "max":     max(values, default=0),
        "min":     min(values, default=0),
        "count":   len(values),
    }


def generate_report(context: dict, results: dict) -> dict:
    """Combine fetch + processing results into a final report."""
    fetch_result   = results.get("fetch_source_data", {})
    process_result = results.get("process_data", {})
    print("  [generate_report] Composing report...")
    time.sleep(0.05)

    return {
        "title":      f"Data Report — {fetch_result.get('source', 'unknown')}",
        "row_count":  fetch_result.get("count", 0),
        "statistics": process_result,
        "status":     "ready",
    }
