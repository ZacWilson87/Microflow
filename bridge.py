"""bridge.py — Python DSL for defining workflows that run on the microflow-core Go engine.

Usage
-----
1. Decorate task functions with @task.
2. Create a Flow, register tasks (optionally with depends_on), and call flow.export().
3. Run the exported flow.json with the Go engine:
       ./microflow-core run flow.json

Example
-------
    from bridge import task, Flow

    @task(retries=2, timeout=30)
    def fetch_data(context, results):
        return {"rows": 42}

    @task()
    def process(context, results):
        return results["fetch_data"]["rows"] * 2

    flow = Flow("my-pipeline")
    flow.register(fetch_data)
    flow.register(process, depends_on=["fetch_data"])
    flow.export("flow.json")
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable

# Attribute name stamped on decorated functions (mirrors _HITL_ATTR in microflow.py)
_TASK_ATTR = "_microflow_bridge_spec"


# ── Bridge task spec ──────────────────────────────────────────────────────────

@dataclass
class _TaskSpec:
    """Internal task metadata attached to decorated functions."""
    task_id: str
    entrypoint: str          # "module.submodule.func_name"
    runtime: str = "python3"
    retry_policy: dict = field(default_factory=lambda: {"max": 3, "backoff": "exponential"})
    dependencies: list[str] = field(default_factory=list)
    timeout_sec: int | None = None

    def to_dict(self) -> dict:
        d = {
            "task_id":      self.task_id,
            "entrypoint":   self.entrypoint,
            "runtime":      self.runtime,
            "retry_policy": self.retry_policy,
            "dependencies": self.dependencies,
        }
        if self.timeout_sec is not None:
            d["timeout_sec"] = self.timeout_sec
        return d


# ── @task decorator ───────────────────────────────────────────────────────────

def task(
    task_id: str | None = None,
    retries: int = 3,
    timeout: int | None = None,
    backoff: str = "exponential",
) -> Callable:
    """Decorator that marks a function as a microflow bridge task.

    Parameters
    ----------
    task_id:
        Unique identifier for this task. Defaults to ``fn.__name__``.
    retries:
        Maximum number of retry attempts after the first failure.
    timeout:
        Per-attempt timeout in seconds. ``None`` means no timeout.
    backoff:
        Retry back-off strategy: ``"exponential"`` or ``"linear"``.
    """
    def decorator(fn: Callable) -> Callable:
        tid = task_id or fn.__name__
        module = fn.__module__
        entrypoint = f"{module}.{fn.__name__}"

        spec = _TaskSpec(
            task_id=tid,
            entrypoint=entrypoint,
            retry_policy={"max": retries, "backoff": backoff},
            timeout_sec=timeout,
        )

        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        setattr(wrapper, _TASK_ATTR, spec)
        return wrapper

    return decorator


# ── Flow builder ──────────────────────────────────────────────────────────────

class Flow:
    """A workflow definition that can be serialised to a JSON spec for the Go engine.

    Parameters
    ----------
    name:
        Human-readable workflow name (used for logging and display).
    context:
        Arbitrary key/value dict passed to every task at runtime.
    """

    def __init__(self, name: str, context: dict | None = None) -> None:
        self.name = name
        self.context: dict = context or {}
        self._specs: list[_TaskSpec] = []

    def register(
        self,
        fn: Callable,
        depends_on: list[str] | None = None,
    ) -> "Flow":
        """Register a @task-decorated function with optional dependency overrides.

        Parameters
        ----------
        fn:
            A function decorated with ``@task``.
        depends_on:
            List of ``task_id`` strings this task must wait for. If ``None``,
            uses the list provided at decoration time (default: ``[]``).

        Returns
        -------
        self — allows method chaining.
        """
        spec = getattr(fn, _TASK_ATTR, None)
        if spec is None:
            raise ValueError(
                f"{fn.__name__!r} is not decorated with @task. "
                "Apply @task before registering."
            )
        spec = copy.copy(spec)          # don't mutate the original
        if depends_on is not None:
            spec.dependencies = list(depends_on)
        self._specs.append(spec)
        return self

    def export(self, path: str = "flow.json") -> str:
        """Serialise the workflow to a JSON file readable by the Go engine.

        Parameters
        ----------
        path:
            Destination file path.

        Returns
        -------
        The resolved path that was written.

        Raises
        ------
        ValueError:
            If no tasks have been registered.
        """
        if not self._specs:
            raise ValueError("Flow has no tasks. Register at least one task before exporting.")

        data = {
            "name":    self.name,
            "context": self.context,
            "tasks":   [s.to_dict() for s in self._specs],
        }
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2)
        print(f"[bridge] Exported {len(self._specs)} task(s) → {path}")
        return path

    # ── Inspection helpers ────────────────────────────────────────────────────

    def task_ids(self) -> list[str]:
        """Return the ordered list of registered task IDs."""
        return [s.task_id for s in self._specs]

    def __repr__(self) -> str:
        return f"Flow(name={self.name!r}, tasks={self.task_ids()!r})"
