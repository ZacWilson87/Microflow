# microflow.py — The Irreducible AI Agent Workflow Engine
# Zero stdlib-external dependencies. Python 3.11+.
# Read top-to-bottom: every design decision is visible, nothing is magic.

# ── 1. TYPES & CONSTANTS ─────────────────────────────────────────
# ── 2. PERSISTENCE (SQLite) ──────────────────────────────────────
# ── 3. DAG RESOLVER ──────────────────────────────────────────────
# ── 4. EXECUTOR ──────────────────────────────────────────────────
# ── 5. RETRY ENGINE ──────────────────────────────────────────────
# ── 6. HITL GATE ─────────────────────────────────────────────────
# ── 7. OBSERVABILITY EMITTER ─────────────────────────────────────
# ── 8. WORKFLOW RUNNER ───────────────────────────────────────────
# ── 9. PUBLIC API ────────────────────────────────────────────────
