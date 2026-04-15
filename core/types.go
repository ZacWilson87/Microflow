package main

import "time"

// ── Task status constants ─────────────────────────────────────────────────────

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSuccess   = "success"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// ── Bridge schema (mirrors flow.json produced by bridge.py) ──────────────────

// RetryPolicy controls retry behaviour for a single task.
type RetryPolicy struct {
	Max     int    `json:"max"`     // maximum retry attempts after first failure
	Backoff string `json:"backoff"` // "exponential" | "linear"
}

// TaskSpec is one task entry in flow.json.
type TaskSpec struct {
	TaskID       string      `json:"task_id"`
	Entrypoint   string      `json:"entrypoint"`    // dotted Python path
	Runtime      string      `json:"runtime"`       // "python3" (extendable)
	RetryPolicy  RetryPolicy `json:"retry_policy"`
	Dependencies []string    `json:"dependencies"`  // task_ids this task waits for
	TimeoutSec   *float64    `json:"timeout_sec"`   // nil ⇒ no timeout
}

// FlowSpec is the top-level object in flow.json.
type FlowSpec struct {
	Name    string            `json:"name"`
	Context map[string]any    `json:"context"`
	Tasks   []TaskSpec        `json:"tasks"`
}

// ── Runtime result ────────────────────────────────────────────────────────────

// TaskResult holds the live state of a task during / after execution.
type TaskResult struct {
	TaskID     string
	Status     string
	Output     any
	Error      string
	Attempt    int
	StartedAt  float64
	FinishedAt float64
}

// ── Observability event ───────────────────────────────────────────────────────

// Event is the JSON-line emitted for every state transition.
type Event struct {
	TS         float64        `json:"ts"`
	WorkflowID string         `json:"workflow_id"`
	TaskID     string         `json:"task_id"`
	Event      string         `json:"event"`
	Payload    map[string]any `json:"payload"`
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func nowUnix() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func floatPtr(v float64) *float64 { return &v }
