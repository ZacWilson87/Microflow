package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

// Sink is a function that receives every observability event.
type Sink func(Event)

// Executor runs a single TaskSpec as a Python subprocess, handling retries and
// timeout.  It is designed to be called in its own goroutine.
type Executor struct {
	WorkflowID string
	WorkerPath string // absolute path to worker.py
	PythonBin  string // "python3" or full path
	Sink       Sink
}

// Execute runs task to completion (including retries) and returns a final
// TaskResult.  The call blocks until the task succeeds, exhausts retries, or
// is cancelled via ctx.
func (e *Executor) Execute(
	ctx context.Context,
	task TaskSpec,
	flowContext map[string]any,
	resultStore map[string]any, // name → output of succeeded tasks
) *TaskResult {
	result := &TaskResult{
		TaskID:    task.TaskID,
		Status:    StatusRunning,
		StartedAt: nowUnix(),
	}
	e.emit(task.TaskID, "task_started", map[string]any{"attempt": 0})

	attempt := 0
	for {
		output, err := e.runSubprocess(ctx, task, flowContext, resultStore, attempt)
		if err == nil {
			result.Status = StatusSuccess
			result.Output = output
			result.Attempt = attempt
			result.FinishedAt = nowUnix()
			e.emit(task.TaskID, "task_succeeded", map[string]any{
				"attempt": attempt,
				"output":  output,
			})
			return result
		}

		errMsg := err.Error()
		attempt++
		if attempt > task.RetryPolicy.Max {
			result.Status = StatusFailed
			result.Error = errMsg
			result.Attempt = attempt - 1
			result.FinishedAt = nowUnix()
			e.emit(task.TaskID, "task_failed", map[string]any{
				"attempt": attempt - 1,
				"error":   errMsg,
			})
			return result
		}

		delay := backoffDelay(task.RetryPolicy.Backoff, attempt)
		e.emit(task.TaskID, "task_retrying", map[string]any{
			"attempt":    attempt,
			"delay_s":    delay.Seconds(),
			"error":      errMsg,
		})

		select {
		case <-ctx.Done():
			result.Status = StatusCancelled
			result.FinishedAt = nowUnix()
			return result
		case <-time.After(delay):
		}
	}
}

// runSubprocess launches worker.py for a single attempt of task.
// Returns (output, nil) on success or (nil, error) on failure.
func (e *Executor) runSubprocess(
	ctx context.Context,
	task TaskSpec,
	flowContext map[string]any,
	resultStore map[string]any,
	attempt int,
) (any, error) {
	// Build the args payload passed to worker.py as JSON.
	argsPayload := map[string]any{
		"context": flowContext,
		"results": resultStore,
		"attempt": attempt,
	}
	argsJSON, err := json.Marshal(argsPayload)
	if err != nil {
		return nil, fmt.Errorf("marshal args: %w", err)
	}

	// Apply per-task timeout if configured.
	runCtx := ctx
	if task.TimeoutSec != nil {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(*task.TimeoutSec*float64(time.Second)))
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, e.PythonBin, e.WorkerPath, task.Entrypoint, string(argsJSON))

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// Prefer the structured error from stderr when available.
		if stderr.Len() > 0 {
			var errEnv map[string]string
			if jsonErr := json.Unmarshal(stderr.Bytes(), &errEnv); jsonErr == nil {
				if msg, ok := errEnv["error"]; ok {
					return nil, fmt.Errorf("%s", msg)
				}
			}
			return nil, fmt.Errorf("%s", stderr.String())
		}
		if runCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("task timed out after %.1fs", *task.TimeoutSec)
		}
		return nil, err
	}

	// Parse {"output": <value>} from stdout.
	var envelope map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		return nil, fmt.Errorf("parse worker output: %w (raw: %s)", err, stdout.String())
	}
	return envelope["output"], nil
}

// backoffDelay returns the sleep duration before the next retry attempt.
// Exponential: base * 2^attempt.  Linear: base * attempt.  Base = 1s.
func backoffDelay(strategy string, attempt int) time.Duration {
	base := time.Second
	if strategy == "linear" {
		return base * time.Duration(attempt)
	}
	// exponential (default)
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
	}
	return d
}

// emit sends an observability event through the configured sink.
func (e *Executor) emit(taskID, event string, payload map[string]any) {
	if e.Sink == nil {
		return
	}
	e.Sink(Event{
		TS:         nowUnix(),
		WorkflowID: e.WorkflowID,
		TaskID:     taskID,
		Event:      event,
		Payload:    payload,
	})
}
