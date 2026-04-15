package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const tickInterval = 100 * time.Millisecond

// resultMsg is the message sent from a goroutine back to the scheduler when a
// task finishes.
type resultMsg struct {
	taskID string
	result *TaskResult
}

// Scheduler drives a workflow to completion by submitting ready tasks as
// goroutines and collecting their results in a central tick loop.
type Scheduler struct {
	flow     FlowSpec
	executor *Executor
	// taskByID provides O(1) lookup of TaskSpec by task_id.
	taskByID map[string]TaskSpec
}

// NewScheduler validates the DAG and returns a ready-to-run Scheduler.
// Returns an error if the graph contains cycles or unknown dependencies.
func NewScheduler(flow FlowSpec, exec *Executor) (*Scheduler, error) {
	if _, err := topologicalSort(flow.Tasks); err != nil {
		return nil, fmt.Errorf("invalid DAG: %w", err)
	}
	byID := make(map[string]TaskSpec, len(flow.Tasks))
	for _, t := range flow.Tasks {
		byID[t.TaskID] = t
	}
	return &Scheduler{flow: flow, executor: exec, taskByID: byID}, nil
}

// Run executes the workflow and returns the final results map (task_id →
// *TaskResult).  It blocks until the workflow completes or ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) (map[string]*TaskResult, error) {
	results := make(map[string]*TaskResult, len(s.flow.Tasks))
	resultCh := make(chan resultMsg, len(s.flow.Tasks))
	inFlight := 0
	var mu sync.Mutex // guards results and inFlight (read by goroutines)

	s.executor.emit("", "workflow_started", map[string]any{
		"name":       s.flow.Name,
		"task_count": len(s.flow.Tasks),
	})

	for {
		// ── Phase 1: drain completed tasks ───────────────────────────────────
		drained := true
		for drained {
			select {
			case msg := <-resultCh:
				mu.Lock()
				results[msg.taskID] = msg.result
				inFlight--
				if msg.result.Status == StatusFailed {
					cancelDownstream(s.flow.Tasks, results, msg.taskID)
				}
				mu.Unlock()
			default:
				drained = false
			}
		}

		// ── Phase 2: submit newly-ready tasks ─────────────────────────────────
		mu.Lock()
		ready := readyTasks(s.flow.Tasks, results)
		for _, tid := range ready {
			task := s.taskByID[tid]
			// Stamp a running placeholder to prevent double-submission.
			results[tid] = &TaskResult{
				TaskID:    tid,
				Status:    StatusRunning,
				StartedAt: nowUnix(),
			}
			inFlight++
			// Capture loop variable for goroutine closure.
			capturedTask := task
			capturedResultStore := buildResultStore(results)
			go func() {
				r := s.executor.Execute(ctx, capturedTask, s.flow.Context, capturedResultStore)
				resultCh <- resultMsg{taskID: capturedTask.TaskID, result: r}
			}()
		}
		currentInFlight := inFlight
		mu.Unlock()

		// ── Phase 3: check terminal conditions ───────────────────────────────
		mu.Lock()
		done := allSuccess(s.flow.Tasks, results)
		failed := anyFailed(results) && currentInFlight == 0
		mu.Unlock()

		if done {
			s.executor.emit("", "workflow_completed", map[string]any{
				"task_count": len(s.flow.Tasks),
			})
			return results, nil
		}
		if failed {
			ids := failedTaskIDs(results)
			s.executor.emit("", "workflow_failed", map[string]any{
				"failed_tasks": ids,
			})
			return results, fmt.Errorf("workflow failed: tasks %v did not succeed", ids)
		}

		// Check for context cancellation.
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		time.Sleep(tickInterval)
	}
}

// buildResultStore returns a name → output map of all successfully completed
// tasks.  Passed to each Python subprocess so tasks can read prior outputs.
func buildResultStore(results map[string]*TaskResult) map[string]any {
	store := make(map[string]any, len(results))
	for tid, r := range results {
		if r.Status == StatusSuccess {
			store[tid] = r.Output
		}
	}
	return store
}

// failedTaskIDs returns a slice of task IDs with status "failed".
func failedTaskIDs(results map[string]*TaskResult) []string {
	ids := make([]string, 0)
	for tid, r := range results {
		if r.Status == StatusFailed {
			ids = append(ids, tid)
		}
	}
	return ids
}
