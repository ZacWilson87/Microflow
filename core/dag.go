package main

import "fmt"

// topologicalSort returns task IDs in a valid execution order using Kahn's
// algorithm.  Returns an error if the graph contains a cycle.
func topologicalSort(tasks []TaskSpec) ([]string, error) {
	// Build a fast lookup: task_id → TaskSpec
	byID := make(map[string]TaskSpec, len(tasks))
	for _, t := range tasks {
		byID[t.TaskID] = t
	}

	// Validate that all declared dependencies exist.
	for _, t := range tasks {
		for _, dep := range t.Dependencies {
			if _, ok := byID[dep]; !ok {
				return nil, fmt.Errorf(
					"task %q depends on unknown task %q", t.TaskID, dep,
				)
			}
		}
	}

	// in-degree map and children adjacency list
	inDegree := make(map[string]int, len(tasks))
	children := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		if _, ok := inDegree[t.TaskID]; !ok {
			inDegree[t.TaskID] = 0
		}
		for _, dep := range t.Dependencies {
			inDegree[t.TaskID]++
			children[dep] = append(children[dep], t.TaskID)
		}
	}

	// Seed queue with tasks that have no dependencies.
	queue := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if inDegree[t.TaskID] == 0 {
			queue = append(queue, t.TaskID)
		}
	}

	order := make([]string, 0, len(tasks))
	for len(queue) > 0 {
		tid := queue[0]
		queue = queue[1:]
		order = append(order, tid)
		for _, child := range children[tid] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(order) != len(tasks) {
		return nil, fmt.Errorf("cyclic dependency detected in task graph")
	}
	return order, nil
}

// readyTasks returns task IDs that are ready to run: all dependencies have
// status "success" and the task itself has not yet been started.
func readyTasks(tasks []TaskSpec, results map[string]*TaskResult) []string {
	ready := make([]string, 0)
	for _, t := range tasks {
		if _, started := results[t.TaskID]; started {
			continue // already running or done
		}
		allDone := true
		for _, dep := range t.Dependencies {
			r, ok := results[dep]
			if !ok || r.Status != StatusSuccess {
				allDone = false
				break
			}
		}
		if allDone {
			ready = append(ready, t.TaskID)
		}
	}
	return ready
}

// allSuccess returns true when every task has status "success".
func allSuccess(tasks []TaskSpec, results map[string]*TaskResult) bool {
	if len(results) < len(tasks) {
		return false
	}
	for _, t := range tasks {
		r, ok := results[t.TaskID]
		if !ok || r.Status != StatusSuccess {
			return false
		}
	}
	return true
}

// anyFailed returns true when at least one task has status "failed".
func anyFailed(results map[string]*TaskResult) bool {
	for _, r := range results {
		if r.Status == StatusFailed {
			return true
		}
	}
	return false
}

// cancelDownstream performs a BFS from each failed task and marks all
// downstream tasks as "cancelled" (unless already terminal).
func cancelDownstream(
	tasks []TaskSpec,
	results map[string]*TaskResult,
	failedID string,
) {
	// Build children map
	children := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		for _, dep := range t.Dependencies {
			children[dep] = append(children[dep], t.TaskID)
		}
	}

	visited := map[string]bool{failedID: true}
	queue := []string{failedID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, child := range children[cur] {
			if visited[child] {
				continue
			}
			visited[child] = true
			if r, ok := results[child]; ok {
				// Already terminal — don't overwrite success/failed.
				if r.Status == StatusSuccess || r.Status == StatusFailed {
					continue
				}
			}
			if _, ok := results[child]; !ok {
				results[child] = &TaskResult{TaskID: child}
			}
			results[child].Status = StatusCancelled
			queue = append(queue, child)
		}
	}
}
