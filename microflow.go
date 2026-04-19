// microflow.go — The Irreducible AI Agent Workflow Engine (Go edition)
// Zero external dependencies. Go 1.21+.
// Read top-to-bottom: every design decision is visible, nothing is magic.

package microflow

// ── 1. TYPES & CONSTANTS ─────────────────────────────────────────

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const tickInterval = 100 * time.Millisecond

const (
	StatusPending     = "pending"
	StatusRunning     = "running"
	StatusSuccess     = "success"
	StatusFailed      = "failed"
	StatusWaitingHITL = "waiting_hitl"
	StatusCancelled   = "cancelled"
	StatusRetrying    = "retrying"
)

type TaskFunc func(ctx map[string]any, results map[string]any) (any, error)
type Option func(*Task)
type Sink func(Event)

type Task struct {
	ID           string
	Name         string
	Fn           TaskFunc
	DependsOn    []string
	Retries      int
	TimeoutSec   float64
	HITLRequired bool
}

type TaskResult struct {
	TaskID    string  `json:"task_id"`
	Status    string  `json:"status"`
	Output    any     `json:"output,omitempty"`
	Error     string  `json:"error,omitempty"`
	Attempts  int     `json:"attempts"`
	StartedAt float64 `json:"started_at,omitempty"`
	DoneAt    float64 `json:"done_at,omitempty"`
}

type HITLSignal struct {
	Decision string         // "approve" | "reject" | "override"
	TaskName string
	Payload  map[string]any
	Reason   string
}

type Event struct {
	TS         float64        `json:"ts"`
	WorkflowID string         `json:"workflow_id"`
	TaskID     string         `json:"task_id,omitempty"`
	EventType  string         `json:"event"`
	Payload    map[string]any `json:"payload,omitempty"`
}

// CyclicDependencyError is returned when the task graph contains a cycle.
type CyclicDependencyError struct{ msg string }

func (e CyclicDependencyError) Error() string { return e.msg }

type Workflow struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Context map[string]any    `json:"context"`
	Tasks   map[string]*Task  `json:"-"`
	mu      sync.Mutex
	results map[string]*TaskResult
	sink    Sink
	hitlCh  chan HITLSignal
	dbPath  string
}

// fnResult carries a task function's return values over a channel.
type fnResult struct{ out any; err error }

// ── 2. PERSISTENCE (JSON FILE) ───────────────────────────────────
//
// Every state transition writes the full workflow snapshot to a JSON file.
// Zero dependencies, plain-text: `cat workflow-<id>.json` shows the exact
// state machine at any moment.  On crash, Replay() loads this file and
// skips already-completed tasks.

type snapshot struct {
	ID      string                 `json:"id"`
	Name    string                 `json:"name"`
	Context map[string]any         `json:"context"`
	Results map[string]*TaskResult `json:"results"`
}

func (wf *Workflow) save() {
	if wf.dbPath == "" {
		return
	}
	data, err := json.MarshalIndent(snapshot{wf.ID, wf.Name, wf.Context, wf.results}, "", "  ")
	if err == nil {
		_ = os.WriteFile(wf.dbPath, data, 0644)
	}
}

func (wf *Workflow) load() {
	if wf.dbPath == "" {
		return
	}
	data, err := os.ReadFile(wf.dbPath)
	if err != nil {
		return // no prior snapshot — fresh run
	}
	var snap snapshot
	if json.Unmarshal(data, &snap) == nil {
		wf.Context = snap.Context
		wf.results = snap.Results
	}
}

// ── 3. DAG RESOLVER ──────────────────────────────────────────────
//
// Kahn's algorithm: count in-degrees, process nodes with zero in-degree,
// decrement neighbours.  If we can't order all nodes, there's a cycle.

func topologicalSort(tasks map[string]*Task) ([]string, error) {
	inDegree := make(map[string]int, len(tasks))
	for name := range tasks {
		inDegree[name] = 0
	}
	for _, t := range tasks {
		for _, dep := range t.DependsOn {
			if _, ok := tasks[dep]; !ok {
				return nil, fmt.Errorf("task %q depends on unknown task %q", t.Name, dep)
			}
			inDegree[t.Name]++
		}
	}
	queue := []string{}
	for name, d := range inDegree {
		if d == 0 {
			queue = append(queue, name)
		}
	}
	order := make([]string, 0, len(tasks))
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		for _, t := range tasks {
			for _, dep := range t.DependsOn {
				if dep == n {
					inDegree[t.Name]--
					if inDegree[t.Name] == 0 {
						queue = append(queue, t.Name)
					}
				}
			}
		}
	}
	if len(order) != len(tasks) {
		return nil, CyclicDependencyError{"cyclic dependency detected in task graph"}
	}
	return order, nil
}

func readyTasks(tasks map[string]*Task, results map[string]*TaskResult) []string {
	ready := []string{}
	for name, t := range tasks {
		if r, ok := results[name]; ok && r.Status != StatusRetrying {
			continue
		}
		allDone := true
		for _, dep := range t.DependsOn {
			r, ok := results[dep]
			if !ok || r.Status != StatusSuccess {
				allDone = false
				break
			}
		}
		if allDone {
			ready = append(ready, name)
		}
	}
	return ready
}

// cancelDownstream BFS-marks all transitive dependents of failedName as cancelled.
func cancelDownstream(failed string, tasks map[string]*Task, results map[string]*TaskResult) {
	queue := []string{failed}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for name, t := range tasks {
			if _, done := results[name]; done {
				continue
			}
			for _, dep := range t.DependsOn {
				if dep == cur {
					results[name] = &TaskResult{TaskID: name, Status: StatusCancelled}
					queue = append(queue, name)
					break
				}
			}
		}
	}
}

func allSuccess(tasks map[string]*Task, results map[string]*TaskResult) bool {
	for name := range tasks {
		r, ok := results[name]
		if !ok || r.Status != StatusSuccess {
			return false
		}
	}
	return true
}

func anyFailed(results map[string]*TaskResult) bool {
	for _, r := range results {
		if r.Status == StatusFailed {
			return true
		}
	}
	return false
}

// ── 4. RETRY ENGINE ──────────────────────────────────────────────
//
// Each task runs inside a goroutine.  Failures trigger exponential backoff
// and a retry up to t.Retries times.  Timeout is enforced via
// context.WithTimeout so the goroutine cannot outlive its budget.

func backoff(attempt int) time.Duration {
	secs := math.Min(math.Pow(2, float64(attempt)), 30)
	return time.Duration(secs * float64(time.Second))
}

type taskMsg struct {
	name   string
	result *TaskResult
}

func runTask(ctx context.Context, wf *Workflow, t *Task, resultCh chan<- taskMsg) {
	tr := &TaskResult{TaskID: t.Name, Status: StatusRunning, StartedAt: nowUnix()}
	wf.mu.Lock(); wf.results[t.Name] = tr; wf.save(); wf.mu.Unlock()
	emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, TaskID: t.Name, EventType: "task_started"})

	// HITL gate: pause until a reviewer sends approve/reject/override.
	if t.HITLRequired {
		tr.Status = StatusWaitingHITL
		wf.mu.Lock(); wf.save(); wf.mu.Unlock()
		emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, TaskID: t.Name, EventType: "task_waiting_hitl"})
		sig := <-wf.hitlCh
		if sig.Decision == "reject" {
			tr.Status, tr.Error, tr.DoneAt = StatusFailed, "rejected: "+sig.Reason, nowUnix()
			wf.mu.Lock(); wf.save(); wf.mu.Unlock()
			emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, TaskID: t.Name, EventType: "task_rejected"})
			resultCh <- taskMsg{t.Name, tr}
			return
		}
		if len(sig.Payload) > 0 {
			wf.mu.Lock()
			for k, v := range sig.Payload { wf.Context[k] = v }
			wf.mu.Unlock()
		}
		tr.Status = StatusRunning
		emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, TaskID: t.Name, EventType: "task_approved"})
	}

	// Retry loop: attempt up to Retries+1 times with exponential backoff.
	for attempt := 0; attempt < t.Retries+1; attempt++ {
		if attempt > 0 {
			tr.Status = StatusRetrying
			wf.mu.Lock(); wf.save(); wf.mu.Unlock()
			emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, TaskID: t.Name, EventType: "task_retrying",
				Payload: map[string]any{"attempt": attempt}})
			select {
			case <-ctx.Done():
				tr.Status, tr.Error, tr.DoneAt = StatusFailed, ctx.Err().Error(), nowUnix()
				resultCh <- taskMsg{t.Name, tr}
				return
			case <-time.After(backoff(attempt)):
			}
		}
		tr.Attempts = attempt + 1

		runCtx := ctx
		var cancel context.CancelFunc
		if t.TimeoutSec > 0 {
			runCtx, cancel = context.WithTimeout(ctx, time.Duration(t.TimeoutSec*float64(time.Second)))
		}
		wf.mu.Lock()
		ctxCopy := copyMap(wf.Context)
		store := buildResultStore(wf.results)
		wf.mu.Unlock()

		done := make(chan fnResult, 1)
		go func() { out, err := t.Fn(ctxCopy, store); done <- fnResult{out, err} }()

		var res fnResult
		select {
		case res = <-done:
		case <-runCtx.Done():
			res.err = fmt.Errorf("timeout after %.1fs", t.TimeoutSec)
		}
		if cancel != nil { cancel() }

		if res.err == nil {
			tr.Status, tr.Output, tr.DoneAt = StatusSuccess, res.out, nowUnix()
			wf.mu.Lock(); wf.save(); wf.mu.Unlock()
			emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, TaskID: t.Name, EventType: "task_succeeded",
				Payload: map[string]any{"output": res.out}})
			resultCh <- taskMsg{t.Name, tr}
			return
		}
		tr.Error = res.err.Error()
	}

	tr.Status, tr.DoneAt = StatusFailed, nowUnix()
	wf.mu.Lock(); wf.save(); wf.mu.Unlock()
	emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, TaskID: t.Name, EventType: "task_failed",
		Payload: map[string]any{"error": tr.Error}})
	resultCh <- taskMsg{t.Name, tr}
}

func buildResultStore(results map[string]*TaskResult) map[string]any {
	store := make(map[string]any, len(results))
	for name, r := range results {
		if r.Status == StatusSuccess {
			store[name] = r.Output
		}
	}
	return store
}

func copyMap(m map[string]any) map[string]any {
	c := make(map[string]any, len(m))
	for k, v := range m { c[k] = v }
	return c
}

// ── 5. SCHEDULER (GOROUTINE TICK LOOP) ───────────────────────────
//
// The tick loop wakes every 100ms and:
//   1. Drains the result channel (non-blocking)
//   2. Submits newly-ready tasks as goroutines
//   3. Checks terminal conditions
//
// Submitting BEFORE the terminal check is critical: the last task may have
// just finished and its successor is now ready — checking terminal first
// would produce a false "done" with work left on the table.

func runWorkflow(wf *Workflow) (map[string]any, error) {
	if _, err := topologicalSort(wf.Tasks); err != nil {
		return nil, err
	}
	resultCh := make(chan taskMsg, len(wf.Tasks))
	inFlight := map[string]bool{}
	ctx := context.Background()
	emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, EventType: "workflow_started"})

	for {
		// 1. Drain results (non-blocking).
		for drained := true; drained; {
			select {
			case msg := <-resultCh:
				wf.mu.Lock()
				wf.results[msg.name] = msg.result
				delete(inFlight, msg.name)
				if msg.result.Status == StatusFailed {
					cancelDownstream(msg.name, wf.Tasks, wf.results)
				}
				wf.save()
				wf.mu.Unlock()
			default:
				drained = false
			}
		}
		// 2. Submit ready tasks.
		wf.mu.Lock()
		for _, name := range readyTasks(wf.Tasks, wf.results) {
			if !inFlight[name] {
				inFlight[name] = true
				go runTask(ctx, wf, wf.Tasks[name], resultCh)
			}
		}
		done := allSuccess(wf.Tasks, wf.results)
		failed := anyFailed(wf.results) && len(inFlight) == 0
		wf.mu.Unlock()

		// 3. Terminal check.
		if done {
			emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, EventType: "workflow_succeeded"})
			return buildResultStore(wf.results), nil
		}
		if failed {
			emit(wf, Event{TS: nowUnix(), WorkflowID: wf.ID, EventType: "workflow_failed"})
			return nil, fmt.Errorf("one or more tasks failed")
		}
		time.Sleep(tickInterval)
	}
}

// ── 6. OBSERVABILITY ─────────────────────────────────────────────

func emit(wf *Workflow, e Event) {
	if wf.sink != nil {
		wf.sink(e)
	}
}

// DefaultSink writes JSON-line events to stdout — swap it for your own
// to ship events to a log aggregator, database, or UI.
func DefaultSink(e Event) {
	data, _ := json.Marshal(e)
	fmt.Println(string(data))
}

func nowUnix() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// ── 7. PUBLIC API ─────────────────────────────────────────────────
//
// Eight functions — the complete public surface of the engine.

func CreateWorkflow(name string, ctx map[string]any) *Workflow {
	if ctx == nil {
		ctx = map[string]any{}
	}
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	return &Workflow{
		ID:      id,
		Name:    name,
		Context: ctx,
		Tasks:   map[string]*Task{},
		results: map[string]*TaskResult{},
		hitlCh:  make(chan HITLSignal, 1),
		dbPath:  filepath.Join(os.TempDir(), "workflow-"+id+".json"),
	}
}

func AddTask(wf *Workflow, name string, fn TaskFunc, opts ...Option) *Task {
	t := &Task{ID: name, Name: name, Fn: fn}
	for _, o := range opts { o(t) }
	wf.Tasks[name] = t
	return t
}

func Run(wf *Workflow) (map[string]any, error) {
	wf.load()
	return runWorkflow(wf)
}

func RunAsync(wf *Workflow) (<-chan map[string]any, <-chan error) {
	outCh, errCh := make(chan map[string]any, 1), make(chan error, 1)
	go func() {
		res, err := Run(wf)
		if err != nil { errCh <- err } else { outCh <- res }
	}()
	return outCh, errCh
}

func SendSignal(wf *Workflow, taskName string, sig HITLSignal) error {
	sig.TaskName = taskName
	select {
	case wf.hitlCh <- sig:
		return nil
	default:
		return fmt.Errorf("signal channel full — another signal is pending")
	}
}

func GetStatus(wf *Workflow) map[string]string {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	out := make(map[string]string, len(wf.Tasks))
	for name := range wf.Tasks {
		if r, ok := wf.results[name]; ok {
			out[name] = r.Status
		} else {
			out[name] = StatusPending
		}
	}
	return out
}

func Replay(wf *Workflow) (map[string]any, error) {
	wf.load()
	return runWorkflow(wf)
}

func RegisterSink(wf *Workflow, sink Sink) { wf.sink = sink }

// ── Option helpers ────────────────────────────────────────────────

func WithRetries(n int) Option        { return func(t *Task) { t.Retries = n } }
func WithTimeout(secs float64) Option { return func(t *Task) { t.TimeoutSec = secs } }
func WithHITL() Option                { return func(t *Task) { t.HITLRequired = true } }
func WithDependsOn(names ...string) Option {
	return func(t *Task) { t.DependsOn = append(t.DependsOn, names...) }
}
