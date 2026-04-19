package microflow_test

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	mf "github.com/zacwilson87/microflow"
)

// ── helpers ───────────────────────────────────────────────────────

func taskOK(out any) mf.TaskFunc {
	return func(_ map[string]any, _ map[string]any) (any, error) { return out, nil }
}

func taskFail(msg string) mf.TaskFunc {
	return func(_ map[string]any, _ map[string]any) (any, error) { return nil, errors.New(msg) }
}

// ── basic execution ───────────────────────────────────────────────

func TestLinearChain(t *testing.T) {
	wf := mf.CreateWorkflow("linear", nil)
	mf.AddTask(wf, "a", taskOK(1))
	mf.AddTask(wf, "b", func(_ map[string]any, res map[string]any) (any, error) {
		return res["a"].(int) + 1, nil
	}, mf.WithDependsOn("a"))

	results, err := mf.Run(wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results["b"] != 2 {
		t.Fatalf("expected b=2, got %v", results["b"])
	}
}

func TestParallelFan(t *testing.T) {
	wf := mf.CreateWorkflow("fan", nil)
	mf.AddTask(wf, "x", taskOK("x"))
	mf.AddTask(wf, "y", taskOK("y"))
	mf.AddTask(wf, "z", taskOK("z"))
	mf.AddTask(wf, "merge", func(_ map[string]any, res map[string]any) (any, error) {
		return fmt.Sprintf("%v%v%v", res["x"], res["y"], res["z"]), nil
	}, mf.WithDependsOn("x", "y", "z"))

	results, err := mf.Run(wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results["merge"] == nil {
		t.Fatal("merge result missing")
	}
}

func TestContextPassthrough(t *testing.T) {
	wf := mf.CreateWorkflow("ctx", map[string]any{"env": "test"})
	mf.AddTask(wf, "read", func(ctx map[string]any, _ map[string]any) (any, error) {
		return ctx["env"], nil
	})
	results, err := mf.Run(wf)
	if err != nil {
		t.Fatal(err)
	}
	if results["read"] != "test" {
		t.Fatalf("expected 'test', got %v", results["read"])
	}
}

// ── failure & cancellation ────────────────────────────────────────

func TestTaskFailure(t *testing.T) {
	wf := mf.CreateWorkflow("fail", nil)
	mf.AddTask(wf, "boom", taskFail("boom"))
	_, err := mf.Run(wf)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestDownstreamCancellation(t *testing.T) {
	wf := mf.CreateWorkflow("cancel", nil)
	mf.AddTask(wf, "root", taskFail("root fails"))
	mf.AddTask(wf, "child", taskOK("never runs"), mf.WithDependsOn("root"))

	mf.Run(wf) //nolint
	status := mf.GetStatus(wf)
	if status["child"] != mf.StatusCancelled {
		t.Fatalf("expected child=cancelled, got %q", status["child"])
	}
}

func TestCyclicDependency(t *testing.T) {
	wf := mf.CreateWorkflow("cycle", nil)
	mf.AddTask(wf, "a", taskOK(1), mf.WithDependsOn("b"))
	mf.AddTask(wf, "b", taskOK(2), mf.WithDependsOn("a"))
	_, err := mf.Run(wf)
	if err == nil {
		t.Fatal("expected cyclic dependency error")
	}
	var cycErr mf.CyclicDependencyError
	if !errors.As(err, &cycErr) {
		t.Fatalf("expected CyclicDependencyError, got %T: %v", err, err)
	}
}

// ── retry ─────────────────────────────────────────────────────────

func TestRetrySuccess(t *testing.T) {
	var calls atomic.Int32
	wf := mf.CreateWorkflow("retry", nil)
	mf.AddTask(wf, "flaky", func(_ map[string]any, _ map[string]any) (any, error) {
		n := calls.Add(1)
		if n < 3 {
			return nil, errors.New("not yet")
		}
		return "ok", nil
	}, mf.WithRetries(3))

	results, err := mf.Run(wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results["flaky"] != "ok" {
		t.Fatalf("expected ok, got %v", results["flaky"])
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 calls, got %d", calls.Load())
	}
}

func TestRetryExhausted(t *testing.T) {
	wf := mf.CreateWorkflow("exhausted", nil)
	mf.AddTask(wf, "always_fail", taskFail("always"), mf.WithRetries(2))
	_, err := mf.Run(wf)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── timeout ───────────────────────────────────────────────────────

func TestTimeout(t *testing.T) {
	wf := mf.CreateWorkflow("timeout", nil)
	mf.AddTask(wf, "slow", func(_ map[string]any, _ map[string]any) (any, error) {
		time.Sleep(2 * time.Second)
		return "done", nil
	}, mf.WithTimeout(0.1))

	_, err := mf.Run(wf)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// ── HITL ──────────────────────────────────────────────────────────

func TestHITLApprove(t *testing.T) {
	wf := mf.CreateWorkflow("hitl", nil)
	mf.AddTask(wf, "review", taskOK("reviewed"), mf.WithHITL())

	go func() {
		time.Sleep(50 * time.Millisecond)
		mf.SendSignal(wf, "review", mf.HITLSignal{Decision: "approve"})
	}()

	results, err := mf.Run(wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results["review"] != "reviewed" {
		t.Fatalf("expected 'reviewed', got %v", results["review"])
	}
}

func TestHITLReject(t *testing.T) {
	wf := mf.CreateWorkflow("hitl-reject", nil)
	mf.AddTask(wf, "review", taskOK("should not run"), mf.WithHITL())

	go func() {
		time.Sleep(50 * time.Millisecond)
		mf.SendSignal(wf, "review", mf.HITLSignal{Decision: "reject", Reason: "bad output"})
	}()

	_, err := mf.Run(wf)
	if err == nil {
		t.Fatal("expected rejection error")
	}
}

func TestHITLPayloadMerge(t *testing.T) {
	wf := mf.CreateWorkflow("hitl-payload", nil)
	mf.AddTask(wf, "gated", func(ctx map[string]any, _ map[string]any) (any, error) {
		return ctx["injected"], nil
	}, mf.WithHITL())

	go func() {
		time.Sleep(50 * time.Millisecond)
		mf.SendSignal(wf, "gated", mf.HITLSignal{
			Decision: "approve",
			Payload:  map[string]any{"injected": "hello"},
		})
	}()

	results, err := mf.Run(wf)
	if err != nil {
		t.Fatal(err)
	}
	if results["gated"] != "hello" {
		t.Fatalf("expected 'hello', got %v", results["gated"])
	}
}

// ── observability ─────────────────────────────────────────────────

func TestSinkReceivesEvents(t *testing.T) {
	var events []mf.Event
	wf := mf.CreateWorkflow("sink", nil)
	mf.RegisterSink(wf, func(e mf.Event) { events = append(events, e) })
	mf.AddTask(wf, "t", taskOK(42))

	mf.Run(wf) //nolint
	if len(events) == 0 {
		t.Fatal("no events received")
	}
	// expect at least: workflow_started, task_started, task_succeeded, workflow_succeeded
	types := map[string]bool{}
	for _, e := range events {
		types[e.EventType] = true
	}
	for _, want := range []string{"workflow_started", "task_started", "task_succeeded", "workflow_succeeded"} {
		if !types[want] {
			t.Errorf("missing event type %q", want)
		}
	}
}

// ── RunAsync ──────────────────────────────────────────────────────

func TestRunAsync(t *testing.T) {
	wf := mf.CreateWorkflow("async", nil)
	mf.AddTask(wf, "a", taskOK("async-result"))

	outCh, errCh := mf.RunAsync(wf)
	select {
	case res := <-outCh:
		if res["a"] != "async-result" {
			t.Fatalf("expected async-result, got %v", res["a"])
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for async result")
	}
}
