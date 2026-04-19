// hitl_review — task pauses at a HITL gate; a goroutine approves after 500ms.
// Run: go run ./examples/go/hitl_review
package main

import (
	"fmt"
	"time"

	mf "github.com/zacwilson87/microflow"
)

func draftReport(_ map[string]any, _ map[string]any) (any, error) {
	return map[string]any{"draft": "Q1 revenue up 12%", "confidence": 0.91}, nil
}

func publishReport(ctx map[string]any, results map[string]any) (any, error) {
	draft := results["draft_report"].(map[string]any)
	approvedBy := ctx["reviewer"]
	fmt.Printf("  [publish] publishing report approved by %v\n", approvedBy)
	return map[string]any{"published": true, "report": draft}, nil
}

func main() {
	wf := mf.CreateWorkflow("hitl-review", nil)
	mf.RegisterSink(wf, mf.DefaultSink)

	mf.AddTask(wf, "draft_report", draftReport)
	mf.AddTask(wf, "review_gate", func(_ map[string]any, _ map[string]any) (any, error) {
		fmt.Println("  [review_gate] waiting for human approval...")
		return "approved", nil
	}, mf.WithHITL(), mf.WithDependsOn("draft_report"))
	mf.AddTask(wf, "publish_report", publishReport,
		mf.WithDependsOn("draft_report", "review_gate"))

	// Simulate a human reviewer approving after 500ms.
	go func() {
		time.Sleep(500 * time.Millisecond)
		fmt.Println("  [human] approving report...")
		mf.SendSignal(wf, "review_gate", mf.HITLSignal{
			Decision: "approve",
			Payload:  map[string]any{"reviewer": "alice"},
		})
	}()

	results, err := mf.Run(wf)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}
	fmt.Printf("\nresult: %v\n", results["publish_report"])
}
