// retry_chaos — a flaky task fails twice then succeeds on the third attempt.
// Run: go run ./examples/go/retry_chaos
package main

import (
	"errors"
	"fmt"
	"sync/atomic"

	mf "github.com/zacwilson87/microflow"
)

func main() {
	var attempts atomic.Int32

	flaky := func(_ map[string]any, _ map[string]any) (any, error) {
		n := attempts.Add(1)
		fmt.Printf("  [flaky] attempt %d\n", n)
		if n < 3 {
			return nil, errors.New("transient error — try again")
		}
		return map[string]any{"status": "ok", "attempts": int(n)}, nil
	}

	wf := mf.CreateWorkflow("retry-chaos", nil)
	mf.RegisterSink(wf, mf.DefaultSink)
	mf.AddTask(wf, "flaky_task", flaky, mf.WithRetries(3))

	results, err := mf.Run(wf)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}
	fmt.Printf("\nresult: %v\n", results["flaky_task"])
}
