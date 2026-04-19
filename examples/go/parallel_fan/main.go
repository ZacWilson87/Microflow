// parallel_fan — three independent tasks run concurrently, fan-in to a summary.
// Run: go run ./examples/go/parallel_fan
package main

import (
	"fmt"
	"time"

	mf "github.com/zacwilson87/microflow"
)

func scrape(name string, delay time.Duration) mf.TaskFunc {
	return func(_ map[string]any, _ map[string]any) (any, error) {
		fmt.Printf("  [%s] fetching...\n", name)
		time.Sleep(delay)
		return map[string]any{"source": name, "count": 10}, nil
	}
}

func summarise(_ map[string]any, results map[string]any) (any, error) {
	total := 0
	for _, v := range results {
		total += v.(map[string]any)["count"].(int)
	}
	return map[string]any{"total": total}, nil
}

func main() {
	wf := mf.CreateWorkflow("parallel-fan", nil)
	mf.RegisterSink(wf, mf.DefaultSink)

	mf.AddTask(wf, "source_a", scrape("source_a", 100*time.Millisecond))
	mf.AddTask(wf, "source_b", scrape("source_b", 150*time.Millisecond))
	mf.AddTask(wf, "source_c", scrape("source_c", 80*time.Millisecond))
	mf.AddTask(wf, "summarise", summarise,
		mf.WithDependsOn("source_a", "source_b", "source_c"))

	results, err := mf.Run(wf)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}
	fmt.Printf("\nsummary: %v\n", results["summarise"])
}
