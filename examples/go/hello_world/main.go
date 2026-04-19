// hello_world — a two-task linear chain.
// Run: go run ./examples/go/hello_world
package main

import (
	"fmt"

	mf "github.com/zacwilson87/microflow"
)

func fetchData(ctx map[string]any, _ map[string]any) (any, error) {
	env := ctx["env"]
	fmt.Printf("  [fetch_data] querying %v...\n", env)
	return map[string]any{"rows": 42}, nil
}

func process(_ map[string]any, results map[string]any) (any, error) {
	rows := results["fetch_data"].(map[string]any)["rows"].(int)
	fmt.Printf("  [process] got %d rows\n", rows)
	return map[string]any{"processed": rows * 2}, nil
}

func main() {
	wf := mf.CreateWorkflow("hello-world", map[string]any{"env": "prod"})
	mf.RegisterSink(wf, mf.DefaultSink)

	mf.AddTask(wf, "fetch_data", fetchData)
	mf.AddTask(wf, "process", process, mf.WithDependsOn("fetch_data"))

	results, err := mf.Run(wf)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}
	fmt.Printf("\nresult: %v\n", results["process"])
}
