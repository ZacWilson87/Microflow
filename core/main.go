package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// printJSONSink writes each observability event as a JSON line to stdout.
func printJSONSink(event Event) {
	b, _ := json.Marshal(event)
	fmt.Println(string(b))
}

func usage() {
	fmt.Fprintf(os.Stderr, `microflow-core — Go engine for the microflow hybrid architecture

Usage:
  microflow-core run <flow.json> [--worker <path>] [--python <bin>]

Arguments:
  <flow.json>         Path to the workflow spec produced by bridge.py

Flags:
  --worker <path>     Path to worker.py  (default: worker.py next to this binary)
  --python <bin>      Python interpreter (default: python3)

Example:
  microflow-core run examples/flow.json
  microflow-core run examples/flow.json --worker /app/worker.py --python python3.11
`)
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		usage()
		os.Exit(0)
	}
	if args[0] != "run" {
		fmt.Fprintf(os.Stderr, "unknown subcommand %q — only 'run' is supported\n", args[0])
		usage()
		os.Exit(1)
	}
	args = args[1:] // consume "run"

	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "error: missing <flow.json> argument")
		usage()
		os.Exit(1)
	}

	flowPath := args[0]
	args = args[1:]

	// Defaults
	workerPath := filepath.Join(filepath.Dir(os.Args[0]), "worker.py")
	pythonBin := "python3"

	// Parse remaining flags
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--worker":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "error: --worker requires a value")
				os.Exit(1)
			}
			i++
			workerPath = args[i]
		case "--python":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "error: --python requires a value")
				os.Exit(1)
			}
			i++
			pythonBin = args[i]
		default:
			fmt.Fprintf(os.Stderr, "unknown flag %q\n", args[i])
			usage()
			os.Exit(1)
		}
	}

	// ── Load flow.json ────────────────────────────────────────────────────────
	raw, err := os.ReadFile(flowPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading %s: %v\n", flowPath, err)
		os.Exit(1)
	}
	var flow FlowSpec
	if err := json.Unmarshal(raw, &flow); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing %s: %v\n", flowPath, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "[microflow-core] workflow=%q tasks=%d worker=%s python=%s\n",
		flow.Name, len(flow.Tasks), workerPath, pythonBin)

	// ── Resolve worker.py path ────────────────────────────────────────────────
	absWorker, err := filepath.Abs(workerPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error resolving worker path: %v\n", err)
		os.Exit(1)
	}
	if _, err := os.Stat(absWorker); err != nil {
		fmt.Fprintf(os.Stderr, "worker not found at %s: %v\n", absWorker, err)
		os.Exit(1)
	}

	// ── Build executor + scheduler ────────────────────────────────────────────
	exec := &Executor{
		WorkflowID: flow.Name,
		WorkerPath: absWorker,
		PythonBin:  pythonBin,
		Sink:       printJSONSink,
	}

	scheduler, err := NewScheduler(flow, exec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// ── Run ───────────────────────────────────────────────────────────────────
	ctx := context.Background()
	results, runErr := scheduler.Run(ctx)

	// ── Summary (to stderr so stdout stays clean JSON lines) ─────────────────
	fmt.Fprintln(os.Stderr, "\n[microflow-core] Final results:")
	for _, task := range flow.Tasks {
		r, ok := results[task.TaskID]
		if !ok {
			fmt.Fprintf(os.Stderr, "  %-25s [not started]\n", task.TaskID)
			continue
		}
		switch r.Status {
		case StatusSuccess:
			outBytes, _ := json.Marshal(r.Output)
			fmt.Fprintf(os.Stderr, "  %-25s [success] output=%s\n", task.TaskID, outBytes)
		case StatusFailed:
			errPreview := r.Error
			if len(errPreview) > 120 {
				errPreview = errPreview[:120] + "..."
			}
			fmt.Fprintf(os.Stderr, "  %-25s [failed]  error=%s\n", task.TaskID, errPreview)
		default:
			fmt.Fprintf(os.Stderr, "  %-25s [%s]\n", task.TaskID, r.Status)
		}
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "\n[microflow-core] workflow failed: %v\n", runErr)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "\n[microflow-core] workflow completed successfully")
}
