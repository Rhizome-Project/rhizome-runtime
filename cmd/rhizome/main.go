package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	daemonpkg "github.com/Rhizome-Project/rhizome-runtime/internal/daemon"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/transport/rpc"
)

func main() {
	if code := runRootCLI(os.Args[1:], os.Stdout, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

func runRootCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		printUsage(stderr)
		return 2
	}
	if isHelpArgument(args[0]) {
		printUsage(stdout)
		return 0
	}
	if len(args) > 1 && isHelpArgument(args[1]) && printCommandGroupUsage(stdout, args[0]) {
		return 0
	}

	var err error
	switch args[0] {
	case "task":
		err = runTask(args[1:])
	case "workspace":
		err = runWorkspace(args[1:])
	case "agent":
		err = runAgent(args[1:])
	case "auth":
		err = runAuth(args[1:])
	case "tool":
		err = runTool(args[1:])
	case "finops":
		err = runFinops(args[1:])
	case "approval":
		err = runApproval(args[1:])
	case "daemon":
		err = runDaemon(args[1:])
	case "runtime":
		err = runRuntime(args[1:])
	case "audit":
		err = runAudit(args[1:])
	case "backup":
		err = runBackup(args[1:])
	case "doctor":
		err = runDoctor(args[1:])
	case "serve":
		err = runServe(args[1:])
	case "config":
		err = runConfig(args[1:])
	default:
		err = fmt.Errorf("unknown command: %s", args[0])
	}

	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func printCommandGroupUsage(out io.Writer, command string) bool {
	lines := map[string][]string{
		"task":      {"rhizome task <submit|status|graph|hydrate|close|template|run> [flags]"},
		"workspace": {"rhizome workspace <create|status|search|doc|task|artifact> [flags]"},
		"agent":     {"rhizome agent <register|heartbeat|bootstrap|update|task|create|list|show|delete> [flags]"},
		"auth": {
			"rhizome auth login [--listen-addr :1455]",
			"rhizome auth login --no-save --print-api-key [--listen-addr :1455]",
		},
		"tool":     {"rhizome tool <register|status|list|remove> [flags]"},
		"finops":   {"rhizome finops <spend|ledger> [flags]"},
		"approval": {"rhizome approval <list|decide|patch-queue-enable> [flags]"},
		"daemon":   {"rhizome daemon run [flags]"},
		"runtime":  {"rhizome runtime metrics [flags]"},
		"audit":    {"rhizome audit export [flags]"},
		"backup":   {"rhizome backup <create|restore|verify|verify-restore> [flags]"},
		"config":   {"rhizome config <show|save|load> [flags]"},
	}
	usage, ok := lines[strings.ToLower(strings.TrimSpace(command))]
	if !ok {
		return false
	}
	fmt.Fprintln(out, "Usage:")
	for _, line := range usage {
		fmt.Fprintln(out, "  "+line)
	}
	return true
}

func isHelpArgument(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "help", "-h", "--help":
		return true
	default:
		return false
	}
}

func runDaemon(args []string) error {
	if len(args) < 1 || args[0] != "run" {
		printDaemonUsage(os.Stderr)
		return errors.New("missing daemon subcommand")
	}

	traceID := newTraceID()
	fs := flag.NewFlagSet("daemon run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	once := fs.Bool("once", false, "Run a single scheduling tick and exit")
	pollMS := fs.Int("poll-ms", 1000, "Polling interval in milliseconds")
	maxNodes := fs.Int("max-nodes", 10, "Maximum nodes per tick")
	nodeTimeoutSec := fs.Int("node-timeout-sec", 120, "Executor node timeout in seconds")
	format := fs.String("format", "json", "Output format: json|jsonl")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	if *pollMS <= 0 {
		return errors.New("--poll-ms must be positive")
	}
	if *maxNodes <= 0 {
		return errors.New("--max-nodes must be positive")
	}
	if *nodeTimeoutSec <= 0 {
		return errors.New("--node-timeout-sec must be positive")
	}
	outputFormat, err := normalizeOutputFormat(*format)
	if err != nil {
		return err
	}

	cfg := app.LoadConfig()
	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	runner, err := newDaemonRunner(store, cfg, *maxNodes, *nodeTimeoutSec, "daemon")
	if err != nil {
		return err
	}

	if *once {
		runCtx, runCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer runCancel()
		processed, err := runner.RunOnce(runCtx)
		if err != nil {
			return fmt.Errorf("daemon run once: %w", err)
		}
		if outputFormat == outputFormatJSONL {
			return writeJSONLine(os.Stdout, daemonEvent{
				Event:          "daemon_tick",
				Mode:           "once",
				ProcessedNodes: processed,
				TraceID:        traceID,
				TS:             time.Now().UTC().Format(time.RFC3339Nano),
			})
		}
		return writeJSON(os.Stdout, map[string]any{
			"processed_nodes": processed,
			"mode":            "once",
			"trace_id":        traceID,
		})
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if outputFormat == outputFormatJSON {
		err = runner.RunLoop(runCtx, time.Duration(*pollMS)*time.Millisecond)
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("daemon run loop: %w", err)
		}

		return writeJSON(os.Stdout, map[string]any{
			"status":   "stopped",
			"mode":     "loop",
			"trace_id": traceID,
		})
	}

	ticker := time.NewTicker(time.Duration(*pollMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		tickCtx, tickCancel := context.WithTimeout(runCtx, 5*time.Minute)
		processed, tickErr := runner.RunOnce(tickCtx)
		tickCancel()
		if tickErr != nil && !errors.Is(tickErr, context.Canceled) {
			return fmt.Errorf("daemon run loop: %w", tickErr)
		}

		if err := writeJSONLine(os.Stdout, daemonEvent{
			Event:          "daemon_tick",
			Mode:           "loop",
			ProcessedNodes: processed,
			TraceID:        traceID,
			TS:             time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			return err
		}

		select {
		case <-runCtx.Done():
			return writeJSONLine(os.Stdout, daemonEvent{
				Event:   "daemon_stopped",
				Mode:    "loop",
				Status:  "stopped",
				TraceID: traceID,
				TS:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		case <-ticker.C:
		}
	}
}

func openStore() (*sqlite.Store, error) {
	cfg := app.LoadConfig()
	store, err := sqlite.NewStore(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store: %w", err)
	}
	return store, nil
}

func writeJSON(out *os.File, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writeJSONLine(out *os.File, v any) error {
	enc := json.NewEncoder(out)
	return enc.Encode(v)
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, "Usage:")
	fmt.Fprintln(out, "  rhizome task <submit|status|graph|hydrate|close|template|run> [flags]")
	fmt.Fprintln(out, "  rhizome workspace <create|status|search|doc|task|artifact> [flags]")
	fmt.Fprintln(out, "  rhizome agent <register|heartbeat|bootstrap|update|task|create|list|show|delete> [flags]")
	fmt.Fprintln(out, "  rhizome auth login [--listen-addr :1455]")
	fmt.Fprintln(out, "  rhizome auth login --no-save --print-api-key [--listen-addr :1455]")
	fmt.Fprintln(out, "  rhizome tool <register|status|list|remove> [flags]")
	fmt.Fprintln(out, "  rhizome finops <spend|ledger> [flags]")
	fmt.Fprintln(out, "  rhizome approval <list|decide|patch-queue-enable> [flags]")
	fmt.Fprintln(out, "  rhizome daemon run [flags]")
	fmt.Fprintln(out, "  rhizome serve [--addr 127.0.0.1:8420] [--allow-remote] [--with-daemon]")
	fmt.Fprintln(out, "  rhizome runtime metrics [flags]")
	fmt.Fprintln(out, "  rhizome audit export [flags]")
	fmt.Fprintln(out, "  rhizome backup <create|restore|verify|verify-restore> [flags]")
	fmt.Fprintln(out, "  rhizome doctor [flags]")
	fmt.Fprintln(out, "  rhizome config <show|save|load> [flags]")
}

func printDaemonUsage(out *os.File) {
	fmt.Fprintln(out, "Daemon commands:")
	fmt.Fprintln(out, "  rhizome daemon run [--once] [--poll-ms 1000] [--max-nodes 10] [--node-timeout-sec 120] [--format json|jsonl]")
}

func printRuntimeUsage(out *os.File) {
	fmt.Fprintln(out, "Runtime commands:")
	fmt.Fprintln(out, "  rhizome runtime metrics [--last 1] [--format json|jsonl] [--metrics-file path.jsonl]")
}

func nextCLIID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

func newTraceID() string {
	return nextCLIID("tr")
}

func newDaemonRunner(
	store *sqlite.Store,
	cfg app.Config,
	maxNodes int,
	nodeTimeoutSec int,
	actorID string,
) (*daemonpkg.Runner, error) {
	workDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("resolve working directory: %w", err)
	}
	runtimeClient, err := rpc.NewStdioRuntimeClient(rpc.StdioRuntimeClientConfig{
		PythonBin:    cfg.ExecutorPython,
		BridgeScript: cfg.ExecutorBridgeScript,
		WorkDir:      workDir,
		Env: map[string]string{
			"RHIZOME_WORKSPACE_ROOT": cfg.WorkspaceRoot,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime client: %w", err)
	}

	runner, err := daemonpkg.NewRunner(store, runtimeClient, daemonpkg.RunnerConfig{
		WorkspaceRoot:   cfg.WorkspaceRoot,
		MaxNodesPerTick: maxNodes,
		NodeTimeoutSec:  nodeTimeoutSec,
		ActorID:         actorID,
	})
	if err != nil {
		return nil, fmt.Errorf("create daemon runner: %w", err)
	}
	return runner, nil
}

const (
	outputFormatJSON  = "json"
	outputFormatJSONL = "jsonl"
)

type daemonEvent struct {
	Event          string `json:"event"`
	Mode           string `json:"mode"`
	Status         string `json:"status,omitempty"`
	ProcessedNodes int    `json:"processed_nodes,omitempty"`
	TraceID        string `json:"trace_id"`
	TS             string `json:"ts"`
}

func normalizeOutputFormat(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", outputFormatJSON:
		return outputFormatJSON, nil
	case outputFormatJSONL:
		return outputFormatJSONL, nil
	default:
		return "", fmt.Errorf("unsupported --format value: %s", raw)
	}
}
