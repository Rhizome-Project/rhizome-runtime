package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runFinops(args []string) error {
	if len(args) < 1 {
		printFinopsUsage(os.Stderr)
		return errors.New("missing finops subcommand")
	}

	switch args[0] {
	case "spend":
		return runFinopsSpend(args[1:])
	case "ledger":
		return runFinopsLedger(args[1:])
	default:
		printFinopsUsage(os.Stderr)
		return fmt.Errorf("unknown finops subcommand: %s", args[0])
	}
}

func runFinopsSpend(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("finops spend", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	nodeID := fs.String("node-id", "", "Node identifier")
	amountUSD := fs.Float64("amount-usd", 0, "Spend amount in USD")
	serviceID := fs.String("service-id", "manual_spend", "Service identifier")
	ownerUserID := fs.String("owner-user-id", "", "Owner user id (optional, inferred from task by default)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*taskID) == "" {
		return errors.New("--task-id is required")
	}
	if strings.TrimSpace(*nodeID) == "" {
		return errors.New("--node-id is required")
	}
	if *amountUSD <= 0 {
		return errors.New("--amount-usd must be positive")
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	status, err := store.GetTaskStatus(ctx, "", *taskID)
	if err != nil {
		if errors.Is(err, sqlite.ErrTaskNotFound) {
			return fmt.Errorf("task not found: %s", strings.TrimSpace(*taskID))
		}
		return fmt.Errorf("lookup task for spend: %w", err)
	}

	owner := strings.TrimSpace(*ownerUserID)
	if owner == "" {
		owner = status.OwnerUserID
	}
	if owner == "" {
		return errors.New("owner_user_id cannot be inferred; pass --owner-user-id explicitly")
	}

	txID := nextCLIID("tx")
	if err := store.RecordSpendTransaction(ctx, sqlite.SpendTransactionInput{
		TxID:        txID,
		OwnerUserID: owner,
		TaskID:      strings.TrimSpace(*taskID),
		NodeID:      strings.TrimSpace(*nodeID),
		ServiceID:   strings.TrimSpace(*serviceID),
		AmountUSD:   *amountUSD,
	}); err != nil {
		return fmt.Errorf("record spend transaction: %w", err)
	}

	if err := store.SetNodeStatus(ctx, sqlite.NodeStatusUpdateInput{
		TransitionID: nextCLIID("node-transition"),
		TaskID:       strings.TrimSpace(*taskID),
		NodeID:       strings.TrimSpace(*nodeID),
		NewStatus:    model.NodeStatusRunning,
		Reason:       "auto_approve",
		ActorID:      owner,
	}); err != nil {
		return fmt.Errorf("set node status to RUNNING after spend: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"tx_id":      txID,
		"task_id":    strings.TrimSpace(*taskID),
		"node_id":    strings.TrimSpace(*nodeID),
		"amount_usd": *amountUSD,
		"status":     "RECORDED",
		"trace_id":   traceID,
	})
}

func runFinopsLedger(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("finops ledger", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	format := fs.String("format", "json", "Output format: json|csv")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*taskID) == "" {
		return errors.New("--task-id is required")
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	entries, err := store.ListSpendTransactionsByTask(ctx, *taskID)
	if err != nil {
		return fmt.Errorf("list spend transactions: %w", err)
	}

	switch strings.ToLower(strings.TrimSpace(*format)) {
	case "json", "":
		return writeJSON(os.Stdout, map[string]any{
			"task_id":  strings.TrimSpace(*taskID),
			"entries":  entries,
			"trace_id": traceID,
		})
	case "csv":
		return writeLedgerCSV(os.Stdout, entries)
	default:
		return fmt.Errorf("unsupported --format value: %s", *format)
	}
}

func printFinopsUsage(out *os.File) {
	fmt.Fprintln(out, "FinOps commands:")
	fmt.Fprintln(out, "  rhizome finops spend --task-id <id> --node-id <id> --amount-usd <n> [--service-id manual_spend] [--owner-user-id <user>]")
	fmt.Fprintln(out, "  rhizome finops ledger --task-id <id> [--format json|csv]")
}

func writeLedgerCSV(out *os.File, entries []sqlite.SpendTransactionRecord) error {
	writer := csv.NewWriter(out)
	defer writer.Flush()

	if err := writer.Write([]string{
		"tx_id", "owner_user_id", "task_id", "node_id", "service_id", "amount_usd", "created_at",
	}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for _, entry := range entries {
		if err := writer.Write([]string{
			entry.TxID,
			entry.OwnerUserID,
			entry.TaskID,
			entry.NodeID,
			entry.ServiceID,
			strconv.FormatFloat(entry.AmountUSD, 'f', -1, 64),
			entry.CreatedAt,
		}); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}

	return writer.Error()
}
