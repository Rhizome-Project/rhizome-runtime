package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runAudit(args []string) error {
	if len(args) < 1 {
		printAuditUsage(os.Stderr)
		return errors.New("missing audit subcommand")
	}

	switch args[0] {
	case "export":
		return runAuditExport(args[1:])
	default:
		printAuditUsage(os.Stderr)
		return fmt.Errorf("unknown audit subcommand: %s", args[0])
	}
}

func runAuditExport(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("audit export", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	eventType := fs.String("event-type", "", "Filter by audit event type")
	entityType := fs.String("entity-type", "", "Filter by entity type")
	entityID := fs.String("entity-id", "", "Filter by entity id")
	actorID := fs.String("actor-id", "", "Filter by actor id")
	limit := fs.Int("limit", 100, "Maximum number of audit events to export")
	format := fs.String("format", "json", "Output format: json|jsonl|csv")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *limit <= 0 {
		return errors.New("--limit must be positive")
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

	filter := sqlite.AuditEventFilter{
		EventType:  strings.TrimSpace(*eventType),
		EntityType: strings.TrimSpace(*entityType),
		EntityID:   strings.TrimSpace(*entityID),
		ActorID:    strings.TrimSpace(*actorID),
		Limit:      *limit,
	}
	events, err := store.ListAuditEvents(ctx, filter)
	if err != nil {
		return fmt.Errorf("list audit events: %w", err)
	}

	switch strings.ToLower(strings.TrimSpace(*format)) {
	case "", outputFormatJSON:
		return writeJSON(os.Stdout, map[string]any{
			"trace_id": traceID,
			"count":    len(events),
			"filters": map[string]any{
				"event_type":  filter.EventType,
				"entity_type": filter.EntityType,
				"entity_id":   filter.EntityID,
				"actor_id":    filter.ActorID,
				"limit":       filter.Limit,
			},
			"entries": events,
		})
	case outputFormatJSONL:
		for _, entry := range events {
			if err := writeJSONLine(os.Stdout, map[string]any{
				"event":    "audit_entry",
				"trace_id": traceID,
				"entry":    entry,
				"ts":       time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				return err
			}
		}
		return writeJSONLine(os.Stdout, map[string]any{
			"event":    "audit_export_summary",
			"trace_id": traceID,
			"count":    len(events),
			"filters": map[string]any{
				"event_type":  filter.EventType,
				"entity_type": filter.EntityType,
				"entity_id":   filter.EntityID,
				"actor_id":    filter.ActorID,
				"limit":       filter.Limit,
			},
			"ts": time.Now().UTC().Format(time.RFC3339Nano),
		})
	case "csv":
		return writeAuditCSV(os.Stdout, events)
	default:
		return fmt.Errorf("unsupported --format value: %s", *format)
	}
}

func printAuditUsage(out *os.File) {
	fmt.Fprintln(out, "Audit commands:")
	fmt.Fprintln(out, "  rhizome audit export [--event-type type] [--entity-type type] [--entity-id id] [--actor-id id] [--limit 100] [--format json|jsonl|csv]")
}

func writeAuditCSV(out *os.File, entries []sqlite.AuditEventRecord) error {
	writer := csv.NewWriter(out)
	defer writer.Flush()

	if err := writer.Write([]string{
		"event_id", "event_type", "entity_type", "entity_id", "actor_id", "payload_json", "created_at",
	}); err != nil {
		return fmt.Errorf("write audit csv header: %w", err)
	}

	for _, entry := range entries {
		if err := writer.Write([]string{
			entry.EventID,
			entry.EventType,
			entry.EntityType,
			entry.EntityID,
			entry.ActorID,
			entry.PayloadJSON,
			entry.CreatedAt,
		}); err != nil {
			return fmt.Errorf("write audit csv row: %w", err)
		}
	}

	return writer.Error()
}
