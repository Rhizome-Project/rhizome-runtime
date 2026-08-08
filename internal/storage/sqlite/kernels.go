package sqlite

import (
	"context"
	"database/sql"
)

// JournalKernel defines raw audit and base-level ledger semantics.
type JournalKernel interface {
	AddAuditEvent(ctx context.Context, input AuditEventInput) error
	addAuditEventTx(ctx context.Context, tx *sql.Tx, input AuditEventInput) error
}

// MemoryKernel defines MemoryNode, Tension, and knowledge graph semantics.
// (Interface surface will be populated iteratively)
type MemoryKernel interface {
}

// CoordinationKernel defines Task, AgentWork, Node mapping semantics.
type CoordinationKernel interface {
	SetNodeStatus(ctx context.Context, input NodeStatusUpdateInput) error
}

// StatKernel defines RSP, Anomaly detection, telemetry semantics.
type StatKernel interface {
}

// -- Core Implementations --

// journalCore implements JournalKernel
type journalCore struct {
	db      *sql.DB
	writeDB *sql.DB
	store   *Store
}

// memoryCore implements MemoryKernel
type memoryCore struct {
	db      *sql.DB
	writeDB *sql.DB
	journal JournalKernel
	store   *Store
}

// coordinationCore implements CoordinationKernel
type coordinationCore struct {
	db      *sql.DB
	writeDB *sql.DB
	memory  MemoryKernel
	store   *Store
}

// statCore implements StatKernel
type statCore struct {
	db      *sql.DB
	writeDB *sql.DB
	memory  MemoryKernel
	store   *Store
}

// Helper to begin immediate transactions across all cores if needed.
func beginTxImmediate(writeDB *sql.DB, ctx context.Context) (*sql.Tx, error) {
	tx, err := writeDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return tx, nil
}
