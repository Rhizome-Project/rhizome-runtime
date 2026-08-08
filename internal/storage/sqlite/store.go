package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/fssecure"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/orchestrator"

	_ "modernc.org/sqlite"
)

const (
	taskStatusPending = model.TaskStatusPending
	nodeStatusPending = model.NodeStatusPending
	nodeStatusBlocked = model.NodeStatusBlocked
)

var (
	ErrTaskNotFound                   = errors.New("task not found")
	ErrNodeNotFound                   = errors.New("node not found")
	ErrApprovalNotFound               = errors.New("approval not found")
	ErrBudgetExceeded                 = errors.New("budget exceeded")
	ErrFeatureDisabled                = errors.New("feature disabled")
	ErrTaskProjectFieldsBindingActive = errors.New("task project lane/gate mutation blocked by active task binding")
)

var idCounter uint64

type Store struct {
	db          *sql.DB // Read-only pool
	writeDB     *sql.DB // Write-only pool (MaxOpenConns=1)
	rspFirehose *RSPFirehose
	dbPath      string

	authorityIdentityPath   string
	authorityBootInstanceID string
	authorityDiagnostics    atomic.Value

	beforeLocalSessionReclaimTxHook   func(context.Context, localSessionReclaimCandidate)
	beforeLocalTaskClaimReclaimTxHook func(context.Context, localTaskClaimReclaimCandidate)

	allowLegacyPatchOnlySubmits bool

	JournalKernel
	MemoryKernel
	CoordinationKernel
	StatKernel
}

// DB returns the underlying database connection.
func (s *Store) DB() *sql.DB { return s.db }

// WriteDB returns the serialized write connection for helper stores that own writes.
func (s *Store) WriteDB() *sql.DB { return s.writeDB }

// AllowLegacyPatchOnlySubmitsForTesting permits tests to seed historical
// patch_only_temp_repo queue items. Runtime submit paths must leave this off.
func (s *Store) AllowLegacyPatchOnlySubmitsForTesting() {
	if s != nil {
		s.allowLegacyPatchOnlySubmits = true
	}
}

func (s *Store) LegacyPatchOnlySubmitsAllowedForTesting() bool {
	return s != nil && s.allowLegacyPatchOnlySubmits
}

type RPCAccessLogInput struct {
	Method      string
	WorkspaceID string
	Actor       string
	Status      string
	ErrorMsg    string
	LatencyMs   int64
	CreatedAt   time.Time
}

func (s *Store) RecordRPCAccess(ctx context.Context, input RPCAccessLogInput) error {
	if s == nil {
		return errors.New("store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	createdAt := input.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO rpc_access_log (method, workspace_id, actor, status, error_msg, latency_ms, created_at) VALUES (?,?,?,?,?,?,?)`,
		strings.TrimSpace(input.Method),
		strings.TrimSpace(input.WorkspaceID),
		strings.TrimSpace(input.Actor),
		strings.TrimSpace(input.Status),
		strings.TrimSpace(input.ErrorMsg),
		input.LatencyMs,
		createdAt.Format(time.RFC3339Nano),
	)
	return err
}

func (s *Store) SetRSPFirehoseLiveMirror(fn func(RuntimeEventRecord)) {
	if s == nil || s.rspFirehose == nil {
		return
	}
	s.rspFirehose.SetLiveMirror(fn)
}

// RSPFirehoseStatus contains honest readiness signals for the firehose loop.
type RSPFirehoseStatus struct {
	Running       bool  `json:"running"`
	DroppedEvents int64 `json:"dropped_events"`
}

// RSPFirehoseReadiness returns the current firehose loop state.
// This reflects actual goroutine ownership and event loss, not config flags.
func (s *Store) RSPFirehoseReadiness() RSPFirehoseStatus {
	if s == nil || s.rspFirehose == nil {
		return RSPFirehoseStatus{}
	}
	return RSPFirehoseStatus{
		Running:       s.rspFirehose.IsRunning(),
		DroppedEvents: s.rspFirehose.DroppedEvents(),
	}
}

// BeginTxImmediate starts an SQLite transaction using the dedicated writeDB (MaxOpenConns=1)
// to prevent SQLITE_BUSY deadlocks caused by concurrent writers.
func (s *Store) BeginTxImmediate(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin write tx: %w", err)
	}
	return tx, nil
}

func beginConnImmediate(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate transaction: %w", err)
	}
	return nil
}

type TaskCreateInput struct {
	WorkspaceID          string
	TaskID               string
	OwnerUserID          string
	Priority             string
	Title                string
	Description          string
	TaskKind             string
	TaskTemplate         string
	TaskClass            string
	TaskClassSource      string
	Tags                 []string
	ProjectID            string
	ProjectLane          string
	RequiresProjectGate  bool
	DependencyTaskIDs    []string
	RelatedTaskIDs       []string
	TaskRequirementsJSON string
	WriteScopeHints      []string

	// CarrierKind, when set, stamps the decisive-path carrier kind onto the born task at the single
	// storage creation chokepoint (createTaskWithGraphTx). It is the birth-site half of the decisive-path
	// primitive (root A, stage 4): every born-active/pending owner-bound carrier declares its kind at
	// birth so decisivePathRoute can classify it (and the fail-closed default catches an unrecognized
	// kind). Empty = not a gated decisive-path carrier (ordinary task).
	CarrierKind string
}

type TaskCloseInput struct {
	WorkspaceID string
	TaskID      string
	ActorID     string
	Resolution  string
	Reason      string
}

type TaskStatus struct {
	TaskID               string           `json:"task_id"`
	Title                string           `json:"title,omitempty"`
	Description          string           `json:"description,omitempty"`
	OwnerUserID          string           `json:"owner_user_id"`
	Priority             string           `json:"priority"`
	Status               string           `json:"status"`
	TaskKind             string           `json:"task_kind"`
	TaskTemplate         string           `json:"task_template"`
	TaskClass            string           `json:"task_class,omitempty"`
	TaskClassSource      string           `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt   string           `json:"task_class_updated_at,omitempty"`
	ProjectID            string           `json:"project_id,omitempty"`
	ProjectLane          string           `json:"project_lane,omitempty"`
	RequiresProjectGate  bool             `json:"requires_project_gate,omitempty"`
	TaskRequirementsJSON string           `json:"task_requirements_json,omitempty"`
	WriteScopeHints      []string         `json:"write_scope_hints,omitempty"`
	CreatedAt            string           `json:"created_at"`
	UpdatedAt            string           `json:"updated_at"`
	NodeCounts           map[string]int   `json:"node_counts"`
	Nodes                []TaskStatusNode `json:"nodes"`
}

type TaskProjectFieldsUpdateInput struct {
	WorkspaceID         string
	TaskID              string
	ProjectID           *string
	TaskKind            *string
	ProjectLane         *string
	RequiresProjectGate *bool
	ActorID             string
}

type TaskClassEvidencePutInput struct {
	WorkspaceID     string
	TaskID          string
	TaskClass       string
	TaskClassSource string
	ActorID         string
}

type TaskStatusNode struct {
	NodeID       string   `json:"node_id"`
	Type         string   `json:"type"`
	Status       string   `json:"status"`
	AttemptCount int      `json:"attempt_count"`
	LastError    *string  `json:"last_error,omitempty"`
	DependsOn    []string `json:"depends_on"`
}

type ResourceRequestCreateInput struct {
	RequestID        string
	TaskID           string
	NodeID           string
	OwnerUserID      string
	ResourceType     string
	ServiceID        string
	EstimatedCostUSD float64
	Justification    string
	IdempotencyKey   string
	Decision         string
	DecisionReason   string
}

type ApprovalCreateInput struct {
	ApprovalID string
	RequestID  string
	Status     string
	TTLSec     int
}

type ApprovalEventInput struct {
	EventID     string
	ApprovalID  string
	EventType   string
	ActorID     string
	PayloadJSON string
	OccurredAt  string
}

type ApprovalDecisionInput struct {
	ApprovalID   string
	NewStatus    string
	DecidedBy    string
	DecisionNote string
}

type SpendTransactionInput struct {
	TxID        string
	OwnerUserID string
	TaskID      string
	NodeID      string
	ServiceID   string
	AmountUSD   float64
}

type SpendGuardedInput struct {
	TxID             string
	OwnerUserID      string
	TaskID           string
	NodeID           string
	ServiceID        string
	AmountUSD        float64
	MaxDailySpendUSD float64
	MaxTaskSpendUSD  float64
}

type ClearingEntryInput struct {
	EntryID        string
	DebtorUserID   string
	CreditorUserID string
	ResourceKey    string
	Amount         float64
}

type NodeStatusUpdateInput struct {
	TransitionID string
	TaskID       string
	NodeID       string
	NewStatus    string
	Reason       string
	ActorID      string
}

type AuditEventInput struct {
	EventID     string
	EventType   string
	EntityType  string
	EntityID    string
	ActorID     string
	PayloadJSON string
}

type ApprovalRecord struct {
	ApprovalID   string  `json:"approval_id"`
	RequestID    string  `json:"request_id"`
	TaskID       string  `json:"task_id"`
	NodeID       string  `json:"node_id"`
	Status       string  `json:"status"`
	TTLSec       int     `json:"ttl_sec"`
	CreatedAt    string  `json:"created_at"`
	DecidedAt    *string `json:"decided_at,omitempty"`
	DecidedBy    *string `json:"decided_by,omitempty"`
	DecisionNote *string `json:"decision_note,omitempty"`
}

type SpendTransactionRecord struct {
	TxID        string  `json:"tx_id"`
	OwnerUserID string  `json:"owner_user_id"`
	TaskID      string  `json:"task_id"`
	NodeID      string  `json:"node_id"`
	ServiceID   string  `json:"service_id"`
	AmountUSD   float64 `json:"amount_usd"`
	CreatedAt   string  `json:"created_at"`
}

type AuditEventRecord struct {
	EventID     string `json:"event_id"`
	EventType   string `json:"event_type"`
	EntityType  string `json:"entity_type"`
	EntityID    string `json:"entity_id"`
	ActorID     string `json:"actor_id,omitempty"`
	PayloadJSON string `json:"payload_json,omitempty"`
	CreatedAt   string `json:"created_at"`
}

type AuditEventFilter struct {
	EventType  string
	EntityType string
	EntityID   string
	ActorID    string
	Limit      int
}

type ExecutableNode struct {
	WorkspaceID  string `json:"workspace_id,omitempty"`
	TaskID       string `json:"task_id"`
	NodeID       string `json:"node_id"`
	NodeType     string `json:"node_type"`
	Status       string `json:"status"`
	AttemptCount int    `json:"attempt_count"`
	OwnerUserID  string `json:"owner_user_id"`
	Priority     string `json:"priority"`
}

func NewStore(dbPath string) (*Store, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, errors.New("db path cannot be empty")
	}

	filesystemPath, fileBacked, err := sqliteFilesystemPath(dbPath)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(filesystemPath)
	if fileBacked && dir != "." && dir != "" {
		if err := fssecure.EnsurePrivateParentDir(dir); err != nil {
			return nil, fmt.Errorf("create db directory: %w", err)
		}
	}
	if fileBacked {
		file, err := fssecure.OpenPrivateFile(filesystemPath, os.O_CREATE|os.O_RDWR)
		if err != nil {
			return nil, fmt.Errorf("secure sqlite db file: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close secured sqlite db file: %w", err)
		}
	}

	db, err := sql.Open("sqlite", sqliteDSNWithPragmas(dbPath,
		"busy_timeout=5000",
		"foreign_keys=ON",
	))
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	writeDB, err := sql.Open("sqlite", sqliteDSNWithPragmas(dbPath,
		"busy_timeout=5000",
		"foreign_keys=ON",
	))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite write db: %w", err)
	}

	// Pool settings for concurrent WAL usage (Readers)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0)

	// Pool settings for Writers (Strictly 1 connection to prevent SQLITE_BUSY)
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}
	if err := writeDB.PingContext(ctx); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("ping sqlite write db: %w", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA auto_vacuum=INCREMENTAL;"); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("set auto_vacuum: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}
	if _, err := writeDB.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("set WAL mode (write): %w", err)
	}
	if _, err := writeDB.ExecContext(ctx, "PRAGMA auto_vacuum=INCREMENTAL;"); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("set auto_vacuum (write): %w", err)
	}
	if _, err := writeDB.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("enable foreign keys (write): %w", err)
	}
	if fileBacked {
		for _, path := range []string{filesystemPath, filesystemPath + "-wal", filesystemPath + "-shm"} {
			if err := fssecure.RestrictExistingFile(path); err != nil {
				_ = db.Close()
				_ = writeDB.Close()
				return nil, fmt.Errorf("secure sqlite file %s: %w", path, err)
			}
		}
	}

	authorityDBPath := dbPath
	if fileBacked {
		authorityDBPath = filesystemPath
	}
	s := &Store{
		db:                      db,
		writeDB:                 writeDB,
		dbPath:                  dbPath,
		authorityIdentityPath:   authorityIdentityPathForDB(authorityDBPath),
		authorityBootInstanceID: nextID("authboot"),
	}

	jCore := &journalCore{db: db, writeDB: writeDB, store: s}
	mCore := &memoryCore{db: db, writeDB: writeDB, journal: jCore, store: s}
	cCore := &coordinationCore{db: db, writeDB: writeDB, memory: mCore, store: s}
	sCore := &statCore{db: db, writeDB: writeDB, memory: mCore, store: s}

	s.JournalKernel = jCore
	s.MemoryKernel = mCore
	s.CoordinationKernel = cCore
	s.StatKernel = sCore

	s.rspFirehose = NewRSPFirehose(s)
	// We use Background because the firehose runs for the lifetime of the Store.
	s.rspFirehose.Start(context.Background())

	return s, nil
}

func sqliteFilesystemPath(dbPath string) (string, bool, error) {
	raw := strings.TrimSpace(dbPath)
	if raw == ":memory:" {
		return "", false, nil
	}
	if !strings.HasPrefix(strings.ToLower(raw), "file:") {
		return raw, true, nil
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false, fmt.Errorf("parse sqlite file URI: %w", err)
	}
	if strings.EqualFold(parsed.Query().Get("mode"), "memory") {
		return "", false, nil
	}
	if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
		return "", false, fmt.Errorf("sqlite file URI host %q is unsupported", parsed.Host)
	}
	path := parsed.Path
	if path == "" {
		path, err = url.PathUnescape(parsed.Opaque)
		if err != nil {
			return "", false, fmt.Errorf("decode sqlite file URI path: %w", err)
		}
	}
	if path == ":memory:" {
		return "", false, nil
	}
	if runtime.GOOS == "windows" && len(path) >= 3 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	path = filepath.FromSlash(path)
	if strings.TrimSpace(path) == "" {
		return "", false, errors.New("sqlite file URI path cannot be empty")
	}
	return path, true, nil
}

func sqliteDSNWithPragmas(dbPath string, pragmas ...string) string {
	query := url.Values{}
	query.Set("_txlock", "immediate")
	for _, pragma := range pragmas {
		pragma = strings.TrimSpace(pragma)
		if pragma == "" {
			continue
		}
		query.Add("_pragma", pragma)
	}
	if len(query) == 0 {
		return dbPath
	}

	separator := "?"
	switch {
	case strings.Contains(dbPath, "?") && (strings.HasSuffix(dbPath, "?") || strings.HasSuffix(dbPath, "&")):
		separator = ""
	case strings.Contains(dbPath, "?"):
		separator = "&"
	}
	return dbPath + separator + query.Encode()
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if s.rspFirehose != nil {
		if err := s.rspFirehose.Stop(); err != nil {
			return fmt.Errorf("rsp firehose stop: %w", err)
		}
	}
	var errs []string
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, "read db close: "+err.Error())
		}
	}
	if s.writeDB != nil {
		if err := s.writeDB.Close(); err != nil {
			errs = append(errs, "write db close: "+err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (s *Store) PruneMaintenance(ctx context.Context, rpcRetention time.Duration) (int64, error) {
	threshold := time.Now().UTC().Add(-rpcRetention).Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin prune tx: %w", err)
	}

	res, err := tx.ExecContext(ctx, `DELETE FROM rpc_access_log WHERE created_at < ?`, threshold)
	if err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("delete old rpc logs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit prune tx: %w", err)
	}

	deleted, _ := res.RowsAffected()

	// Incrementally vacuum up to 100 freed pages (~400KB) at a time
	if _, err := s.writeDB.ExecContext(ctx, `PRAGMA incremental_vacuum(100);`); err != nil {
		return deleted, fmt.Errorf("incremental vacuum: %w", err)
	}

	return deleted, nil
}

// Temporary wrapper during Phase 4A migration to satisfy internal calls outside journalCore without massive package disruption.
func (s *Store) addAuditEventTx(ctx context.Context, tx *sql.Tx, input AuditEventInput) error {
	return s.JournalKernel.(*journalCore).addAuditEventTx(ctx, tx, input)
}

func (s *Store) ApplyMigrations(ctx context.Context) error {
	files, err := migrationFileNames()
	if err != nil {
		return fmt.Errorf("list migration files: %w", err)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin migration lock tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := s.ensureMigrationTableTx(ctx, tx); err != nil {
		return err
	}

	for _, file := range files {
		applied, err := s.isMigrationAppliedTx(ctx, tx, file)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		sqlText, err := migrationSQL(file)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}

		if _, err := tx.ExecContext(ctx, sqlText); err != nil {
			return fmt.Errorf("execute migration %s: %w", file, err)
		}

		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
			file,
			time.Now().UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("record migration %s: %w", file, err)
		}
	}

	if err := s.bootstrapWorkspaceSecuritySettingsTx(ctx, tx); err != nil {
		return fmt.Errorf("bootstrap workspace security settings: %w", err)
	}
	if err := s.backfillProjectPatchQueueMaterializationAuthorityProofsTx(ctx, tx); err != nil {
		return fmt.Errorf("backfill project patch queue materialization authority proofs: %w", err)
	}
	if err := s.backfillProjectPatchQueueDecisionContinuationsTx(ctx, tx); err != nil {
		return fmt.Errorf("backfill project patch queue decision continuations: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration lock tx: %w", err)
	}
	committed = true
	return nil
}

func (s *Store) ensureMigrationTableTx(ctx context.Context, tx *sql.Tx) error {
	const query = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL
);`
	_, err := tx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	return nil
}

func (s *Store) isMigrationAppliedTx(ctx context.Context, tx *sql.Tx, version string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(
		ctx,
		"SELECT COUNT(1) FROM schema_migrations WHERE version = ?",
		version,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("check migration %s: %w", version, err)
	}
	return count > 0, nil
}

func (s *Store) bootstrapWorkspaceSecuritySettingsTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT workspace_id FROM workspaces ORDER BY workspace_id`)
	if err != nil {
		return fmt.Errorf("query workspaces for security bootstrap: %w", err)
	}

	var workspaceIDs []string
	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan workspace for security bootstrap: %w", err)
		}
		workspaceIDs = append(workspaceIDs, workspaceID)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close workspace security bootstrap rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate workspaces for security bootstrap: %w", err)
	}

	for _, workspaceID := range workspaceIDs {
		if err := s.ensureWorkspaceAuthSettingsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) CreateTaskWithGraph(ctx context.Context, input TaskCreateInput, graph dag.Graph) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin task tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.createTaskWithGraphTx(ctx, tx, input, graph, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit task tx: %w", err)
	}
	return nil
}

func (s *Store) CreateTaskWithGraphAndWorkspaceEvent(ctx context.Context, input TaskCreateInput, graph dag.Graph, attachment TaskAttachmentInput, eventInput RuntimeEventInput) (RuntimeEventRecord, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin task event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if strings.TrimSpace(input.WorkspaceID) == "" {
		input.WorkspaceID = strings.TrimSpace(attachment.WorkspaceID)
	}
	if err := s.createTaskWithGraphTx(ctx, tx, input, graph, now); err != nil {
		return RuntimeEventRecord{}, err
	}
	if err := s.attachTaskToWorkspaceTx(ctx, tx, attachment, now); err != nil {
		return RuntimeEventRecord{}, err
	}
	if err := s.addWorkspaceTaskDependencyLinksTx(ctx, tx, WorkspaceTaskDependencyLinksInput{
		WorkspaceID:       attachment.WorkspaceID,
		TaskID:            input.TaskID,
		DependencyTaskIDs: input.DependencyTaskIDs,
		CreatedBy:         attachment.LinkedBy,
	}, now); err != nil {
		return RuntimeEventRecord{}, err
	}
	if err := s.addWorkspaceTaskRelatedLinksTx(ctx, tx, WorkspaceTaskRelatedLinksInput{
		WorkspaceID:    attachment.WorkspaceID,
		TaskID:         input.TaskID,
		RelatedTaskIDs: input.RelatedTaskIDs,
		CreatedBy:      attachment.LinkedBy,
	}, now); err != nil {
		return RuntimeEventRecord{}, err
	}
	if strings.TrimSpace(eventInput.CreatedAt) == "" {
		eventInput.CreatedAt = now
	}
	runtimeEvent, err := s.appendRuntimeEventTx(ctx, tx, eventInput)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit task event tx: %w", err)
	}
	return runtimeEvent, nil
}

// stampDecisivePathCarrierKindJSON writes the decisive-path carrier kind into a task's requirements JSON
// under "decisive_path_kind" at the single creation chokepoint. An empty kind leaves the JSON unchanged
// (an ordinary, non-gated task). This is the birth-site stamp the decisive-path primitive (root A) reads:
// every born-active/pending owner-bound carrier declares its kind here so decisivePathRoute can classify
// it, and the route's fail-closed default catches an unrecognized kind instead of looping.
func stampDecisivePathCarrierKindJSON(requirementsJSON, carrierKind string) string {
	kind := decisivePathNormalizeKind(carrierKind)
	if kind == "" {
		return requirementsJSON
	}
	fields := map[string]any{}
	if trimmed := strings.TrimSpace(requirementsJSON); trimmed != "" && trimmed != "null" {
		if err := json.Unmarshal([]byte(trimmed), &fields); err != nil || fields == nil {
			fields = map[string]any{}
		}
	}
	fields["decisive_path_kind"] = kind
	encoded, err := json.Marshal(fields)
	if err != nil {
		return requirementsJSON
	}
	return string(encoded)
}

func (s *Store) createTaskWithGraphTx(ctx context.Context, tx *sql.Tx, input TaskCreateInput, graph dag.Graph, now string) error {
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return errors.New("task_id is required")
	}

	owner := strings.TrimSpace(input.OwnerUserID)
	if owner == "" {
		return errors.New("owner_user_id is required")
	}

	priority := normalizePriority(input.Priority)
	if !validPriority(priority) {
		return fmt.Errorf("invalid priority: %s", input.Priority)
	}
	taskKind, err := normalizeStoredTaskKind(input.TaskKind)
	if err != nil {
		return err
	}
	taskTemplate := strings.TrimSpace(input.TaskTemplate)
	if taskTemplate == "" {
		taskTemplate = model.TaskTemplateGeneric
	}
	if _, ok := model.LookupTaskTemplate(taskTemplate); !ok {
		return fmt.Errorf("invalid task template: %s", taskTemplate)
	}
	if isLegacyTaskKind(taskKind) && !model.ValidTaskTemplateForKind(taskTemplate, taskKind) {
		return fmt.Errorf("task template %s does not support task kind %s", taskTemplate, taskKind)
	}
	projectID := taskSubmitStoragePatchQueueProjectIDForCreateInput(input)
	if projectID != strings.TrimSpace(input.ProjectID) {
		input.ProjectID = projectID
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if projectID != "" {
		if err := s.ensureTaskProjectInWorkspaceTx(ctx, tx, workspaceID, projectID); err != nil {
			return err
		}
	}
	if err := s.enforceTaskSubmitPatchQueueGateTx(ctx, tx, input); err != nil {
		return err
	}
	if err := s.enforceTaskSubmitIntegratedAcceptanceCoverageGateTx(ctx, tx, input); err != nil {
		return err
	}
	projectLane := normalizeProjectLane(input.ProjectLane)
	taskClass, taskClassSource, taskClassUpdatedAt, err := normalizeTaskClassEvidence(
		strings.TrimSpace(input.TaskClass),
		strings.TrimSpace(input.TaskClassSource),
	)
	if err != nil {
		return err
	}

	now = strings.TrimSpace(now)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if taskClassUpdatedAt == "" && taskClass != "" {
		taskClassUpdatedAt = now
	}

	tagsJSON := "[]"
	if len(input.Tags) > 0 {
		b, _ := json.Marshal(input.Tags)
		tagsJSON = string(b)
	}
	taskRequirementsJSON := normalizeTaskRequirementsJSON(input.TaskRequirementsJSON)
	writeScopeHints, normalizedRequirementsJSON := normalizeTaskCreateSemanticWriteScopeHints(input, taskID, taskRequirementsJSON)
	taskRequirementsJSON = normalizedRequirementsJSON
	// Decisive-path primitive (root A, stage 4): stamp the carrier kind at the single creation chokepoint
	// so every born-active/pending owner-bound carrier is route-classifiable by decisivePathRoute.
	taskRequirementsJSON = stampDecisivePathCarrierKindJSON(taskRequirementsJSON, input.CarrierKind)
	writeScopeHintsJSON := encodeTaskWriteScopeHintsJSON(writeScopeHints)

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO tasks(task_id, owner_user_id, priority, status, title, description, task_kind, task_template, task_class, task_class_source, task_class_updated_at, tags_json, project_id, project_lane, requires_project_gate, task_requirements_json, write_scope_hints_json, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID,
		owner,
		priority,
		taskStatusPending,
		strings.TrimSpace(input.Title),
		strings.TrimSpace(input.Description),
		taskKind,
		taskTemplate,
		taskClass,
		taskClassSource,
		taskClassUpdatedAt,
		tagsJSON,
		projectID,
		projectLane,
		boolToSQLiteInt(input.RequiresProjectGate),
		taskRequirementsJSON,
		writeScopeHintsJSON,
		now,
		now,
	)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	if err := s.upsertTaskPatchQueueIdentityTx(ctx, tx, input, now); err != nil {
		return err
	}

	for _, node := range graph.Nodes {
		nodeType := strings.TrimSpace(node.Type)
		if nodeType == "" {
			nodeType = "generic"
		}

		status := nodeStatusPending
		if len(node.DependsOn) > 0 {
			status = nodeStatusBlocked
		}

		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO dag_nodes(node_id, task_id, node_type, status, attempt_count, last_error, created_at, updated_at)
			 VALUES (?, ?, ?, ?, 0, NULL, ?, ?)`,
			node.NodeID,
			taskID,
			nodeType,
			status,
			now,
			now,
		)
		if err != nil {
			return fmt.Errorf("insert node %s: %w", node.NodeID, err)
		}

		for _, dep := range node.DependsOn {
			_, err = tx.ExecContext(
				ctx,
				`INSERT INTO node_dependencies(task_id, node_id, depends_on_node_id)
				 VALUES (?, ?, ?)`,
				taskID,
				node.NodeID,
				dep,
			)
			if err != nil {
				return fmt.Errorf("insert dependency %s -> %s: %w", node.NodeID, dep, err)
			}
		}
	}
	return nil
}

func normalizeTaskCreateSemanticWriteScopeHints(input TaskCreateInput, taskID, taskRequirementsJSON string) ([]string, string) {
	paths := normalizeStringSlice(input.WriteScopeHints)
	if len(paths) == 0 {
		return paths, taskRequirementsJSON
	}
	task := WorkspaceTaskRecord{
		TaskID:               strings.TrimSpace(taskID),
		Title:                strings.TrimSpace(input.Title),
		Description:          strings.TrimSpace(input.Description),
		TaskKind:             strings.TrimSpace(input.TaskKind),
		ProjectID:            strings.TrimSpace(input.ProjectID),
		ProjectLane:          strings.TrimSpace(input.ProjectLane),
		RequiresProjectGate:  input.RequiresProjectGate,
		TaskRequirementsJSON: taskRequirementsJSON,
		WriteScopeHints:      paths,
		Tags:                 append([]string(nil), input.Tags...),
	}
	if agentWorkTaskPreservesExplicitWriteScopeHints(task) {
		return paths, taskRequirementsJSON
	}
	narrowed := agentWorkLuaAcceptanceWriteScopeHints(task)
	if len(narrowed) == 0 || !agentWorkLuaAcceptanceScopeShouldNarrow(paths, narrowed) {
		return paths, taskRequirementsJSON
	}
	return narrowed, taskRequirementsJSONWithWriteScopeHints(taskRequirementsJSON, narrowed)
}

func taskRequirementsJSONWithWriteScopeHints(raw string, hints []string) string {
	hints = normalizeStringSlice(hints)
	raw = normalizeTaskRequirementsJSON(raw)
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload == nil {
		payload = map[string]any{}
	}
	if len(hints) == 0 {
		delete(payload, "write_scope_hints")
	} else {
		payload["write_scope_hints"] = hints
		if _, ok := payload["schema"]; !ok {
			payload["schema"] = "task_requirements.v1"
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return raw
	}
	return string(encoded)
}

// resolveTaskIDWithQuerier canonicalizes a (possibly case/alias-variant) task_id to the stored
// task_id within a workspace, mirroring resolveProjectIDWithQuerier (CR-21 / SA-3). task_id is global
// in `tasks` and scoped per workspace via workspace_tasks, so resolution is workspace-scoped. Exact
// match wins first (zero behavior change for correctly-cased ids); a case-insensitive fallback recovers
// an aliased id and fails closed on ambiguity. Callers use it best-effort: on any error they keep the
// original id and let the downstream exact lookup surface the real not-found error.
func (s *Store) resolveTaskIDWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID, taskID string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" || taskID == "" {
		return "", errors.New("workspace_id and task_id are required")
	}
	var canonical string
	err := q.QueryRowContext(ctx,
		`SELECT task_id FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`,
		workspaceID, taskID,
	).Scan(&canonical)
	if err == nil {
		return canonical, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("resolve task_id: %w", err)
	}
	rows, err := q.QueryContext(ctx,
		`SELECT task_id
		   FROM workspace_tasks
		  WHERE workspace_id = ? AND lower(task_id) = lower(?)
		  ORDER BY task_id ASC
		  LIMIT 2`,
		workspaceID, taskID,
	)
	if err != nil {
		return "", fmt.Errorf("resolve task_id case alias: %w", err)
	}
	defer rows.Close()
	var matches []string
	for rows.Next() {
		var match string
		if err := rows.Scan(&match); err != nil {
			return "", fmt.Errorf("scan task_id case alias: %w", err)
		}
		matches = append(matches, strings.TrimSpace(match))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("resolve task_id case alias: %w", err)
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("resolve task_id: %w", sql.ErrNoRows)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("task_id %q is ambiguous in workspace %s", taskID, workspaceID)
	}
}

func (s *Store) GetTaskStatus(ctx context.Context, workspaceID, taskID string) (TaskStatus, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return TaskStatus{}, errors.New("task_id is required")
	}
	if workspaceID != "" {
		if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, s.db, workspaceID, taskID); rerr == nil {
			taskID = resolved
		}
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, nil, workspaceID, taskID); err != nil {
			return TaskStatus{}, err
		}
	}

	var out TaskStatus
	var requiresProjectGate int
	var taskRequirementsJSON, writeScopeHintsJSON string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT task_id, title, description, owner_user_id, priority, status, task_kind, task_template, COALESCE(task_class, ''), COALESCE(task_class_source, ''), COALESCE(task_class_updated_at, ''), COALESCE(project_id, ''), COALESCE(project_lane, ''), COALESCE(requires_project_gate, 0), COALESCE(task_requirements_json, '{}'), COALESCE(write_scope_hints_json, '[]'), created_at, updated_at
		 FROM tasks
		 WHERE task_id = ?`,
		taskID,
	).Scan(
		&out.TaskID,
		&out.Title,
		&out.Description,
		&out.OwnerUserID,
		&out.Priority,
		&out.Status,
		&out.TaskKind,
		&out.TaskTemplate,
		&out.TaskClass,
		&out.TaskClassSource,
		&out.TaskClassUpdatedAt,
		&out.ProjectID,
		&out.ProjectLane,
		&requiresProjectGate,
		&taskRequirementsJSON,
		&writeScopeHintsJSON,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TaskStatus{}, ErrTaskNotFound
		}
		return TaskStatus{}, fmt.Errorf("query task status: %w", err)
	}
	out.RequiresProjectGate = sqliteIntToBool(requiresProjectGate)
	out.TaskRequirementsJSON = normalizeTaskRequirementsJSON(taskRequirementsJSON)
	out.WriteScopeHints = parseTaskWriteScopeHintsJSON(writeScopeHintsJSON)

	out.NodeCounts = map[string]int{}

	countRows, err := s.db.QueryContext(
		ctx,
		`SELECT status, COUNT(1)
		 FROM dag_nodes
		 WHERE task_id = ?
		 GROUP BY status`,
		taskID,
	)
	if err != nil {
		return TaskStatus{}, fmt.Errorf("query node counts: %w", err)
	}
	defer countRows.Close()

	for countRows.Next() {
		var status string
		var count int
		if err := countRows.Scan(&status, &count); err != nil {
			return TaskStatus{}, fmt.Errorf("scan node count: %w", err)
		}
		out.NodeCounts[status] = count
	}
	if err := countRows.Err(); err != nil {
		return TaskStatus{}, fmt.Errorf("iterate node counts: %w", err)
	}

	depsByNode := map[string][]string{}
	depRows, err := s.db.QueryContext(
		ctx,
		`SELECT node_id, depends_on_node_id
		 FROM node_dependencies
		 WHERE task_id = ?
		 ORDER BY node_id, depends_on_node_id`,
		taskID,
	)
	if err != nil {
		return TaskStatus{}, fmt.Errorf("query node dependencies: %w", err)
	}
	defer depRows.Close()

	for depRows.Next() {
		var nodeID, dependsOn string
		if err := depRows.Scan(&nodeID, &dependsOn); err != nil {
			return TaskStatus{}, fmt.Errorf("scan dependency: %w", err)
		}
		depsByNode[nodeID] = append(depsByNode[nodeID], dependsOn)
	}
	if err := depRows.Err(); err != nil {
		return TaskStatus{}, fmt.Errorf("iterate dependencies: %w", err)
	}

	nodeRows, err := s.db.QueryContext(
		ctx,
		`SELECT node_id, node_type, status, attempt_count, last_error
		 FROM dag_nodes
		 WHERE task_id = ?
		 ORDER BY node_id`,
		taskID,
	)
	if err != nil {
		return TaskStatus{}, fmt.Errorf("query nodes: %w", err)
	}
	defer nodeRows.Close()

	for nodeRows.Next() {
		var node TaskStatusNode
		var lastErr sql.NullString
		if err := nodeRows.Scan(
			&node.NodeID,
			&node.Type,
			&node.Status,
			&node.AttemptCount,
			&lastErr,
		); err != nil {
			return TaskStatus{}, fmt.Errorf("scan node: %w", err)
		}

		if lastErr.Valid {
			v := lastErr.String
			node.LastError = &v
		}

		node.DependsOn = depsByNode[node.NodeID]
		out.Nodes = append(out.Nodes, node)
	}
	if err := nodeRows.Err(); err != nil {
		return TaskStatus{}, fmt.Errorf("iterate nodes: %w", err)
	}

	return out, nil
}

func (s *Store) ResolveSingleTaskWorkspace(ctx context.Context, taskID string) (string, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", errors.New("task_id is required")
	}

	var workspaceID sql.NullString
	var workspaceCount int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT MIN(workspace_id), COUNT(1)
		   FROM workspace_tasks
		  WHERE task_id = ?`,
		taskID,
	).Scan(&workspaceID, &workspaceCount); err != nil {
		return "", fmt.Errorf("resolve task workspace: %w", err)
	}

	switch {
	case workspaceCount == 0:
		return "", ErrWorkspaceTaskAbsent
	case workspaceCount > 1:
		return "", ErrTaskWorkspaceAmbiguous
	default:
		return strings.TrimSpace(workspaceID.String), nil
	}
}

func (s *Store) PutTaskClassEvidence(ctx context.Context, input TaskClassEvidencePutInput) (TaskStatus, error) {
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return TaskStatus{}, errors.New("task_id is required")
	}
	taskClass, taskClassSource, taskClassUpdatedAt, err := normalizeTaskClassEvidence(
		strings.TrimSpace(input.TaskClass),
		strings.TrimSpace(input.TaskClassSource),
	)
	if err != nil {
		return TaskStatus{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if taskClassUpdatedAt == "" && taskClass != "" {
		taskClassUpdatedAt = now
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TaskStatus{}, fmt.Errorf("begin task class tx: %w", err)
	}

	if input.WorkspaceID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, input.WorkspaceID, taskID); err != nil {
			_ = tx.Rollback()
			return TaskStatus{}, err
		}
	}

	var previousClass, previousSource string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(task_class, ''), COALESCE(task_class_source, '') FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&previousClass, &previousSource); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return TaskStatus{}, ErrTaskNotFound
		}
		return TaskStatus{}, fmt.Errorf("query task class evidence: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE tasks
		    SET task_class = ?,
		        task_class_source = ?,
		        task_class_updated_at = ?,
		        updated_at = ?
		  WHERE task_id = ?`,
		taskClass,
		taskClassSource,
		taskClassUpdatedAt,
		now,
		taskID,
	); err != nil {
		_ = tx.Rollback()
		return TaskStatus{}, fmt.Errorf("update task class evidence: %w", err)
	}

	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "task.class.put"
	}
	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "task_class_evidence_put",
		EntityType: "task",
		EntityID:   taskID,
		ActorID:    actorID,
		PayloadJSON: mustJSON(map[string]any{
			"task_id":                    taskID,
			"task_class":                 taskClass,
			"task_class_source":          taskClassSource,
			"task_class_updated_at":      taskClassUpdatedAt,
			"previous_task_class":        strings.TrimSpace(previousClass),
			"previous_task_class_source": strings.TrimSpace(previousSource),
		}),
	}); err != nil {
		_ = tx.Rollback()
		return TaskStatus{}, err
	}

	if err := tx.Commit(); err != nil {
		return TaskStatus{}, fmt.Errorf("commit task class tx: %w", err)
	}
	return s.GetTaskStatus(ctx, input.WorkspaceID, taskID)
}

func (s *Store) UpdateTaskProjectFields(ctx context.Context, input TaskProjectFieldsUpdateInput) (TaskStatus, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return TaskStatus{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return TaskStatus{}, errors.New("task_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TaskStatus{}, fmt.Errorf("begin task project fields tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.updateTaskProjectFieldsTx(ctx, tx, input, now); err != nil {
		return TaskStatus{}, err
	}
	if err := tx.Commit(); err != nil {
		return TaskStatus{}, fmt.Errorf("commit task project fields tx: %w", err)
	}
	return s.GetTaskStatus(ctx, workspaceID, taskID)
}

func (s *Store) UpdateTaskProjectFieldsWithRuntimeEvent(ctx context.Context, input TaskProjectFieldsUpdateInput, eventInput RuntimeEventInput) (TaskStatus, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return TaskStatus{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return TaskStatus{}, RuntimeEventRecord{}, errors.New("task_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return TaskStatus{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return TaskStatus{}, RuntimeEventRecord{}, fmt.Errorf("begin task project fields event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var runtimeEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.updateTaskProjectFieldsTx(ctx, tx, input, now); err != nil {
			return err
		}
		if strings.TrimSpace(eventInput.CreatedAt) == "" {
			eventInput.CreatedAt = now
		}
		if strings.TrimSpace(eventInput.WorkspaceID) == "" {
			eventInput.WorkspaceID = workspaceID
		}
		if strings.TrimSpace(eventInput.EntityID) == "" {
			eventInput.EntityID = taskID
		}
		if strings.TrimSpace(eventInput.TaskID) == "" {
			eventInput.TaskID = taskID
		}
		appended, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, eventInput)
		if err != nil {
			return err
		}
		runtimeEvent = appended
		return nil
	}); err != nil {
		return TaskStatus{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return TaskStatus{}, RuntimeEventRecord{}, fmt.Errorf("commit task project fields event tx: %w", err)
	}
	status, err := s.GetTaskStatus(ctx, workspaceID, taskID)
	if err != nil {
		return TaskStatus{}, RuntimeEventRecord{}, err
	}
	return status, runtimeEvent, nil
}

func (s *Store) updateTaskProjectFieldsTx(ctx context.Context, tx *sql.Tx, input TaskProjectFieldsUpdateInput, now string) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return errors.New("task_id is required")
	}

	if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
		return err
	}
	if err := s.ensureTaskProjectSingleWorkspaceTx(ctx, tx, workspaceID, taskID); err != nil {
		return err
	}

	var projectID, taskKind, projectLane string
	var requiresProjectGate int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(project_id, ''), task_kind, COALESCE(project_lane, ''), COALESCE(requires_project_gate, 0)
		 FROM tasks
		 WHERE task_id = ?`,
		taskID,
	).Scan(&projectID, &taskKind, &projectLane, &requiresProjectGate); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("query task project fields: %w", err)
	}
	previousProjectLane := normalizeProjectLane(projectLane)
	previousRequiresProjectGate := requiresProjectGate

	if input.ProjectID != nil {
		projectID = strings.TrimSpace(*input.ProjectID)
	}
	if projectID != "" {
		if err := s.ensureTaskProjectInWorkspaceTx(ctx, tx, workspaceID, projectID); err != nil {
			return err
		}
	}
	if input.TaskKind != nil {
		normalized, err := normalizeStoredTaskKind(*input.TaskKind)
		if err != nil {
			return err
		}
		taskKind = normalized
	} else {
		normalized, err := normalizeStoredTaskKind(taskKind)
		if err != nil {
			return err
		}
		taskKind = normalized
	}
	if input.ProjectLane != nil {
		projectLane = normalizeProjectLane(*input.ProjectLane)
	} else {
		projectLane = normalizeProjectLane(projectLane)
	}
	if input.RequiresProjectGate != nil {
		requiresProjectGate = boolToSQLiteInt(*input.RequiresProjectGate)
	}
	if err := s.guardTaskProjectLaneGateMutationTx(ctx, tx, workspaceID, projectID, taskID, strings.TrimSpace(input.ActorID), previousProjectLane, previousRequiresProjectGate, projectLane, requiresProjectGate, now); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE tasks
		    SET project_id = ?,
		        task_kind = ?,
		        project_lane = ?,
		        requires_project_gate = ?,
		        updated_at = ?
		  WHERE task_id = ?`,
		projectID,
		taskKind,
		projectLane,
		requiresProjectGate,
		now,
		taskID,
	); err != nil {
		return fmt.Errorf("update task project fields: %w", err)
	}

	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = "task.project_fields.update"
	}
	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "task_project_fields_updated",
		EntityType: "task",
		EntityID:   taskID,
		ActorID:    actorID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":          workspaceID,
			"task_id":               taskID,
			"project_id":            projectID,
			"task_kind":             taskKind,
			"project_lane":          projectLane,
			"requires_project_gate": sqliteIntToBool(requiresProjectGate),
		}),
	}); err != nil {
		return err
	}

	return nil
}

func (s *Store) guardTaskProjectLaneGateMutationTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, taskID, actorID, previousProjectLane string, previousRequiresProjectGate int, nextProjectLane string, nextRequiresProjectGate int, now string) error {
	if normalizeProjectLane(previousProjectLane) == normalizeProjectLane(nextProjectLane) && previousRequiresProjectGate == nextRequiresProjectGate {
		return nil
	}
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return fmt.Errorf("%w: project_lane/requires_project_gate mutation for task %s requires project_id", ErrProjectLeadRequired, strings.TrimSpace(taskID))
	}
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return fmt.Errorf("%w: project_lane/requires_project_gate mutation for task %s requires actor_id matching the active project strategic lead", ErrProjectLeadMismatch, strings.TrimSpace(taskID))
	}
	if err := s.expireProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now); err != nil {
		return err
	}
	lead, ok, err := s.getActiveProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: project_lane/requires_project_gate mutation for task %s requires active project strategic lead for project %s", ErrProjectLeadRequired, strings.TrimSpace(taskID), projectID)
	}
	if strings.TrimSpace(lead.AgentID) != actorID {
		return fmt.Errorf("%w: actor %s is not active project strategic lead for project %s", ErrProjectLeadMismatch, actorID, projectID)
	}
	hasActiveClaim, err := taskProjectFieldsTaskHasActiveClaimTx(ctx, tx, workspaceID, taskID)
	if err != nil {
		return err
	}
	if hasActiveClaim {
		return fmt.Errorf("%w: task %s has an active claim", ErrTaskProjectFieldsBindingActive, strings.TrimSpace(taskID))
	}
	hasBranchBinding, err := taskProjectFieldsTaskHasBranchBindingTx(ctx, tx, workspaceID, taskID)
	if err != nil {
		return err
	}
	if hasBranchBinding {
		return fmt.Errorf("%w: task %s has an active branch binding", ErrTaskProjectFieldsBindingActive, strings.TrimSpace(taskID))
	}
	return nil
}

func taskProjectFieldsTaskHasActiveClaimTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM task_claims
 WHERE workspace_id = ?
   AND task_id = ?
   AND claim_status IN (?, ?)`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID), model.TaskClaimStatusClaimed, model.TaskClaimStatusBlocked).Scan(&count); err != nil {
		return false, fmt.Errorf("check task project field active claim binding: %w", err)
	}
	return count > 0, nil
}

func taskProjectFieldsTaskHasBranchBindingTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_branch_registry
 WHERE workspace_id = ?
   AND (active_task_id = ? OR active_claim_id = ?)`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID), strings.TrimSpace(taskID)).Scan(&count); err != nil {
		return false, fmt.Errorf("check task project field branch binding: %w", err)
	}
	return count > 0, nil
}

func (s *Store) CloseTask(ctx context.Context, input TaskCloseInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	taskID := strings.TrimSpace(input.TaskID)
	actorID := strings.TrimSpace(input.ActorID)
	resolution := strings.TrimSpace(input.Resolution)
	if resolution == "" {
		resolution = model.TaskStatusResolved
	}
	if workspaceID == "" {
		return errors.New("workspace_id is required for authority-backed task close")
	}
	if taskID == "" {
		return errors.New("task_id is required")
	}
	if actorID == "" {
		return errors.New("actor_id is required for authority-backed task close")
	}
	payload, err := AttachTaskPromptContextEnvelope(map[string]any{
		"workspace_id": workspaceID,
		"task_id":      taskID,
		"actor_id":     actorID,
		"resolution":   resolution,
		"reason":       strings.TrimSpace(input.Reason),
		"status":       resolution,
	}, BuildTaskPromptContextEnvelope("task.close", "cli_local", workspaceID, "operator", actorID))
	if err != nil {
		return err
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal task close runtime payload: %w", err)
	}
	_, _, err = s.CloseTaskWithRuntimeEvent(ctx, input, RuntimeEventInput{
		DedupKey:    "task:" + taskID + ":closed:" + resolution,
		WorkspaceID: workspaceID,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "operator",
		ActorID:     actorID,
		TaskID:      taskID,
		PayloadJSON: string(payloadJSON),
	})
	return err
}

type taskCloseTxResult struct {
	changed           bool
	forceRuntimeEvent bool
}

func (s *Store) CloseTaskWithRuntimeEvent(ctx context.Context, input TaskCloseInput, eventInput RuntimeEventInput) (RuntimeEventRecord, bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, false, errors.New("workspace_id is required")
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, false, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, false, fmt.Errorf("begin close task event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var result taskCloseTxResult
	var runtimeEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		result, innerErr = s.closeTaskTx(ctx, tx, input, now)
		if innerErr != nil {
			return innerErr
		}
		if !result.changed {
			return nil
		}
		if result.forceRuntimeEvent {
			eventInput.DedupKey = ""
		}
		if strings.TrimSpace(eventInput.CreatedAt) == "" {
			eventInput.CreatedAt = now
		}
		if strings.TrimSpace(eventInput.WorkspaceID) == "" {
			eventInput.WorkspaceID = workspaceID
		}
		runtimeEvent, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, eventInput)
		return innerErr
	}); err != nil {
		return RuntimeEventRecord{}, false, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, false, fmt.Errorf("commit close task event tx: %w", err)
	}
	return runtimeEvent, result.changed, nil
}

func (s *Store) closeTaskTx(ctx context.Context, tx *sql.Tx, input TaskCloseInput, now string) (taskCloseTxResult, error) {
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return taskCloseTxResult{}, errors.New("task_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	resolution := strings.TrimSpace(input.Resolution)
	if resolution == "" {
		resolution = model.TaskStatusResolved
	}
	switch resolution {
	case model.TaskStatusResolved, model.TaskStatusFailed, model.TaskStatusCancelled:
	default:
		return taskCloseTxResult{}, fmt.Errorf("invalid task resolution: %s", resolution)
	}

	now = strings.TrimSpace(now)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}

	if workspaceID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
			return taskCloseTxResult{}, err
		}
	}

	var currentStatus, taskKind, projectID string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT status, task_kind, COALESCE(project_id, '') FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&currentStatus, &taskKind, &projectID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskCloseTxResult{}, ErrTaskNotFound
		}
		return taskCloseTxResult{}, fmt.Errorf("query task for close: %w", err)
	}
	if taskKind != model.TaskKindCoordination && resolution != model.TaskStatusCancelled {
		return taskCloseTxResult{}, fmt.Errorf("task %s is %s and cannot be closed manually (only cancellation allowed)", taskID, taskKind)
	}
	var claimSnapshot taskClaimTransitionSnapshot
	var claimSnapshotOK bool
	if workspaceID != "" {
		snapshot, ok, err := loadTaskClaimTransitionSnapshotTx(ctx, tx, taskID, workspaceID)
		if err != nil {
			return taskCloseTxResult{}, err
		}
		claimSnapshot = snapshot
		claimSnapshotOK = ok
	}
	if currentStatus == resolution {
		// tasks.status already matches, but task_claims might be out of sync — fix it
		claimStatus := taskResolutionToClaimStatus(resolution)
		result, err := tx.ExecContext(ctx,
			`UPDATE task_claims SET claim_status = ?, updated_at = ? WHERE task_id = ? AND claim_status != ?`,
			claimStatus, now, taskID, claimStatus,
		)
		if err != nil {
			return taskCloseTxResult{}, fmt.Errorf("sync task claim status on idempotent close: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return taskCloseTxResult{}, fmt.Errorf("sync task claim status on idempotent close rows affected: %w", err)
		}
		admissionChanged := false
		if workspaceID != "" && claimSnapshotOK {
			transition, err := clearTaskClaimProjectAdmissionTx(ctx, tx, workspaceID, taskID, claimSnapshot.AgentID, claimStatus, now, claimSnapshot, taskClaimProjectAdmissionClearOptions{})
			if err != nil {
				return taskCloseTxResult{}, err
			}
			admissionChanged = transition != nil
		}
		executionRunsChanged, err := closeTaskExecutionRunsTx(ctx, tx, workspaceID, taskID, resolution, now)
		if err != nil {
			return taskCloseTxResult{}, err
		}
		return taskCloseTxResult{
			changed:           affected > 0 || admissionChanged || executionRunsChanged,
			forceRuntimeEvent: affected > 0 || admissionChanged || executionRunsChanged,
		}, nil
	}
	if isTerminalTaskStatus(currentStatus) {
		return taskCloseTxResult{}, fmt.Errorf("task %s is already terminal with status %s", taskID, currentStatus)
	}
	if resolution == model.TaskStatusResolved {
		adm, err := s.evaluateTerminalAdmissionTx(ctx, tx, workspaceID, WorkspaceTaskRecord{TaskID: taskID}, TerminalWriteIntent{
			Side:       SideAdmission,
			Kind:       GenuineCompletion,
			Resolution: resolution,
			Origin:     OriginP03,
		})
		if err != nil {
			return taskCloseTxResult{}, err
		}
		if adm.Decision == TerminalReject {
			return taskCloseTxResult{}, adm.Err
		}
	}

	targetNodeStatus := mapTaskResolutionToNodeStatus(resolution)

	rows, err := tx.QueryContext(
		ctx,
		`SELECT node_id, status FROM dag_nodes WHERE task_id = ? ORDER BY node_id`,
		taskID,
	)
	if err != nil {
		return taskCloseTxResult{}, fmt.Errorf("query task nodes for close: %w", err)
	}
	type nodeState struct {
		nodeID  string
		current string
	}
	nodes := []nodeState{}
	for rows.Next() {
		var row nodeState
		if err := rows.Scan(&row.nodeID, &row.current); err != nil {
			_ = rows.Close()
			return taskCloseTxResult{}, fmt.Errorf("scan task node for close: %w", err)
		}
		nodes = append(nodes, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return taskCloseTxResult{}, fmt.Errorf("iterate task nodes for close: %w", err)
	}
	_ = rows.Close()

	for _, node := range nodes {
		if isTerminalNodeStatus(node.current) {
			continue
		}
		var lastError any
		if targetNodeStatus == model.NodeStatusFailed {
			lastError = strings.TrimSpace(input.Reason)
		}
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE dag_nodes SET status = ?, last_error = ?, updated_at = ? WHERE task_id = ? AND node_id = ?`,
			targetNodeStatus,
			lastError,
			now,
			taskID,
			node.nodeID,
		); err != nil {
			return taskCloseTxResult{}, fmt.Errorf("update coordination node status: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO node_state_transitions(
			  transition_id, task_id, node_id, from_status, to_status, reason, actor_id, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			nextID("node_transition"),
			taskID,
			node.nodeID,
			node.current,
			targetNodeStatus,
			strings.TrimSpace(input.Reason),
			strings.TrimSpace(input.ActorID),
			now,
		); err != nil {
			return taskCloseTxResult{}, fmt.Errorf("insert coordination node transition: %w", err)
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE tasks SET status = ?, close_reason = ?, updated_at = ? WHERE task_id = ?`,
		resolution,
		strings.TrimSpace(input.Reason),
		now,
		taskID,
	); err != nil {
		return taskCloseTxResult{}, fmt.Errorf("update coordination task status: %w", err)
	}

	claimStatus := taskResolutionToClaimStatus(resolution)
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE task_claims SET claim_status = ?, summary = ?, updated_at = ? WHERE task_id = ?`,
		claimStatus,
		strings.TrimSpace(input.Reason),
		now,
		taskID,
	); err != nil {
		return taskCloseTxResult{}, fmt.Errorf("update task claim status on close: %w", err)
	}
	if workspaceID != "" {
		if claimSnapshotOK {
			if _, err := clearTaskClaimProjectAdmissionTx(ctx, tx, workspaceID, taskID, claimSnapshot.AgentID, claimStatus, now, claimSnapshot, taskClaimProjectAdmissionClearOptions{}); err != nil {
				return taskCloseTxResult{}, err
			}
		}
		if err := s.resolveOpenOperatorQueuesForClosedTaskTx(ctx, tx, workspaceID, taskID, resolution, strings.TrimSpace(input.ActorID), now); err != nil {
			return taskCloseTxResult{}, err
		}
		if _, err := closeTaskExecutionRunsTx(ctx, tx, workspaceID, taskID, resolution, now); err != nil {
			return taskCloseTxResult{}, err
		}
		if strings.TrimSpace(projectID) != "" {
			if err := s.releaseProjectRolesIfNoOpenTasksTx(ctx, tx, workspaceID, strings.TrimSpace(projectID), strings.TrimSpace(input.ActorID), strings.TrimSpace(input.Reason), now); err != nil {
				return taskCloseTxResult{}, err
			}
		}
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "coordination_task_closed",
		EntityType: "task",
		EntityID:   taskID,
		ActorID:    strings.TrimSpace(input.ActorID),
		PayloadJSON: mustJSON(map[string]any{
			"task_id":     taskID,
			"from_status": currentStatus,
			"to_status":   resolution,
			"reason":      strings.TrimSpace(input.Reason),
		}),
	}); err != nil {
		return taskCloseTxResult{}, err
	}
	return taskCloseTxResult{changed: true}, nil
}

func closeTaskExecutionRunsTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, resolution, now string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" || taskID == "" {
		return false, nil
	}
	runStatus := taskResolutionToExecutionRunStatus(resolution)
	stepStatus := taskResolutionToExecutionStepStatus(resolution)
	runResult, err := tx.ExecContext(ctx, `
UPDATE execution_runs
   SET status = ?,
       outcome = ?,
       closed_at = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND COALESCE(task_id, '') = ?
   AND status IN ('PLANNED', 'ACTIVE', 'BLOCKED', 'VERIFYING')`,
		runStatus, runStatus, now, now, workspaceID, taskID)
	if err != nil {
		return false, fmt.Errorf("close execution runs on task close: %w", err)
	}
	stepResult, err := tx.ExecContext(ctx, `
UPDATE execution_steps
   SET status = ?,
       completed_at = COALESCE(completed_at, ?),
       updated_at = ?
 WHERE workspace_id = ?
   AND run_id IN (
       SELECT run_id
         FROM execution_runs
        WHERE workspace_id = ?
          AND COALESCE(task_id, '') = ?
   )
   AND status IN ('PENDING', 'ACTIVE', 'BLOCKED')`,
		stepStatus, now, now, workspaceID, workspaceID, taskID)
	if err != nil {
		return false, fmt.Errorf("close execution steps on task close: %w", err)
	}
	runAffected, err := runResult.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("close execution runs rows affected: %w", err)
	}
	stepAffected, err := stepResult.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("close execution steps rows affected: %w", err)
	}
	return runAffected > 0 || stepAffected > 0, nil
}

func releaseTaskExecutionRunsTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID, now string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || taskID == "" || agentID == "" {
		return false, nil
	}
	runResult, err := tx.ExecContext(ctx, `
UPDATE execution_runs
   SET status = 'CANCELLED',
       outcome = 'RELEASED',
       closed_at = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND COALESCE(task_id, '') = ?
   AND COALESCE(agent_id, '') = ?
   AND status IN ('PLANNED', 'ACTIVE', 'BLOCKED', 'VERIFYING')`,
		now, now, workspaceID, taskID, agentID)
	if err != nil {
		return false, fmt.Errorf("cancel execution runs on task release: %w", err)
	}
	stepResult, err := tx.ExecContext(ctx, `
UPDATE execution_steps
   SET status = 'CANCELLED',
       completed_at = COALESCE(completed_at, ?),
       updated_at = ?
 WHERE workspace_id = ?
   AND run_id IN (
       SELECT run_id
         FROM execution_runs
        WHERE workspace_id = ?
          AND COALESCE(task_id, '') = ?
          AND COALESCE(agent_id, '') = ?
   )
   AND status IN ('PENDING', 'ACTIVE', 'BLOCKED')`,
		now, now, workspaceID, workspaceID, taskID, agentID)
	if err != nil {
		return false, fmt.Errorf("cancel execution steps on task release: %w", err)
	}
	runAffected, err := runResult.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cancel execution runs rows affected: %w", err)
	}
	stepAffected, err := stepResult.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cancel execution steps rows affected: %w", err)
	}
	return runAffected > 0 || stepAffected > 0, nil
}

func taskResolutionToExecutionRunStatus(resolution string) string {
	switch strings.TrimSpace(resolution) {
	case model.TaskStatusResolved:
		return "COMPLETED"
	case model.TaskStatusFailed:
		return "FAILED"
	case model.TaskStatusCancelled:
		return "CANCELLED"
	case model.TaskClaimStatusBlocked:
		return "BLOCKED"
	default:
		return "CANCELLED"
	}
}

func taskResolutionToExecutionStepStatus(resolution string) string {
	return taskResolutionToExecutionRunStatus(resolution)
}

func (s *Store) resolveOpenOperatorQueuesForClosedTaskTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, resolution, actorID, now string) error {
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(taskID) == "" {
		return nil
	}
	resolutionNote := "cleared_by_task_close:" + strings.TrimSpace(resolution)
	if _, err := tx.ExecContext(ctx, `
UPDATE operator_queue_items
   SET status = 'RESOLVED',
       resolution = ?,
       resolved_at = ?,
       resolved_by = ?,
       updated_at = ?,
       revision = revision + 1
 WHERE workspace_id = ?
   AND task_id = ?
   AND status = 'OPEN'`,
		resolutionNote, now, firstNonEmpty(strings.TrimSpace(actorID), "task.close"), now, workspaceID, taskID); err != nil {
		return fmt.Errorf("resolve operator queues on task close: %w", err)
	}
	return nil
}

func (s *Store) releaseProjectRolesIfNoOpenTasksTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID, reason, now string) error {
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(projectID) == "" {
		return nil
	}
	var openTasks int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM workspace_tasks wt
  JOIN tasks t ON t.task_id = wt.task_id
 WHERE wt.workspace_id = ?
   AND t.project_id = ?
   AND t.status NOT IN (?, ?, ?)`,
		workspaceID, projectID, model.TaskStatusResolved, model.TaskStatusFailed, model.TaskStatusCancelled).Scan(&openTasks); err != nil {
		return fmt.Errorf("count open project tasks on task close: %w", err)
	}
	if openTasks > 0 {
		return nil
	}
	summary := strings.TrimSpace(reason)
	if summary == "" {
		summary = "released because project has no open tasks after task close"
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?,
       released_at = ?,
       summary = ?,
       updated_by = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND project_id = ?
   AND status = ?`,
		ProjectRoleStatusReleased, now, summary, firstNonEmpty(strings.TrimSpace(actorID), "task.close"), now, workspaceID, projectID, ProjectRoleStatusActive); err != nil {
		return fmt.Errorf("release project roles after final task close: %w", err)
	}
	return nil
}

func (s *Store) CreateResourceRequest(ctx context.Context, input ResourceRequestCreateInput) error {
	requestID := strings.TrimSpace(input.RequestID)
	if requestID == "" {
		return errors.New("request_id is required")
	}
	if strings.TrimSpace(input.TaskID) == "" {
		return errors.New("task_id is required")
	}
	if strings.TrimSpace(input.NodeID) == "" {
		return errors.New("node_id is required")
	}
	if strings.TrimSpace(input.OwnerUserID) == "" {
		return errors.New("owner_user_id is required")
	}
	if strings.TrimSpace(input.ResourceType) == "" {
		return errors.New("resource_type is required")
	}
	if strings.TrimSpace(input.ServiceID) == "" {
		return errors.New("service_id is required")
	}
	if strings.TrimSpace(input.Justification) == "" {
		return errors.New("justification is required")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return errors.New("idempotency_key is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	decision := strings.TrimSpace(input.Decision)
	decisionReason := strings.TrimSpace(input.DecisionReason)

	var decidedAt any
	if decision != "" {
		decidedAt = now
	}

	_, err := s.writeDB.ExecContext(
		ctx,
		`INSERT INTO resource_requests(
			request_id, task_id, node_id, owner_user_id, resource_type, service_id,
			estimated_cost_usd, justification, idempotency_key,
			decision, decision_reason, created_at, decided_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		requestID,
		strings.TrimSpace(input.TaskID),
		strings.TrimSpace(input.NodeID),
		strings.TrimSpace(input.OwnerUserID),
		strings.TrimSpace(input.ResourceType),
		strings.TrimSpace(input.ServiceID),
		input.EstimatedCostUSD,
		strings.TrimSpace(input.Justification),
		strings.TrimSpace(input.IdempotencyKey),
		decision,
		decisionReason,
		now,
		decidedAt,
	)
	if err != nil {
		return fmt.Errorf("insert resource request: %w", err)
	}
	return nil
}

func (s *Store) CreateApprovalRequest(ctx context.Context, input ApprovalCreateInput) error {
	approvalID := strings.TrimSpace(input.ApprovalID)
	if approvalID == "" {
		return errors.New("approval_id is required")
	}
	requestID := strings.TrimSpace(input.RequestID)
	if requestID == "" {
		return errors.New("request_id is required")
	}

	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = model.ApprovalStatusCreated
	}
	if !model.ValidApprovalStatus(status) {
		return fmt.Errorf("invalid approval status: %s", status)
	}

	ttlSec := input.TTLSec
	if ttlSec <= 0 {
		ttlSec = 300
	}

	_, err := s.writeDB.ExecContext(
		ctx,
		`INSERT INTO approval_requests(
			approval_id, request_id, status, ttl_sec, created_at, decided_at, decided_by, decision_note
		) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL)`,
		approvalID,
		requestID,
		status,
		ttlSec,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert approval request: %w", err)
	}
	return nil
}

func (s *Store) AddApprovalEvent(ctx context.Context, input ApprovalEventInput) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin approval event tx: %w", err)
	}

	if err := s.addApprovalEventTx(ctx, tx, input); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit approval event tx: %w", err)
	}
	return nil
}

func (s *Store) DecideApproval(ctx context.Context, input ApprovalDecisionInput) error {
	approvalID := strings.TrimSpace(input.ApprovalID)
	if approvalID == "" {
		return errors.New("approval_id is required")
	}

	newStatus := strings.TrimSpace(input.NewStatus)
	if newStatus == "" {
		return errors.New("new_status is required")
	}
	if !model.ValidApprovalStatus(newStatus) {
		return fmt.Errorf("invalid approval status: %s", newStatus)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin approval decision tx: %w", err)
	}

	var currentStatus, requestID, taskID, nodeID string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT ar.status, ar.request_id, rr.task_id, rr.node_id
		 FROM approval_requests ar
		 JOIN resource_requests rr ON rr.request_id = ar.request_id
		 WHERE ar.approval_id = ?`,
		approvalID,
	).Scan(&currentStatus, &requestID, &taskID, &nodeID); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return ErrApprovalNotFound
		}
		return fmt.Errorf("query approval status: %w", err)
	}

	if err := orchestrator.ValidateApprovalTransition(currentStatus, newStatus); err != nil {
		_ = tx.Rollback()
		return err
	}

	if currentStatus == newStatus {
		_ = tx.Rollback()
		return nil
	}

	decidedAt := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(
		ctx,
		`UPDATE approval_requests
		 SET status = ?, decided_at = ?, decided_by = ?, decision_note = ?
		 WHERE approval_id = ?`,
		newStatus,
		decidedAt,
		strings.TrimSpace(input.DecidedBy),
		strings.TrimSpace(input.DecisionNote),
		approvalID,
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("update approval status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("rows affected for approval update: %w", err)
	}
	if rowsAffected == 0 {
		_ = tx.Rollback()
		return ErrApprovalNotFound
	}

	eventPayload := mustJSON(map[string]any{
		"approval_id":   approvalID,
		"request_id":    requestID,
		"status":        newStatus,
		"decided_by":    strings.TrimSpace(input.DecidedBy),
		"decision_note": strings.TrimSpace(input.DecisionNote),
		"task_id":       taskID,
		"node_id":       nodeID,
	})

	if err := s.addApprovalEventTx(ctx, tx, ApprovalEventInput{
		EventID:     nextID("approval_event"),
		ApprovalID:  approvalID,
		EventType:   "approval_decided",
		ActorID:     strings.TrimSpace(input.DecidedBy),
		PayloadJSON: eventPayload,
		OccurredAt:  decidedAt,
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:     nextID("audit"),
		EventType:   "approval_decided",
		EntityType:  "approval",
		EntityID:    approvalID,
		ActorID:     strings.TrimSpace(input.DecidedBy),
		PayloadJSON: eventPayload,
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	switch newStatus {
	case model.ApprovalStatusApproved:
		if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
			TransitionID: nextID("node_transition"),
			TaskID:       taskID,
			NodeID:       nodeID,
			NewStatus:    model.NodeStatusRunning,
			Reason:       "approval_approved",
			ActorID:      strings.TrimSpace(input.DecidedBy),
		}); err != nil {
			_ = tx.Rollback()
			return err
		}
	case model.ApprovalStatusRejected:
		if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
			TransitionID: nextID("node_transition"),
			TaskID:       taskID,
			NodeID:       nodeID,
			NewStatus:    model.NodeStatusFailed,
			Reason:       "approval_rejected",
			ActorID:      strings.TrimSpace(input.DecidedBy),
		}); err != nil {
			_ = tx.Rollback()
			return err
		}
	case model.ApprovalStatusExpired:
		if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
			TransitionID: nextID("node_transition"),
			TaskID:       taskID,
			NodeID:       nodeID,
			NewStatus:    model.NodeStatusFailed,
			Reason:       "approval_timeout",
			ActorID:      strings.TrimSpace(input.DecidedBy),
		}); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit approval decision tx: %w", err)
	}

	return nil
}

func (s *Store) RecordSpendTransaction(ctx context.Context, input SpendTransactionInput) error {
	txID := strings.TrimSpace(input.TxID)
	if txID == "" {
		return errors.New("tx_id is required")
	}
	if strings.TrimSpace(input.OwnerUserID) == "" {
		return errors.New("owner_user_id is required")
	}
	if strings.TrimSpace(input.TaskID) == "" {
		return errors.New("task_id is required")
	}
	if strings.TrimSpace(input.NodeID) == "" {
		return errors.New("node_id is required")
	}
	if strings.TrimSpace(input.ServiceID) == "" {
		return errors.New("service_id is required")
	}
	if input.AmountUSD <= 0 {
		return errors.New("amount_usd must be positive")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin spend tx: %w", err)
	}

	result, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO spend_transactions(
			tx_id, owner_user_id, task_id, node_id, service_id, amount_usd, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		txID,
		strings.TrimSpace(input.OwnerUserID),
		strings.TrimSpace(input.TaskID),
		strings.TrimSpace(input.NodeID),
		strings.TrimSpace(input.ServiceID),
		input.AmountUSD,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert spend transaction: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("rows affected for spend insert: %w", err)
	}
	if rowsAffected == 0 {
		_ = tx.Rollback()
		return nil
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "spend_recorded",
		EntityType: "spend_transaction",
		EntityID:   txID,
		PayloadJSON: mustJSON(map[string]any{
			"tx_id":         txID,
			"owner_user_id": strings.TrimSpace(input.OwnerUserID),
			"task_id":       strings.TrimSpace(input.TaskID),
			"node_id":       strings.TrimSpace(input.NodeID),
			"service_id":    strings.TrimSpace(input.ServiceID),
			"amount_usd":    input.AmountUSD,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit spend tx: %w", err)
	}

	return nil
}

func (s *Store) EvaluateAndRecordSpendAtomic(ctx context.Context, input SpendGuardedInput) error {
	txID := strings.TrimSpace(input.TxID)
	if txID == "" {
		return errors.New("tx_id is required")
	}
	ownerUserID := strings.TrimSpace(input.OwnerUserID)
	if ownerUserID == "" {
		return errors.New("owner_user_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return errors.New("task_id is required")
	}
	nodeID := strings.TrimSpace(input.NodeID)
	if nodeID == "" {
		return errors.New("node_id is required")
	}
	serviceID := strings.TrimSpace(input.ServiceID)
	if serviceID == "" {
		return errors.New("service_id is required")
	}
	if input.AmountUSD <= 0 {
		return errors.New("amount_usd must be positive")
	}
	if input.MaxDailySpendUSD < 0 || input.MaxTaskSpendUSD < 0 {
		return errors.New("budget limit cannot be negative")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire sqlite conn for guarded spend: %w", err)
	}
	defer conn.Close()

	finished := false
	if err := beginConnImmediate(ctx, conn); err != nil {
		return err
	}
	defer func() {
		if !finished {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	now := time.Now().UTC()
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)
	dayStartISO := dayStart.Format(time.RFC3339Nano)
	dayEndISO := dayEnd.Format(time.RFC3339Nano)

	var dailySpend sql.NullFloat64
	if err := conn.QueryRowContext(
		ctx,
		`SELECT COALESCE(SUM(amount_usd), 0)
		 FROM spend_transactions
		 WHERE owner_user_id = ?
		   AND created_at >= ?
		   AND created_at < ?`,
		ownerUserID,
		dayStartISO,
		dayEndISO,
	).Scan(&dailySpend); err != nil {
		return fmt.Errorf("query daily spend: %w", err)
	}

	var taskSpend sql.NullFloat64
	if err := conn.QueryRowContext(
		ctx,
		`SELECT COALESCE(SUM(amount_usd), 0)
		 FROM spend_transactions
		 WHERE task_id = ?`,
		taskID,
	).Scan(&taskSpend); err != nil {
		return fmt.Errorf("query task spend: %w", err)
	}

	currentDaily := nullFloat64OrZero(dailySpend)
	currentTask := nullFloat64OrZero(taskSpend)

	nextDaily := currentDaily + input.AmountUSD
	nextTask := currentTask + input.AmountUSD

	if input.MaxDailySpendUSD > 0 && nextDaily > input.MaxDailySpendUSD {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		finished = true
		return fmt.Errorf("%w: daily budget exceeded (current=%.4f, amount=%.4f, limit=%.4f)", ErrBudgetExceeded, currentDaily, input.AmountUSD, input.MaxDailySpendUSD)
	}

	if input.MaxTaskSpendUSD > 0 && nextTask > input.MaxTaskSpendUSD {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		finished = true
		return fmt.Errorf("%w: task budget exceeded (current=%.4f, amount=%.4f, limit=%.4f)", ErrBudgetExceeded, currentTask, input.AmountUSD, input.MaxTaskSpendUSD)
	}

	result, err := conn.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO spend_transactions(
			tx_id, owner_user_id, task_id, node_id, service_id, amount_usd, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		txID,
		ownerUserID,
		taskID,
		nodeID,
		serviceID,
		input.AmountUSD,
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert guarded spend transaction: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for guarded spend insert: %w", err)
	}

	// Duplicate tx_id must be idempotent and treated as success.
	if rowsAffected > 0 {
		payload := mustJSON(map[string]any{
			"tx_id":              txID,
			"owner_user_id":      ownerUserID,
			"task_id":            taskID,
			"node_id":            nodeID,
			"service_id":         serviceID,
			"amount_usd":         input.AmountUSD,
			"daily_spend_before": currentDaily,
			"daily_spend_after":  nextDaily,
			"task_spend_before":  currentTask,
			"task_spend_after":   nextTask,
			"max_daily_usd":      input.MaxDailySpendUSD,
			"max_task_usd":       input.MaxTaskSpendUSD,
		})

		if _, err := conn.ExecContext(
			ctx,
			`INSERT INTO audit_events(
				event_id, event_type, entity_type, entity_id, actor_id, payload_json, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			nextID("audit"),
			"spend_recorded_guarded",
			"spend_transaction",
			txID,
			ownerUserID,
			payload,
			now.Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert guarded spend audit event: %w", err)
		}
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit guarded spend transaction: %w", err)
	}
	finished = true
	return nil
}

func (s *Store) RecordClearingEntry(ctx context.Context, input ClearingEntryInput, teamBorrowingEnabled bool) error {
	if !teamBorrowingEnabled {
		return ErrFeatureDisabled
	}

	entryID := strings.TrimSpace(input.EntryID)
	if entryID == "" {
		return errors.New("entry_id is required")
	}
	if strings.TrimSpace(input.DebtorUserID) == "" {
		return errors.New("debtor_user_id is required")
	}
	if strings.TrimSpace(input.CreditorUserID) == "" {
		return errors.New("creditor_user_id is required")
	}
	if strings.TrimSpace(input.ResourceKey) == "" {
		return errors.New("resource_key is required")
	}
	if input.Amount <= 0 {
		return errors.New("amount must be positive")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin clearing tx: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO clearing_ledger(
			entry_id, debtor_user_id, creditor_user_id, resource_key, amount, created_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		entryID,
		strings.TrimSpace(input.DebtorUserID),
		strings.TrimSpace(input.CreditorUserID),
		strings.TrimSpace(input.ResourceKey),
		input.Amount,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert clearing entry: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "clearing_entry_recorded",
		EntityType: "clearing_ledger",
		EntityID:   entryID,
		PayloadJSON: mustJSON(map[string]any{
			"entry_id":         entryID,
			"debtor_user_id":   strings.TrimSpace(input.DebtorUserID),
			"creditor_user_id": strings.TrimSpace(input.CreditorUserID),
			"resource_key":     strings.TrimSpace(input.ResourceKey),
			"amount":           input.Amount,
		}),
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit clearing tx: %w", err)
	}

	return nil
}

func (s *Store) ListApprovalRequests(ctx context.Context, status string) ([]ApprovalRecord, error) {
	status = strings.TrimSpace(status)
	args := []any{}
	query := `
SELECT ar.approval_id, ar.request_id, ar.status, ar.ttl_sec, ar.created_at, ar.decided_at, ar.decided_by, ar.decision_note,
       rr.task_id, rr.node_id
FROM approval_requests ar
LEFT JOIN resource_requests rr ON rr.request_id = ar.request_id`
	if status != "" {
		query += " WHERE ar.status = ?"
		args = append(args, status)
	}
	query += " ORDER BY ar.created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query approvals: %w", err)
	}
	defer rows.Close()

	out := []ApprovalRecord{}
	for rows.Next() {
		var row ApprovalRecord
		var decidedAt, decidedBy, decisionNote sql.NullString
		if err := rows.Scan(
			&row.ApprovalID,
			&row.RequestID,
			&row.Status,
			&row.TTLSec,
			&row.CreatedAt,
			&decidedAt,
			&decidedBy,
			&decisionNote,
			&row.TaskID,
			&row.NodeID,
		); err != nil {
			return nil, fmt.Errorf("scan approval row: %w", err)
		}
		row.DecidedAt = nullStringPtr(decidedAt)
		row.DecidedBy = nullStringPtr(decidedBy)
		row.DecisionNote = nullStringPtr(decisionNote)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate approvals: %w", err)
	}

	return out, nil
}

func (s *Store) ListSpendTransactionsByTask(ctx context.Context, taskID string) ([]SpendTransactionRecord, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, errors.New("task_id is required")
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT tx_id, owner_user_id, task_id, node_id, service_id, amount_usd, created_at
		 FROM spend_transactions
		 WHERE task_id = ?
		 ORDER BY created_at`,
		taskID,
	)
	if err != nil {
		return nil, fmt.Errorf("query spend transactions: %w", err)
	}
	defer rows.Close()

	out := []SpendTransactionRecord{}
	for rows.Next() {
		var row SpendTransactionRecord
		if err := rows.Scan(
			&row.TxID,
			&row.OwnerUserID,
			&row.TaskID,
			&row.NodeID,
			&row.ServiceID,
			&row.AmountUSD,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan spend row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate spend rows: %w", err)
	}

	return out, nil
}

func (s *Store) ListAuditEvents(ctx context.Context, filter AuditEventFilter) ([]AuditEventRecord, error) {
	clauses := make([]string, 0, 4)
	args := make([]any, 0, 5)

	if eventType := strings.TrimSpace(filter.EventType); eventType != "" {
		clauses = append(clauses, "event_type = ?")
		args = append(args, eventType)
	}
	if entityType := strings.TrimSpace(filter.EntityType); entityType != "" {
		clauses = append(clauses, "entity_type = ?")
		args = append(args, entityType)
	}
	if entityID := strings.TrimSpace(filter.EntityID); entityID != "" {
		clauses = append(clauses, "entity_id = ?")
		args = append(args, entityID)
	}
	if actorID := strings.TrimSpace(filter.ActorID); actorID != "" {
		clauses = append(clauses, "actor_id = ?")
		args = append(args, actorID)
	}

	query := `
SELECT event_id, event_type, entity_type, entity_id, actor_id, payload_json, created_at
FROM audit_events`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC, event_id DESC"

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += " LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer rows.Close()

	out := []AuditEventRecord{}
	for rows.Next() {
		var row AuditEventRecord
		var actorID, payloadJSON sql.NullString
		if err := rows.Scan(
			&row.EventID,
			&row.EventType,
			&row.EntityType,
			&row.EntityID,
			&actorID,
			&payloadJSON,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan audit event row: %w", err)
		}
		if actorID.Valid {
			row.ActorID = actorID.String
		}
		if payloadJSON.Valid {
			row.PayloadJSON = payloadJSON.String
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}

	return out, nil
}

func (s *Store) ListExecutableNodes(ctx context.Context, limit int) ([]ExecutableNode, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT n.task_id, n.node_id, n.node_type, n.status, n.attempt_count, t.owner_user_id, t.priority
		 FROM dag_nodes n
		 JOIN tasks t ON t.task_id = n.task_id
		 WHERE t.task_kind = ?
		   AND COALESCE(t.requires_project_gate, 0) = 0
		   AND NOT (
		     LOWER(TRIM(COALESCE(t.project_lane, ''))) = 'review'
		     AND json_valid(COALESCE(t.task_requirements_json, '{}'))
		     AND LOWER(TRIM(COALESCE(json_extract(t.task_requirements_json, '$.patch_queue_task_kind'), ''))) = 'review_receipt'
		   )
		   AND t.status IN (?, ?)
		   AND n.status IN (?, ?)
		   AND NOT EXISTS (
		     SELECT 1
		     FROM node_dependencies d
		     JOIN dag_nodes dep
		       ON dep.task_id = d.task_id
		      AND dep.node_id = d.depends_on_node_id
		     WHERE d.task_id = n.task_id
		       AND d.node_id = n.node_id
		       AND dep.status <> ?
		   )
		 ORDER BY
		   CASE LOWER(TRIM(t.priority))
		     WHEN 'critical' THEN 0
		     WHEN 'high' THEN 1
		     WHEN 'normal' THEN 2
		     WHEN 'low' THEN 3
		     ELSE 4
		   END,
		   n.created_at,
		   n.node_id
		 LIMIT ?`,
		model.TaskKindExecution,
		model.TaskStatusPending,
		model.TaskStatusRunning,
		model.NodeStatusPending,
		model.NodeStatusBlocked,
		model.NodeStatusResolved,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query executable nodes: %w", err)
	}
	defer rows.Close()

	out := []ExecutableNode{}
	for rows.Next() {
		var row ExecutableNode
		if err := rows.Scan(
			&row.TaskID,
			&row.NodeID,
			&row.NodeType,
			&row.Status,
			&row.AttemptCount,
			&row.OwnerUserID,
			&row.Priority,
		); err != nil {
			return nil, fmt.Errorf("scan executable node: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate executable nodes: %w", err)
	}

	return out, nil
}

func (s *Store) ClaimExecutableNodes(ctx context.Context, limit int, actorID string) ([]ExecutableNode, error) {
	if limit <= 0 {
		limit = 20
	}
	localNode, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("ensure local authority node for executable claim: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin executable node claim tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	type executableClaimCandidate struct {
		ExecutableNode
		ExpectedLeaseToken string
		ExpectedTerm       int64
	}

	rows, err := tx.QueryContext(
		ctx,
		`WITH task_workspace_scope AS (
		   SELECT wt.task_id,
		          MIN(wt.workspace_id) AS workspace_id,
		          COUNT(1) AS workspace_count
		     FROM workspace_tasks wt
		    GROUP BY wt.task_id
		 )
		 SELECT tw.workspace_id, wa.lease_token, wa.term,
		        n.task_id, n.node_id, n.node_type, n.status, n.attempt_count, t.owner_user_id, t.priority
		 FROM dag_nodes n
		 JOIN tasks t ON t.task_id = n.task_id
		 JOIN task_workspace_scope tw ON tw.task_id = n.task_id
		 JOIN workspace_authority wa ON wa.workspace_id = tw.workspace_id AND wa.scope = ?
		 WHERE t.task_kind = ?
		   AND COALESCE(t.requires_project_gate, 0) = 0
		   AND NOT (
		     LOWER(TRIM(COALESCE(t.project_lane, ''))) = 'review'
		     AND json_valid(COALESCE(t.task_requirements_json, '{}'))
		     AND LOWER(TRIM(COALESCE(json_extract(t.task_requirements_json, '$.patch_queue_task_kind'), ''))) = 'review_receipt'
		   )
		   AND tw.workspace_count = 1
		   AND wa.holder_authority_node_id = ?
		   AND TRIM(COALESCE(wa.lease_token, '')) != ''
		   AND wa.term > 0
		   AND t.status IN (?, ?)
		   AND n.status IN (?, ?)
		   AND NOT EXISTS (
		     SELECT 1
		     FROM node_dependencies d
		     JOIN dag_nodes dep
		       ON dep.task_id = d.task_id
		      AND dep.node_id = d.depends_on_node_id
		     WHERE d.task_id = n.task_id
		       AND d.node_id = n.node_id
		       AND dep.status <> ?
		   )
		 ORDER BY
		   CASE LOWER(TRIM(t.priority))
		     WHEN 'critical' THEN 0
		     WHEN 'high' THEN 1
		     WHEN 'normal' THEN 2
		     WHEN 'low' THEN 3
		     ELSE 4
		   END,
		   n.created_at,
		   n.node_id
		 LIMIT ?`,
		authorityScopeWorkspace,
		model.TaskKindExecution,
		localNode.AuthorityNodeID,
		model.TaskStatusPending,
		model.TaskStatusRunning,
		model.NodeStatusPending,
		model.NodeStatusBlocked,
		model.NodeStatusResolved,
		limit,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("query executable nodes for claim: %w", err)
	}

	candidates := make([]executableClaimCandidate, 0, limit)
	for rows.Next() {
		var row executableClaimCandidate
		if err := rows.Scan(
			&row.WorkspaceID,
			&row.ExpectedLeaseToken,
			&row.ExpectedTerm,
			&row.TaskID,
			&row.NodeID,
			&row.NodeType,
			&row.Status,
			&row.AttemptCount,
			&row.OwnerUserID,
			&row.Priority,
		); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return nil, fmt.Errorf("scan executable node for claim: %w", err)
		}
		candidates = append(candidates, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		_ = tx.Rollback()
		return nil, fmt.Errorf("iterate executable nodes for claim: %w", err)
	}
	_ = rows.Close()

	claimed := make([]ExecutableNode, 0, len(candidates))
	checkedWorkspaces := make(map[string]struct{}, len(candidates))
	for _, row := range candidates {
		if _, seen := checkedWorkspaces[row.WorkspaceID]; !seen {
			fenceInput := WorkspaceAuthorityFenceInput{
				WorkspaceID:                   row.WorkspaceID,
				Scope:                         authorityScopeWorkspace,
				ExpectedHolderAuthorityNodeID: localNode.AuthorityNodeID,
				ExpectedLeaseToken:            row.ExpectedLeaseToken,
				ExpectedTerm:                  row.ExpectedTerm,
				ReferenceAt:                   now,
			}
			if _, err := s.checkWorkspaceAuthorityFenceTx(ctx, tx, fenceInput); err != nil {
				_ = tx.Rollback()
				return nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
			}
			checkedWorkspaces[row.WorkspaceID] = struct{}{}
		}
		if row.Status == model.NodeStatusBlocked {
			if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
				TaskID:    row.TaskID,
				NodeID:    row.NodeID,
				NewStatus: model.NodeStatusPending,
				Reason:    "dependencies_resolved",
				ActorID:   strings.TrimSpace(actorID),
			}); err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("set blocked node pending before claim %s/%s: %w", row.TaskID, row.NodeID, err)
			}
		}
		if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
			TaskID:    row.TaskID,
			NodeID:    row.NodeID,
			NewStatus: model.NodeStatusRunning,
			Reason:    "executor_dispatched",
			ActorID:   strings.TrimSpace(actorID),
		}); err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("claim executable node %s/%s: %w", row.TaskID, row.NodeID, err)
		}
		if err := s.incrementNodeAttemptTx(ctx, tx, row.TaskID, row.NodeID); err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("increment node attempt while claiming %s/%s: %w", row.TaskID, row.NodeID, err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ? AND status = ?`,
			model.TaskStatusRunning,
			now,
			row.TaskID,
			model.TaskStatusPending,
		); err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("mark task running while claiming node %s/%s: %w", row.TaskID, row.NodeID, err)
		}

		row.Status = model.NodeStatusRunning
		row.AttemptCount++
		claimed = append(claimed, row.ExecutableNode)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit executable node claim tx: %w", err)
	}
	return claimed, nil
}

func (s *Store) IncrementNodeAttempt(ctx context.Context, taskID, nodeID string) error {
	taskID = strings.TrimSpace(taskID)
	nodeID = strings.TrimSpace(nodeID)
	if taskID == "" {
		return errors.New("task_id is required")
	}
	if nodeID == "" {
		return errors.New("node_id is required")
	}

	result, err := s.writeDB.ExecContext(
		ctx,
		`UPDATE dag_nodes
		 SET attempt_count = attempt_count + 1,
		     updated_at = ?
		 WHERE task_id = ? AND node_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		taskID,
		nodeID,
	)
	if err != nil {
		return fmt.Errorf("increment node attempt: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for increment node attempt: %w", err)
	}
	if affected == 0 {
		return ErrNodeNotFound
	}
	return nil
}

func (s *Store) incrementNodeAttemptTx(ctx context.Context, tx *sql.Tx, taskID, nodeID string) error {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE dag_nodes
		 SET attempt_count = attempt_count + 1,
		     updated_at = ?
		 WHERE task_id = ? AND node_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		taskID,
		nodeID,
	)
	if err != nil {
		return fmt.Errorf("increment node attempt: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for increment node attempt: %w", err)
	}
	if affected == 0 {
		return ErrNodeNotFound
	}
	return nil
}

func (s *Store) UpdateTaskStatusFromNodes(ctx context.Context, taskID, actorID, reason string) (string, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", errors.New("task_id is required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return "", fmt.Errorf("begin task status tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	status, err := s.updateTaskStatusFromNodesTx(ctx, tx, taskID, actorID, reason, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit task status tx: %w", err)
	}
	return status, nil
}

func (s *Store) UpdateTaskStatusFromNodesWithWorkspaceAuthority(ctx context.Context, workspaceID, taskID, actorID, reason string) (string, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", errors.New("task_id is required")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return "", err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return "", fmt.Errorf("begin fenced task status tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status string
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, _ WorkspaceAuthorityRecord) error {
		nextStatus, err := s.updateTaskStatusFromNodesTx(ctx, tx, taskID, actorID, reason, now)
		if err != nil {
			return err
		}
		status = nextStatus
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return "", s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit fenced task status tx: %w", err)
	}
	return status, nil
}

func (s *Store) SetNodeStatusAndUpdateTaskStatusWithWorkspaceAuthority(ctx context.Context, workspaceID string, input NodeStatusUpdateInput, taskReason string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return "", err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return "", fmt.Errorf("begin fenced node status tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var taskStatus string
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, _ WorkspaceAuthorityRecord) error {
		if err := s.setNodeStatusTx(ctx, tx, input); err != nil {
			return err
		}
		nextStatus, err := s.updateTaskStatusFromNodesTx(ctx, tx, input.TaskID, input.ActorID, taskReason, now)
		if err != nil {
			return err
		}
		taskStatus = nextStatus
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return "", s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit fenced node status tx: %w", err)
	}
	return taskStatus, nil
}

func (s *Store) updateTaskStatusFromNodesTx(ctx context.Context, tx *sql.Tx, taskID, actorID, reason, updatedAt string) (string, error) {
	if tx == nil {
		return "", errors.New("task status tx is required")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", errors.New("task_id is required")
	}
	updatedAt = strings.TrimSpace(updatedAt)
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	var currentStatus string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&currentStatus); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrTaskNotFound
		}
		return "", fmt.Errorf("query current task status: %w", err)
	}

	var total, resolved, failed, cancelled, running, awaiting int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT
		   COUNT(1),
		   COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		   COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		   COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		   COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		   COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0)
		 FROM dag_nodes
		 WHERE task_id = ?`,
		model.NodeStatusResolved,
		model.NodeStatusFailed,
		model.NodeStatusCancelled,
		model.NodeStatusRunning,
		model.NodeStatusAwaitingFunds,
		taskID,
	).Scan(&total, &resolved, &failed, &cancelled, &running, &awaiting); err != nil {
		_ = tx.Rollback()
		return "", fmt.Errorf("aggregate node statuses for task %s: %w", taskID, err)
	}

	newStatus := model.TaskStatusPending
	switch {
	case total == 0:
		newStatus = model.TaskStatusPending
	case failed > 0:
		newStatus = model.TaskStatusFailed
	case resolved+cancelled == total:
		if resolved > 0 {
			newStatus = model.TaskStatusResolved
		} else {
			newStatus = model.TaskStatusCancelled
		}
	case running > 0 || awaiting > 0 || resolved > 0:
		newStatus = model.TaskStatusRunning
	default:
		newStatus = model.TaskStatusPending
	}

	if currentStatus != newStatus {
		// W2.1 (P04/P05, H1): the node-aggregation terminal write is the SINGLE gate for the
		// node-completion path (completeNodeClaimWithEvent no longer writes the task status directly,
		// and the three SetNodeStatus RPC callers funnel here too). Only RESOLVED consults a
		// completion contract (OD#1); FAILED/CANCELLED are genuine terminals. On Reject, hold the task
		// at its current status and return the UNCHANGED status so the caller's completer-scoped claim
		// sync is skipped as well - P04 and P05 thus share ONE verdict and the leak cannot be undone a
		// line later. workspace_id (absent from this signature) is resolved from workspace_tasks; a DAG
		// task not attached to any workspace has no project completion contract -> gate no-ops.
		if newStatus == model.TaskStatusResolved {
			var nodeTaskWorkspaceID string
			if err := tx.QueryRowContext(ctx, `SELECT workspace_id FROM workspace_tasks WHERE task_id = ? LIMIT 1`, taskID).Scan(&nodeTaskWorkspaceID); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return "", fmt.Errorf("resolve workspace for node-aggregation admission: %w", err)
			}
			if strings.TrimSpace(nodeTaskWorkspaceID) != "" {
				adm, err := s.evaluateTerminalAdmissionTx(ctx, tx, nodeTaskWorkspaceID, WorkspaceTaskRecord{TaskID: taskID}, TerminalWriteIntent{
					Side:       SideAdmission,
					Kind:       GenuineCompletion,
					Resolution: newStatus,
					Origin:     OriginP05,
					ActorID:    strings.TrimSpace(actorID),
				})
				if err != nil {
					return "", err
				}
				if adm.Decision == TerminalReject {
					return currentStatus, nil
				}
			}
		}
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE tasks
			 SET status = ?, updated_at = ?
			 WHERE task_id = ?`,
			newStatus,
			updatedAt,
			taskID,
		); err != nil {
			return "", fmt.Errorf("update task status: %w", err)
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "task_status_changed",
			EntityType: "task",
			EntityID:   taskID,
			ActorID:    strings.TrimSpace(actorID),
			PayloadJSON: mustJSON(map[string]any{
				"task_id":     taskID,
				"from_status": currentStatus,
				"to_status":   newStatus,
				"reason":      strings.TrimSpace(reason),
				"resolved":    resolved,
				"failed":      failed,
				"cancelled":   cancelled,
				"running":     running,
				"awaiting":    awaiting,
				"nodes_total": total,
			}),
		}); err != nil {
			return "", err
		}
	}

	return newStatus, nil
}

func (c *coordinationCore) SetNodeStatus(ctx context.Context, input NodeStatusUpdateInput) error {
	tx, err := beginTxImmediate(c.writeDB, ctx)
	if err != nil {
		return fmt.Errorf("begin node status tx: %w", err)
	}

	if err := c.store.setNodeStatusTx(ctx, tx, input); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit node status tx: %w", err)
	}
	return nil
}

func (c *journalCore) AddAuditEvent(ctx context.Context, input AuditEventInput) error {
	tx, err := beginTxImmediate(c.writeDB, ctx)
	if err != nil {
		return fmt.Errorf("begin audit tx: %w", err)
	}

	if err := c.addAuditEventTx(ctx, tx, input); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit audit tx: %w", err)
	}
	return nil
}

func (s *Store) setNodeStatusTx(ctx context.Context, tx *sql.Tx, input NodeStatusUpdateInput) error {
	taskID := strings.TrimSpace(input.TaskID)
	nodeID := strings.TrimSpace(input.NodeID)
	newStatus := strings.TrimSpace(input.NewStatus)
	if taskID == "" {
		return errors.New("task_id is required")
	}
	if nodeID == "" {
		return errors.New("node_id is required")
	}
	if newStatus == "" {
		return errors.New("new_status is required")
	}
	if !model.ValidNodeStatus(newStatus) {
		return fmt.Errorf("invalid node status: %s", newStatus)
	}

	var currentStatus string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT status FROM dag_nodes WHERE task_id = ? AND node_id = ?`,
		taskID,
		nodeID,
	).Scan(&currentStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNodeNotFound
		}
		return fmt.Errorf("query node status: %w", err)
	}

	if err := orchestrator.ValidateNodeTransition(currentStatus, newStatus); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	reason := strings.TrimSpace(input.Reason)
	var lastError any
	if newStatus == model.NodeStatusFailed {
		if reason == "" {
			reason = "node_failed"
		}
		lastError = reason
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE dag_nodes SET status = ?, last_error = ?, updated_at = ? WHERE task_id = ? AND node_id = ?`,
		newStatus,
		lastError,
		now,
		taskID,
		nodeID,
	)
	if err != nil {
		return fmt.Errorf("update node status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for node status update: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNodeNotFound
	}

	transitionID := strings.TrimSpace(input.TransitionID)
	if transitionID == "" {
		transitionID = nextID("node_transition")
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO node_state_transitions(
			transition_id, task_id, node_id, from_status, to_status, reason, actor_id, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		transitionID,
		taskID,
		nodeID,
		currentStatus,
		newStatus,
		reason,
		strings.TrimSpace(input.ActorID),
		now,
	)
	if err != nil {
		return fmt.Errorf("insert node transition: %w", err)
	}

	return nil
}

func (s *Store) addApprovalEventTx(ctx context.Context, tx *sql.Tx, input ApprovalEventInput) error {
	eventID := strings.TrimSpace(input.EventID)
	if eventID == "" {
		eventID = nextID("approval_event")
	}
	approvalID := strings.TrimSpace(input.ApprovalID)
	if approvalID == "" {
		return errors.New("approval_id is required")
	}
	eventType := strings.TrimSpace(input.EventType)
	if eventType == "" {
		return errors.New("event_type is required")
	}

	occurredAt := strings.TrimSpace(input.OccurredAt)
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO approval_events(
			event_id, approval_id, event_type, actor_id, payload_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		eventID,
		approvalID,
		eventType,
		strings.TrimSpace(input.ActorID),
		strings.TrimSpace(input.PayloadJSON),
		occurredAt,
	)
	if err != nil {
		return fmt.Errorf("insert approval event: %w", err)
	}
	return nil
}

func (c *journalCore) addAuditEventTx(ctx context.Context, tx *sql.Tx, input AuditEventInput) error {
	eventType := strings.TrimSpace(input.EventType)
	if eventType == "" {
		return errors.New("event_type is required")
	}
	entityType := strings.TrimSpace(input.EntityType)
	if entityType == "" {
		return errors.New("entity_type is required")
	}
	entityID := strings.TrimSpace(input.EntityID)
	if entityID == "" {
		return errors.New("entity_id is required")
	}

	eventID := strings.TrimSpace(input.EventID)
	if eventID == "" {
		eventID = nextID("audit")
	}

	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO audit_events(
			event_id, event_type, entity_type, entity_id, actor_id, payload_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		eventID,
		eventType,
		entityType,
		entityID,
		strings.TrimSpace(input.ActorID),
		strings.TrimSpace(input.PayloadJSON),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	return nil
}

func nullStringPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	out := v.String
	return &out
}

func nextID(prefix string) string {
	n := atomic.AddUint64(&idCounter, 1)
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UTC().UnixNano(), n)
}

func mustJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func nullFloat64OrZero(v sql.NullFloat64) float64 {
	if !v.Valid {
		return 0
	}
	return v.Float64
}

func normalizePriority(v string) string {
	p := strings.ToLower(strings.TrimSpace(v))
	if p == "" {
		return "normal"
	}
	return p
}

func validPriority(v string) bool {
	switch v {
	case "low", "normal", "high", "critical":
		return true
	default:
		return false
	}
}

func normalizeTaskClassEvidence(taskClass, taskClassSource string) (string, string, string, error) {
	rawTaskClass := strings.TrimSpace(taskClass)
	rawTaskClassSource := strings.TrimSpace(taskClassSource)
	taskClass = model.NormalizeTaskClass(rawTaskClass)
	taskClassSource = model.NormalizeTaskClassSource(rawTaskClassSource)

	if rawTaskClass != "" && taskClass == "" {
		return "", "", "", fmt.Errorf("invalid task_class: %s", rawTaskClass)
	}
	if rawTaskClassSource != "" && taskClassSource == "" {
		return "", "", "", fmt.Errorf("invalid task_class_source: %s", rawTaskClassSource)
	}

	if taskClass == "" || taskClass == model.TaskClassUnknown {
		if taskClassSource != "" && taskClassSource != model.TaskClassSourceUnset {
			return "", "", "", fmt.Errorf("task_class_source %s requires a concrete task_class", taskClassSource)
		}
		return "", model.TaskClassSourceUnset, "", nil
	}
	if taskClassSource == "" {
		taskClassSource = model.TaskClassSourceExplicit
	}
	switch taskClassSource {
	case model.TaskClassSourceExplicit, model.TaskClassSourceTemplateDefault:
		return taskClass, taskClassSource, "", nil
	case model.TaskClassSourceUnset:
		return "", "", "", errors.New("task_class_source UNSET cannot be used with a concrete task_class")
	case model.TaskClassSourceHeuristicFallback:
		return "", "", "", errors.New("task_class_source HEURISTIC_FALLBACK is reserved for derived read-side hints")
	default:
		return "", "", "", fmt.Errorf("invalid task_class or task_class_source")
	}
}

func normalizeStoredTaskKind(taskKind string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(taskKind))
	if normalized == "" {
		return model.TaskKindExecution, nil
	}
	switch normalized {
	case model.TaskKindExecution,
		model.TaskKindCoordination:
		return normalized, nil
	default:
		return "", fmt.Errorf("invalid task kind: %s", taskKind)
	}
}

func isLegacyTaskKind(taskKind string) bool {
	switch strings.ToUpper(strings.TrimSpace(taskKind)) {
	case model.TaskKindExecution, model.TaskKindCoordination:
		return true
	default:
		return false
	}
}

func normalizeProjectLane(projectLane string) string {
	return strings.ToLower(strings.TrimSpace(projectLane))
}

func boolToSQLiteInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func sqliteIntToBool(value int) bool {
	return value != 0
}

func (s *Store) ensureTaskProjectInWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) error {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required when project_id is set")
	}
	var count int
	query := `SELECT COUNT(1) FROM projects WHERE workspace_id = ? AND project_id = ?`
	var err error
	if tx != nil {
		err = tx.QueryRowContext(ctx, query, workspaceID, projectID).Scan(&count)
	} else {
		err = s.db.QueryRowContext(ctx, query, workspaceID, projectID).Scan(&count)
	}
	if err != nil {
		return fmt.Errorf("check task project scope: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("%w: project_id %s is not in workspace %s", ErrTaskProjectNotFound, projectID, workspaceID)
	}
	return nil
}

func (s *Store) ensureTaskProjectSingleWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if taskID == "" {
		return errors.New("task_id is required")
	}
	rows, err := tx.QueryContext(ctx, `SELECT workspace_id FROM workspace_tasks WHERE task_id = ?`, taskID)
	if err != nil {
		return fmt.Errorf("query task workspace scope: %w", err)
	}
	defer rows.Close()

	seen := map[string]struct{}{}
	for rows.Next() {
		var attachedWorkspaceID string
		if err := rows.Scan(&attachedWorkspaceID); err != nil {
			return fmt.Errorf("scan task workspace scope: %w", err)
		}
		attachedWorkspaceID = strings.TrimSpace(attachedWorkspaceID)
		if attachedWorkspaceID != "" {
			seen[attachedWorkspaceID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate task workspace scope: %w", err)
	}
	if len(seen) != 1 {
		return fmt.Errorf("%w: task %s must have exactly one workspace before project_id can be set", ErrTaskWorkspaceAmbiguous, taskID)
	}
	if _, ok := seen[workspaceID]; !ok {
		return fmt.Errorf("%w: task %s is not exclusively attached to workspace %s", ErrWorkspaceTaskAbsent, taskID, workspaceID)
	}
	return nil
}

func mapTaskResolutionToNodeStatus(resolution string) string {
	switch resolution {
	case model.TaskStatusResolved:
		return model.NodeStatusResolved
	case model.TaskStatusFailed:
		return model.NodeStatusFailed
	case model.TaskStatusCancelled:
		return model.NodeStatusCancelled
	default:
		return model.NodeStatusResolved
	}
}

func taskResolutionToClaimStatus(resolution string) string {
	switch strings.TrimSpace(resolution) {
	case model.TaskStatusResolved:
		return model.TaskClaimStatusCompleted
	case model.TaskStatusFailed:
		return model.TaskClaimStatusFailed
	case model.TaskStatusCancelled:
		return model.TaskClaimStatusCancelled
	default:
		return model.TaskClaimStatusCompleted
	}
}

func isTerminalNodeStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case model.NodeStatusResolved, model.NodeStatusFailed, model.NodeStatusCancelled:
		return true
	default:
		return false
	}
}
