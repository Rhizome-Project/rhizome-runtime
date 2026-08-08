package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestApplyMigrationsConcurrentStoresSameDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "rhizome-concurrent-migrations.db")
	stores := []*sqlite.Store{
		newMigrationLockStore(t, dbPath),
		newMigrationLockStore(t, dbPath),
		newMigrationLockStore(t, dbPath),
	}

	runConcurrentApplyMigrations(t, ctx, stores)

	var applied int
	if err := stores[0].DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if applied == 0 {
		t.Fatal("expected schema migrations to be recorded")
	}

	var tableName string
	if err := stores[0].DB().QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'workspace_security_settings'`).Scan(&tableName); err != nil {
		t.Fatalf("query bootstrapped auth table: %v", err)
	}
	if tableName != "workspace_security_settings" {
		t.Fatalf("expected workspace_security_settings table, got %q", tableName)
	}
}

func TestApplyMigrationsConcurrentStoresBootstrapWorkspaceSecuritySettings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "rhizome-bootstrap-migrations.db")
	seedStore := newMigrationLockStore(t, dbPath)
	if err := seedStore.ApplyMigrations(ctx); err != nil {
		t.Fatalf("seed migrations: %v", err)
	}
	if err := seedStore.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-migration-bootstrap-lock",
		Title:       "Migration bootstrap lock",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := seedStore.DB().ExecContext(ctx, `DELETE FROM workspace_security_settings WHERE workspace_id = ?`, "ws-migration-bootstrap-lock"); err != nil {
		t.Fatalf("delete seeded workspace security settings: %v", err)
	}

	stores := []*sqlite.Store{
		newMigrationLockStore(t, dbPath),
		newMigrationLockStore(t, dbPath),
	}

	runConcurrentApplyMigrations(t, ctx, stores)

	var settingsCount int
	if err := stores[0].DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM workspace_security_settings WHERE workspace_id = ?`, "ws-migration-bootstrap-lock").Scan(&settingsCount); err != nil {
		t.Fatalf("count bootstrapped workspace security settings: %v", err)
	}
	if settingsCount != 1 {
		t.Fatalf("expected exactly one bootstrapped workspace security settings row, got %d", settingsCount)
	}
}

func newMigrationLockStore(t *testing.T, dbPath string) *sqlite.Store {
	t.Helper()

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func runConcurrentApplyMigrations(t *testing.T, ctx context.Context, stores []*sqlite.Store) {
	t.Helper()

	start := make(chan struct{})
	errCh := make(chan error, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		wg.Add(1)
		go func(i int, store *sqlite.Store) {
			defer wg.Done()
			<-start
			if err := store.ApplyMigrations(ctx); err != nil {
				errCh <- fmt.Errorf("store %d: %w", i, err)
			}
		}(i, store)
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}
