package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHumanTelegramUserIDUniquenessAndLookupConflict(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: "ws-human-telegram",
		Title:       "Human Telegram",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	alice, err := store.RegisterHuman(ctx, HumanRegisterInput{
		WorkspaceID:       "ws-human-telegram",
		WorkspacePassword: DefaultWorkspacePassword,
		Username:          "alice",
		DisplayName:       "Alice",
		Password:          "alice-password",
	})
	if err != nil {
		t.Fatalf("register alice: %v", err)
	}
	bob, err := store.RegisterHuman(ctx, HumanRegisterInput{
		WorkspaceID:       "ws-human-telegram",
		WorkspacePassword: DefaultWorkspacePassword,
		Username:          "bob",
		DisplayName:       "Bob",
		Password:          "bob-password",
	})
	if err != nil {
		t.Fatalf("register bob: %v", err)
	}

	telegramID := int64(424242)
	if _, err := store.UpdateHumanProfile(ctx, HumanProfileUpdateInput{
		WorkspaceID:    "ws-human-telegram",
		UserID:         alice.UserID,
		TelegramUserID: &telegramID,
	}); err != nil {
		t.Fatalf("link alice telegram id: %v", err)
	}

	profile, err := store.GetHumanProfileByTelegramID(ctx, "ws-human-telegram", telegramID)
	if err != nil {
		t.Fatalf("get human by telegram id: %v", err)
	}
	if profile.UserID != alice.UserID {
		t.Fatalf("expected alice by telegram id, got %+v", profile)
	}

	if _, err := store.UpdateHumanProfile(ctx, HumanProfileUpdateInput{
		WorkspaceID:    "ws-human-telegram",
		UserID:         bob.UserID,
		TelegramUserID: &telegramID,
	}); !errors.Is(err, ErrHumanTelegramUserIDConflict) {
		t.Fatalf("expected telegram id conflict, got %v", err)
	}

	clearedID := int64(0)
	if _, err := store.UpdateHumanProfile(ctx, HumanProfileUpdateInput{
		WorkspaceID:    "ws-human-telegram",
		UserID:         alice.UserID,
		TelegramUserID: &clearedID,
	}); err != nil {
		t.Fatalf("clear alice telegram id: %v", err)
	}

	if _, err := store.GetHumanProfileByTelegramID(ctx, "ws-human-telegram", telegramID); !errors.Is(err, ErrHumanNotFound) {
		t.Fatalf("expected telegram lookup to be cleared, got %v", err)
	}

	if _, err := store.UpdateHumanProfile(ctx, HumanProfileUpdateInput{
		WorkspaceID:    "ws-human-telegram",
		UserID:         bob.UserID,
		TelegramUserID: &telegramID,
	}); err != nil {
		t.Fatalf("link bob telegram id after clear: %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `DROP INDEX IF EXISTS idx_workspace_humans_telegram_user_id`); err != nil {
		t.Fatalf("drop telegram index: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx,
		`UPDATE workspace_humans
		    SET telegram_user_id = ?, updated_at = ?
		  WHERE workspace_id = ? AND human_id = ?`,
		telegramID, now, "ws-human-telegram", alice.UserID,
	); err != nil {
		t.Fatalf("reintroduce duplicate telegram id: %v", err)
	}

	if _, err := store.GetHumanProfileByTelegramID(ctx, "ws-human-telegram", telegramID); !errors.Is(err, ErrHumanTelegramUserIDConflict) {
		t.Fatalf("expected duplicate telegram id lookup conflict, got %v", err)
	}
}
