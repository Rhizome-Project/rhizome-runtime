package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func seedMessagingWorkspaceAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	var workspaceCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&workspaceCount); err != nil {
		t.Fatalf("count workspaces for %s: %v", workspaceID, err)
	}
	if workspaceCount == 0 {
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       workspaceID,
			CreatedBy:   "tests",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
	}

	var authorityCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_authority WHERE workspace_id = ? AND scope = ? AND status = ?`, workspaceID, "workspace", "ACTIVE").Scan(&authorityCount); err != nil {
		t.Fatalf("count workspace authority for %s: %v", workspaceID, err)
	}
	if authorityCount == 0 {
		claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	}
}

func TestListWorkspaceMessagesUnaffectedByAgentScopedAcks(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-workspace-messages"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	if err != nil {
		t.Fatalf("send broadcast: %v", err)
	}
	directID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})
	if err != nil {
		t.Fatalf("send direct: %v", err)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, directID})
	if err != nil {
		t.Fatalf("ack messages: %v", err)
	}
	if acknowledged != 2 {
		t.Fatalf("expected acknowledged=2, got %d", acknowledged)
	}

	messages, err := store.ListWorkspaceMessages(ctx, workspaceID, "", 10)
	if err != nil {
		t.Fatalf("list workspace messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 workspace messages after agent-scoped ack, got %d", len(messages))
	}
	if messages[0].MessageID != directID || messages[1].MessageID != broadcastID {
		t.Fatalf("unexpected workspace message order/content: %+v", messages)
	}
	if messages[0].ReadAt != "" || messages[1].ReadAt != "" {
		t.Fatalf("workspace message read_at should remain global/empty, got %+v", messages)
	}
}

func TestPollMessagesDoesNotExposeReadStateAfterScopedAckSplit(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-read-shape"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "hello",
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	if messageID == "" {
		t.Fatal("expected message id")
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 polled message, got %d", len(messages))
	}
	if messages[0].MessageID != messageID {
		t.Fatalf("expected %s, got %+v", messageID, messages[0])
	}
	if messages[0].ReadAt != "" {
		t.Fatalf("poll should not surface global read state, got read_at=%q", messages[0].ReadAt)
	}
}

func TestPollMessagesStillHonorsLegacyGlobalReadAt(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-legacy-read-at"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "legacy-read",
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET read_at = ? WHERE workspace_id = ? AND message_id = ?`,
		"2026-03-20T09:25:00Z", workspaceID, messageID,
	); err != nil {
		t.Fatalf("set legacy read_at: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll messages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected legacy globally-read message to stay hidden, got %+v", messages)
	}
}

func TestAckMessagesDoesNotCountLegacyGlobalReadAtRows(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-legacy-read-at"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "legacy-read",
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET read_at = ? WHERE workspace_id = ? AND message_id = ?`,
		"2026-03-20T09:25:00Z", workspaceID, messageID,
	); err != nil {
		t.Fatalf("set legacy read_at: %v", err)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{messageID})
	if err != nil {
		t.Fatalf("ack legacy globally-read message: %v", err)
	}
	if acknowledged != 0 {
		t.Fatalf("expected acknowledged=0 for legacy globally-read row, got %d", acknowledged)
	}
}

func TestPollMessagesRejectsCompositeCursorWithMismatchedTimestamp(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-mismatched-composite-cursor"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "hello",
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	actualTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		actualTimestamp, messageID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sqlite.EncodeMessageCursor("2026-03-20T13:25:01.000000000Z", messageID), 20, 24)
	if err == nil {
		t.Fatalf("expected mismatched composite cursor error, got messages=%+v", messages)
	}
	if !errors.Is(err, sqlite.ErrInvalidPollCursor) || err.Error() != "after_created_at must be a valid poll cursor" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPollMessagesRejectsCompositeCursorForHiddenMessage(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-hidden-composite-cursor"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	hiddenID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-c",
		Content:     "hidden",
	})
	if err != nil {
		t.Fatalf("send hidden message: %v", err)
	}
	visibleID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "visible",
	})
	if err != nil {
		t.Fatalf("send visible message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, hiddenID, visibleID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sqlite.EncodeMessageCursor(sameTimestamp, hiddenID), 20, 24)
	if err == nil {
		t.Fatalf("expected hidden composite cursor error, got messages=%+v", messages)
	}
	if !errors.Is(err, sqlite.ErrInvalidPollCursor) || err.Error() != "after_created_at must be a valid poll cursor" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPollMessagesRejectsCompositeCursorWithUnknownMessageID(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-invalid-composite-cursor"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "hello",
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id = ?`,
		sameTimestamp, messageID,
	); err != nil {
		t.Fatalf("force created_at: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sqlite.EncodeMessageCursor(sameTimestamp, "msg-missing"), 20, 24)
	if err == nil {
		t.Fatalf("expected invalid composite cursor error, got messages=%+v", messages)
	}
	if !errors.Is(err, sqlite.ErrInvalidPollCursor) || err.Error() != "after_created_at must be a valid poll cursor" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPollMessagesRejectsInvalidCursorFormat(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-invalid-cursor"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "hello",
	}); err != nil {
		t.Fatalf("send message: %v", err)
	}

	for _, cursor := range []string{"not-a-time", "|msg-1", "2026-13-20T13:25:00Z|msg-1", "2026-03-20T13:25:00Z|"} {
		t.Run(cursor, func(t *testing.T) {
			messages, err := store.PollMessages(ctx, workspaceID, "agent-b", cursor, 20, 24)
			if err == nil {
				t.Fatalf("expected invalid cursor error, got messages=%+v", messages)
			}
			if !errors.Is(err, sqlite.ErrInvalidPollCursor) || err.Error() != "after_created_at must be a valid poll cursor" {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestPollMessagesTimestampCursorShimReturnsWholeSameTimestampBatch(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-timestamp-cursor-batch"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sameTimestamp, 1, 24)
	if err != nil {
		t.Fatalf("poll with timestamp-only cursor and tight limit: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected full same-timestamp batch despite limit=1, got %+v", messages)
	}
	if messages[0].MessageID != firstID || messages[1].MessageID != secondID {
		t.Fatalf("unexpected same-timestamp batch order/content: %+v", messages)
	}
}

func TestPollMessagesTimestampCursorShimRedeliversUnackedSameTimestampRows(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-timestamp-cursor-redelivery"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	firstPoll, err := store.PollMessages(ctx, workspaceID, "agent-b", sameTimestamp, 50, 24)
	if err != nil {
		t.Fatalf("first poll with timestamp-only cursor: %v", err)
	}
	if len(firstPoll) != 2 {
		t.Fatalf("expected 2 messages from timestamp-only shim, got %+v", firstPoll)
	}

	secondPoll, err := store.PollMessages(ctx, workspaceID, "agent-b", sameTimestamp, 50, 24)
	if err != nil {
		t.Fatalf("second poll with timestamp-only cursor: %v", err)
	}
	if len(secondPoll) != 2 {
		t.Fatalf("expected same 2 messages to be redelivered, got %+v", secondPoll)
	}
	if secondPoll[0].MessageID != firstID || secondPoll[1].MessageID != secondID {
		t.Fatalf("unexpected redelivery order/content: %+v", secondPoll)
	}
}

func TestPollMessagesCompositeCursorCanAdvancePastLegacyReadSameTimestampMessage(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-composite-cursor-legacy-read"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET read_at = ? WHERE message_id = ?`,
		"2026-03-20T13:26:00.000000000Z", firstID,
	); err != nil {
		t.Fatalf("legacy-read first message: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sqlite.EncodeMessageCursor(sameTimestamp, firstID), 50, 24)
	if err != nil {
		t.Fatalf("poll with composite cursor on legacy-read message: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 same-timestamp follower after legacy-read composite cursor, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[0].Content != "second" {
		t.Fatalf("unexpected composite cursor result after legacy-read predecessor: %+v", messages)
	}
}

func TestPollMessagesCompositeCursorCanAdvancePastAckedSameTimestampMessage(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-composite-cursor-ack"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{firstID})
	if err != nil {
		t.Fatalf("ack first message: %v", err)
	}
	if acknowledged != 1 {
		t.Fatalf("expected acknowledged=1, got %d", acknowledged)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sqlite.EncodeMessageCursor(sameTimestamp, firstID), 50, 24)
	if err != nil {
		t.Fatalf("poll with composite cursor on acked message: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 same-timestamp follower after acked composite cursor, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[0].Content != "second" {
		t.Fatalf("unexpected composite cursor result after acked predecessor: %+v", messages)
	}
}

func TestPollMessagesTimestampCursorFillsRemainingLimitWithNewerRows(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-timestamp-cursor-fill"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}
	thirdID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "third",
	})
	if err != nil {
		t.Fatalf("send third message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	newerTimestamp := "2026-03-20T13:25:01.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = CASE message_id
			WHEN ? THEN ?
			WHEN ? THEN ?
			WHEN ? THEN ?
		END WHERE message_id IN (?, ?, ?)`,
		firstID, sameTimestamp,
		secondID, sameTimestamp,
		thirdID, newerTimestamp,
		firstID, secondID, thirdID,
	); err != nil {
		t.Fatalf("force created_at values: %v", err)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{firstID})
	if err != nil {
		t.Fatalf("ack first message: %v", err)
	}
	if acknowledged != 1 {
		t.Fatalf("expected acknowledged=1, got %d", acknowledged)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sameTimestamp, 2, 24)
	if err != nil {
		t.Fatalf("poll with timestamp cursor fill: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after fill, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[1].MessageID != thirdID {
		t.Fatalf("unexpected timestamp cursor fill order/content: %+v", messages)
	}
}

func TestPollMessagesTimestampCursorCanAdvanceViaAckedSameTimestampRows(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-timestamp-cursor-ack"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{firstID})
	if err != nil {
		t.Fatalf("ack first message: %v", err)
	}
	if acknowledged != 1 {
		t.Fatalf("expected acknowledged=1, got %d", acknowledged)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sameTimestamp, 50, 24)
	if err != nil {
		t.Fatalf("poll with timestamp-only cursor: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 same-timestamp follower with timestamp-only cursor, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[0].Content != "second" {
		t.Fatalf("unexpected timestamp-only cursor result: %+v", messages)
	}
}

func TestPollMessagesCompositeCursorUsesInsertionOrderNotLexicalMessageID(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-composite-rowid"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}
	thirdID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "third",
	})
	if err != nil {
		t.Fatalf("send third message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?, ?)`,
		sameTimestamp, firstID, secondID, thirdID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages
		 SET message_id = CASE message_id
		   WHEN ? THEN 'msg-shared-2'
		   WHEN ? THEN 'msg-shared-10'
		   WHEN ? THEN 'msg-shared-11'
		 END
		 WHERE message_id IN (?, ?, ?)`,
		firstID, secondID, thirdID,
		firstID, secondID, thirdID,
	); err != nil {
		t.Fatalf("rewrite message ids for lexical trap: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", sqlite.EncodeMessageCursor(sameTimestamp, "msg-shared-2"), 50, 24)
	if err != nil {
		t.Fatalf("poll with composite cursor: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 same-timestamp followers after composite cursor, got %+v", messages)
	}
	if messages[0].MessageID != "msg-shared-10" || messages[1].MessageID != "msg-shared-11" {
		t.Fatalf("unexpected rowid-ordered followers: %+v", messages)
	}
}

func TestPollMessagesCompositeCursorReturnsSameTimestampFollowers(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-poll-composite-cursor"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "first",
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	secondID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "second",
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}

	sameTimestamp := "2026-03-20T13:25:00.000000000Z"
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE message_id IN (?, ?)`,
		sameTimestamp, firstID, secondID,
	); err != nil {
		t.Fatalf("force same created_at: %v", err)
	}

	legacyCursorMessages, err := store.PollMessages(ctx, workspaceID, "agent-b", sameTimestamp, 50, 24)
	if err != nil {
		t.Fatalf("poll with legacy cursor: %v", err)
	}
	if len(legacyCursorMessages) != 2 {
		t.Fatalf("expected legacy timestamp-only cursor shim to return same-timestamp rows, got %+v", legacyCursorMessages)
	}

	cursor := sqlite.EncodeMessageCursor(sameTimestamp, firstID)
	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", cursor, 50, 24)
	if err != nil {
		t.Fatalf("poll with composite cursor: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 same-timestamp follower with composite cursor, got %+v", messages)
	}
	if messages[0].MessageID != secondID || messages[0].Content != "second" {
		t.Fatalf("unexpected composite-cursor result: %+v", messages)
	}
}

func TestAckMessagesNormalizesDuplicateAndBlankIDs(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-normalize"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "normalize",
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{"", "  ", messageID, messageID, "\t" + messageID + "\n"})
	if err != nil {
		t.Fatalf("ack messages: %v", err)
	}
	if acknowledged != 1 {
		t.Fatalf("expected acknowledged=1 after normalization, got %d", acknowledged)
	}

	afterAck, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll after ack: %v", err)
	}
	if len(afterAck) != 0 {
		t.Fatalf("expected no visible messages after normalized ack, got %+v", afterAck)
	}
}

func TestClaimedProcessingRequestUsesClaimTimeForExpiry(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-request-claim-time"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "slow",
		Payload:     `{"n":1}`,
		TimeoutSec:  5,
	})
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_requests SET created_at = ? WHERE request_id = ?`,
		time.Now().UTC().Add(-20*time.Second).Format(time.RFC3339Nano), requestID,
	); err != nil {
		t.Fatalf("age queued request: %v", err)
	}

	claimed, err := store.ListPendingAgentRequests(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("claim pending requests: %v", err)
	}
	if len(claimed) != 1 || claimed[0].RequestID != requestID || claimed[0].Status != "PROCESSING" {
		t.Fatalf("expected one claimed PROCESSING request, got %+v", claimed)
	}

	expired, err := store.ExpireRequests(ctx)
	if err != nil {
		t.Fatalf("expire requests: %v", err)
	}
	if expired != 0 {
		t.Fatalf("expected claimed request to use fresh claim time, got expired=%d", expired)
	}

	result, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get request result: %v", err)
	}
	if result.Status != "PROCESSING" {
		t.Fatalf("expected PROCESSING after immediate expire check, got %+v", result)
	}
	if result.RespondedAt != "" {
		t.Fatalf("expected claim timestamp to stay internal while processing, got %+v", result)
	}
}

func TestExpireRequestsTimesOutClaimedProcessingRows(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-request-timeout"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "slow",
		Payload:     `{"n":1}`,
		TimeoutSec:  1,
	})
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	claimed, err := store.ListPendingAgentRequests(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("claim pending requests: %v", err)
	}
	if len(claimed) != 1 || claimed[0].RequestID != requestID || claimed[0].Status != "PROCESSING" {
		t.Fatalf("expected one claimed PROCESSING request, got %+v", claimed)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_requests SET responded_at = ? WHERE request_id = ?`,
		time.Now().UTC().Add(-3*time.Second).Format(time.RFC3339Nano), requestID,
	); err != nil {
		t.Fatalf("age claimed request: %v", err)
	}

	expired, err := store.ExpireRequests(ctx)
	if err != nil {
		t.Fatalf("expire requests: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected expired=1, got %d", expired)
	}

	result, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get request result: %v", err)
	}
	if result.Status != "TIMEOUT" {
		t.Fatalf("expected TIMEOUT after expiring claimed request, got %+v", result)
	}

	claimedAgain, err := store.ListPendingAgentRequests(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("claim pending requests again: %v", err)
	}
	if len(claimedAgain) != 1 || claimedAgain[0].RequestID != requestID || claimedAgain[0].Status != "PROCESSING" {
		t.Fatalf("expected timed-out unanswered request to be reclaimable, got %+v", claimedAgain)
	}

	if err := store.RespondAgentRequest(ctx, requestID, `{"recovered":true}`); err != nil {
		t.Fatalf("respond reclaimed request: %v", err)
	}
	result, err = store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get recovered request result: %v", err)
	}
	if result.Status != "COMPLETED" || result.Response != `{"recovered":true}` {
		t.Fatalf("expected recovered completed request, got %+v", result)
	}
}

func TestRespondAgentRequestAcceptsUnansweredTimedOutRequest(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-request-timeout-late-response"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "late.response",
		Payload:     `{"n":1}`,
		TimeoutSec:  1,
	})
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	claimed, err := store.ListPendingAgentRequests(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("claim request: %v", err)
	}
	if len(claimed) != 1 || claimed[0].RequestID != requestID {
		t.Fatalf("expected claimed request %s, got %+v", requestID, claimed)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_requests SET responded_at = ? WHERE request_id = ?`,
		time.Now().UTC().Add(-3*time.Second).Format(time.RFC3339Nano), requestID,
	); err != nil {
		t.Fatalf("age claimed request: %v", err)
	}
	expired, err := store.ExpireRequests(ctx)
	if err != nil {
		t.Fatalf("expire requests: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected expired=1, got %d", expired)
	}

	if err := store.RespondAgentRequest(ctx, requestID, `{"late":true}`); err != nil {
		t.Fatalf("late response should complete unanswered timeout: %v", err)
	}
	result, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get request result: %v", err)
	}
	if result.Status != "COMPLETED" || result.Response != `{"late":true}` {
		t.Fatalf("expected late completed request, got %+v", result)
	}
}

func TestListPendingAgentRequestsClaimsRequestsOnce(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-request-claim"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	firstID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "first",
		Payload:     `{"n":1}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("create first request: %v", err)
	}
	secondID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "second",
		Payload:     `{"n":2}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("create second request: %v", err)
	}

	open, err := store.ListOpenAgentRequests(ctx, workspaceID, "agent-b", 100)
	if err != nil {
		t.Fatalf("list open requests: %v", err)
	}
	if len(open) != 2 || open[0].Status != "PENDING" || open[1].Status != "PENDING" {
		t.Fatalf("expected read-only open request list to preserve pending rows, got %+v", open)
	}
	unclaimed, err := store.GetAgentRequestResult(ctx, firstID)
	if err != nil {
		t.Fatalf("get first open request: %v", err)
	}
	if unclaimed.Status != "PENDING" {
		t.Fatalf("open request inspection must not claim the row, got %+v", unclaimed)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_requests SET status = 'TIMEOUT', response = NULL, responded_at = ? WHERE request_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano), secondID,
	); err != nil {
		t.Fatalf("mark second request timeout: %v", err)
	}
	open, err = store.ListOpenAgentRequests(ctx, workspaceID, "agent-b", 100)
	if err != nil {
		t.Fatalf("list open requests after timeout: %v", err)
	}
	if len(open) != 2 || open[0].RequestID != firstID || open[0].Status != "PENDING" || open[1].RequestID != secondID || open[1].Status != "TIMEOUT" {
		t.Fatalf("expected read-only open request list to include recoverable timeout rows, got %+v", open)
	}

	claimed, err := store.ListPendingAgentRequests(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("claim pending requests: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("expected 2 claimed requests, got %+v", claimed)
	}
	if claimed[0].RequestID != firstID || claimed[1].RequestID != secondID {
		t.Fatalf("unexpected claimed request order/content: %+v", claimed)
	}
	if claimed[0].Status != "PROCESSING" || claimed[1].Status != "PROCESSING" {
		t.Fatalf("expected claimed requests to be PROCESSING, got %+v", claimed)
	}

	claimedAgain, err := store.ListPendingAgentRequests(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("claim pending requests again: %v", err)
	}
	if len(claimedAgain) != 0 {
		t.Fatalf("expected second claim to be empty, got %+v", claimedAgain)
	}

	if err := store.RespondAgentRequest(ctx, firstID, `{"done":true}`); err != nil {
		t.Fatalf("respond first claimed request: %v", err)
	}
	result, err := store.GetAgentRequestResult(ctx, firstID)
	if err != nil {
		t.Fatalf("get first request result: %v", err)
	}
	if result.Status != "COMPLETED" {
		t.Fatalf("expected completed request after respond, got %+v", result)
	}
}

func TestAgentRequestStoreKeepsTrimmedIDsAndDefaultMethodAlignedAcrossClaimAndResult(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-request-store-trim-default"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: "  " + workspaceID + "  ",
		FromAgentID: "  agent-a  ",
		ToAgentID:   "  agent-b  ",
		Method:      "   ",
		Payload:     `{"n":1}`,
		TimeoutSec:  30,
	})
	if err != nil {
		t.Fatalf("create normalized request: %v", err)
	}

	claimed, err := store.ListPendingAgentRequests(ctx, workspaceID, "agent-b")
	if err != nil {
		t.Fatalf("claim pending requests: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed request, got %+v", claimed)
	}
	if claimed[0].RequestID != requestID {
		t.Fatalf("expected claimed request_id %q, got %+v", requestID, claimed)
	}
	if claimed[0].WorkspaceID != workspaceID || claimed[0].FromAgentID != "agent-a" || claimed[0].ToAgentID != "agent-b" {
		t.Fatalf("expected claimed trimmed ids, got %+v", claimed[0])
	}
	if claimed[0].Method != "default" {
		t.Fatalf("expected claimed default method, got %+v", claimed[0])
	}
	if claimed[0].Status != "PROCESSING" {
		t.Fatalf("expected PROCESSING claimed request, got %+v", claimed[0])
	}

	result, err := store.GetAgentRequestResult(ctx, requestID)
	if err != nil {
		t.Fatalf("get request result: %v", err)
	}
	if result.RequestID != requestID {
		t.Fatalf("expected result request_id %q, got %+v", requestID, result)
	}
	if result.WorkspaceID != workspaceID || result.FromAgentID != "agent-a" || result.ToAgentID != "agent-b" {
		t.Fatalf("expected result trimmed ids, got %+v", result)
	}
	if result.Method != "default" {
		t.Fatalf("expected result default method, got %+v", result)
	}
	if result.Status != "PROCESSING" {
		t.Fatalf("expected PROCESSING result after claim, got %+v", result)
	}
}

func TestSendMessageAppendsRuntimeEventWhenWorkspaceExists(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-message-runtime-event"
		fromAgentID = "agent-a"
		toAgentID   = "agent-b"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Messaging Runtime Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     fromAgentID,
		OwnerUserID: "developer",
		DisplayName: "Agent A",
	}); err != nil {
		t.Fatalf("register sender: %v", err)
	}

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Channel:     "ops",
		ContentType: "application/json",
		Content:     `{"hello":"world"}`,
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 runtime event, got %+v", events)
	}
	event := events[0]
	if event.AgentID != fromAgentID || event.ActorID != fromAgentID {
		t.Fatalf("expected sender agent/actor ids, got %+v", event)
	}
	assertMessagingRuntimeEventPayload(t, event.PayloadJSON, map[string]string{
		"message_id":    messageID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"channel":       "ops",
		"content_type":  "application/json",
		"status":        "SENT",
	})
}

func TestMessagingWithEventReturnsPersistedRuntimeRows(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-messaging-with-event"
		fromAgentID = "agent-a"
		toAgentID   = "agent-b"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Messaging With Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{fromAgentID, toAgentID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	messageID, messageEvent, err := store.SendMessageWithEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Channel:     "ops",
		Content:     "hello",
	})
	if err != nil {
		t.Fatalf("send message with event: %v", err)
	}
	if messageEvent.EventID == "" {
		t.Fatalf("expected send message with event to return runtime row, got %+v", messageEvent)
	}
	messageEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list message runtime events: %v", err)
	}
	if len(messageEvents) != 1 {
		t.Fatalf("expected 1 message runtime event, got %+v", messageEvents)
	}
	if messageEvents[0].EventID != messageEvent.EventID || messageEvents[0].IngestSeq != messageEvent.IngestSeq {
		t.Fatalf("expected returned message runtime row to match persisted row, got returned=%+v persisted=%+v", messageEvent, messageEvents[0])
	}

	requestID, requestEvent, err := store.CreateAgentRequestWithEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      "call.with-event",
		Payload:     `{"ok":true}`,
		TimeoutSec:  45,
	})
	if err != nil {
		t.Fatalf("create agent request with event: %v", err)
	}
	if requestEvent.EventID == "" {
		t.Fatalf("expected create agent request with event to return runtime row, got %+v", requestEvent)
	}
	requestEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list request runtime events: %v", err)
	}
	if len(requestEvents) != 1 {
		t.Fatalf("expected 1 request runtime event, got %+v", requestEvents)
	}
	if requestEvents[0].EventID != requestEvent.EventID || requestEvents[0].IngestSeq != requestEvent.IngestSeq {
		t.Fatalf("expected returned request runtime row to match persisted row, got returned=%+v persisted=%+v", requestEvent, requestEvents[0])
	}

	responseEvent, err := store.RespondAgentRequestWithEvent(ctx, requestID, `{"done":true}`)
	if err != nil {
		t.Fatalf("respond agent request with event: %v", err)
	}
	if responseEvent.EventID == "" {
		t.Fatalf("expected respond agent request with event to return runtime row, got %+v", responseEvent)
	}
	responseEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list response runtime events: %v", err)
	}
	if len(responseEvents) != 1 {
		t.Fatalf("expected 1 response runtime event, got %+v", responseEvents)
	}
	if responseEvents[0].EventID != responseEvent.EventID || responseEvents[0].IngestSeq != responseEvent.IngestSeq {
		t.Fatalf("expected returned response runtime row to match persisted row, got returned=%+v persisted=%+v", responseEvent, responseEvents[0])
	}
}

func TestAgentRequestLifecycleAppendsRuntimeEventsWhenWorkspaceExists(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-request-runtime-event"
		fromAgentID = "agent-a"
		toAgentID   = "agent-b"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Request Runtime Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{fromAgentID, toAgentID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		Method:      "call.runtime",
		Payload:     `{"ok":true}`,
		TimeoutSec:  45,
	})
	if err != nil {
		t.Fatalf("create agent request: %v", err)
	}

	requestEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list request runtime events: %v", err)
	}
	if len(requestEvents) != 1 {
		t.Fatalf("expected 1 request runtime event, got %+v", requestEvents)
	}
	if requestEvents[0].AgentID != fromAgentID || requestEvents[0].ActorID != fromAgentID {
		t.Fatalf("expected requester agent/actor ids, got %+v", requestEvents[0])
	}
	assertMessagingRuntimeEventPayload(t, requestEvents[0].PayloadJSON, map[string]string{
		"request_id":    requestID,
		"from":          fromAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "PENDING",
	})

	if err := store.RespondAgentRequest(ctx, requestID, `{"done":true}`); err != nil {
		t.Fatalf("respond agent request: %v", err)
	}

	responseEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list response runtime events: %v", err)
	}
	if len(responseEvents) != 1 {
		t.Fatalf("expected 1 response runtime event, got %+v", responseEvents)
	}
	if responseEvents[0].AgentID != toAgentID || responseEvents[0].ActorID != toAgentID {
		t.Fatalf("expected responder agent/actor ids, got %+v", responseEvents[0])
	}
	assertMessagingRuntimeEventPayload(t, responseEvents[0].PayloadJSON, map[string]string{
		"request_id":    requestID,
		"from":          toAgentID,
		"from_agent_id": fromAgentID,
		"to_agent_id":   toAgentID,
		"method":        "call.runtime",
		"status":        "COMPLETED",
	})
}

func assertMessagingRuntimeEventPayload(t *testing.T, payloadJSON string, want map[string]string) {
	t.Helper()

	var payload map[string]string
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal runtime event payload: %v", err)
	}
	for key, expected := range want {
		if payload[key] != expected {
			t.Fatalf("expected payload[%q]=%q, got %q in %+v", key, expected, payload[key], payload)
		}
	}
}

func TestPollAndAckShareInboxVisibilityRules(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-inbox-visibility"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	if err != nil {
		t.Fatalf("send broadcast: %v", err)
	}
	selfSentID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-b",
		ToAgentID:   "agent-c",
		Content:     "self-sent",
	})
	if err != nil {
		t.Fatalf("send self-sent: %v", err)
	}
	directVisibleID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct-visible",
	})
	if err != nil {
		t.Fatalf("send direct-visible: %v", err)
	}
	hiddenID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-c",
		Content:     "hidden",
	})
	if err != nil {
		t.Fatalf("send hidden: %v", err)
	}

	messages, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll messages: %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("expected 3 visible messages, got %+v", messages)
	}
	if messages[0].MessageID != broadcastID || messages[1].MessageID != selfSentID || messages[2].MessageID != directVisibleID {
		t.Fatalf("unexpected visible message set/order: %+v", messages)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, selfSentID, directVisibleID, hiddenID})
	if err != nil {
		t.Fatalf("ack messages: %v", err)
	}
	if acknowledged != 3 {
		t.Fatalf("expected acknowledged=3, got %d", acknowledged)
	}

	afterAck, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll after ack: %v", err)
	}
	if len(afterAck) != 0 {
		t.Fatalf("expected no remaining visible messages after ack, got %+v", afterAck)
	}
}

func TestAckMessagesBySenderDoesNotHideMessagesForRecipient(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-sender-scope"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	if err != nil {
		t.Fatalf("send broadcast: %v", err)
	}
	directID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})
	if err != nil {
		t.Fatalf("send direct: %v", err)
	}

	beforeAckSender, err := store.PollMessages(ctx, workspaceID, "agent-a", "", 10, 24)
	if err != nil {
		t.Fatalf("poll sender messages before ack: %v", err)
	}
	if len(beforeAckSender) != 2 {
		t.Fatalf("expected 2 self-visible sender messages, got %+v", beforeAckSender)
	}

	acknowledged, err := store.AckMessages(ctx, workspaceID, "agent-a", []string{broadcastID, directID})
	if err != nil {
		t.Fatalf("ack sender-visible messages: %v", err)
	}
	if acknowledged != 2 {
		t.Fatalf("expected acknowledged=2, got %d", acknowledged)
	}

	afterAckSender, err := store.PollMessages(ctx, workspaceID, "agent-a", "", 10, 24)
	if err != nil {
		t.Fatalf("poll sender after ack: %v", err)
	}
	if len(afterAckSender) != 0 {
		t.Fatalf("expected sender inbox empty after self-ack, got %+v", afterAckSender)
	}

	afterAckRecipient, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll recipient after sender ack: %v", err)
	}
	if len(afterAckRecipient) != 2 {
		t.Fatalf("expected recipient to keep 2 visible messages, got %+v", afterAckRecipient)
	}
	if afterAckRecipient[0].MessageID != broadcastID || afterAckRecipient[1].MessageID != directID {
		t.Fatalf("recipient messages changed after sender ack: %+v", afterAckRecipient)
	}
}

func TestAckMessagesTracksSenderAndRecipientAcksSeparately(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-two-agent-sequence"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	if err != nil {
		t.Fatalf("send broadcast: %v", err)
	}
	directID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})
	if err != nil {
		t.Fatalf("send direct: %v", err)
	}

	senderAcked, err := store.AckMessages(ctx, workspaceID, "agent-a", []string{broadcastID, directID})
	if err != nil {
		t.Fatalf("ack sender-visible messages: %v", err)
	}
	if senderAcked != 2 {
		t.Fatalf("expected sender acknowledged=2, got %d", senderAcked)
	}

	recipientVisible, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll recipient after sender ack: %v", err)
	}
	if len(recipientVisible) != 2 {
		t.Fatalf("expected recipient to keep 2 visible messages, got %+v", recipientVisible)
	}

	recipientAcked, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, directID})
	if err != nil {
		t.Fatalf("ack recipient-visible messages: %v", err)
	}
	if recipientAcked != 2 {
		t.Fatalf("expected recipient acknowledged=2 after sender ack, got %d", recipientAcked)
	}

	recipientAfterAck, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll recipient after recipient ack: %v", err)
	}
	if len(recipientAfterAck) != 0 {
		t.Fatalf("expected recipient inbox empty after ack, got %+v", recipientAfterAck)
	}

	senderAckedAgain, err := store.AckMessages(ctx, workspaceID, "agent-a", []string{broadcastID, directID})
	if err != nil {
		t.Fatalf("re-ack sender-visible messages: %v", err)
	}
	if senderAckedAgain != 0 {
		t.Fatalf("expected sender re-ack acknowledged=0, got %d", senderAckedAgain)
	}
}

func TestAckMessagesKeepsBroadcastAckScopedAcrossThreeAgents(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-three-agent-broadcast"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	if err != nil {
		t.Fatalf("send broadcast: %v", err)
	}

	senderAcked, err := store.AckMessages(ctx, workspaceID, "agent-a", []string{broadcastID})
	if err != nil {
		t.Fatalf("ack sender-visible broadcast: %v", err)
	}
	if senderAcked != 1 {
		t.Fatalf("expected sender acknowledged=1, got %d", senderAcked)
	}

	agentBVisible, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-b after sender ack: %v", err)
	}
	if len(agentBVisible) != 1 || agentBVisible[0].MessageID != broadcastID {
		t.Fatalf("expected agent-b to keep broadcast visible, got %+v", agentBVisible)
	}

	agentBAcked, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID})
	if err != nil {
		t.Fatalf("ack agent-b broadcast: %v", err)
	}
	if agentBAcked != 1 {
		t.Fatalf("expected agent-b acknowledged=1, got %d", agentBAcked)
	}

	agentCAfterBAck, err := store.PollMessages(ctx, workspaceID, "agent-c", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-c after sender+agent-b ack: %v", err)
	}
	if len(agentCAfterBAck) != 1 || agentCAfterBAck[0].MessageID != broadcastID {
		t.Fatalf("expected agent-c to keep broadcast visible, got %+v", agentCAfterBAck)
	}

	agentCAcked, err := store.AckMessages(ctx, workspaceID, "agent-c", []string{broadcastID})
	if err != nil {
		t.Fatalf("ack agent-c broadcast: %v", err)
	}
	if agentCAcked != 1 {
		t.Fatalf("expected agent-c acknowledged=1, got %d", agentCAcked)
	}

	agentCAfterAck, err := store.PollMessages(ctx, workspaceID, "agent-c", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-c after ack: %v", err)
	}
	if len(agentCAfterAck) != 0 {
		t.Fatalf("expected agent-c inbox empty after ack, got %+v", agentCAfterAck)
	}
}

func TestAckMessagesSeparatesOldBroadcastFromFreshDirect(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-broadcast-then-direct"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast-old",
	})
	if err != nil {
		t.Fatalf("send old broadcast: %v", err)
	}

	senderAcked, err := store.AckMessages(ctx, workspaceID, "agent-a", []string{broadcastID})
	if err != nil {
		t.Fatalf("ack sender broadcast: %v", err)
	}
	if senderAcked != 1 {
		t.Fatalf("expected sender acknowledged=1, got %d", senderAcked)
	}

	agentBAckedOld, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID})
	if err != nil {
		t.Fatalf("ack agent-b old broadcast: %v", err)
	}
	if agentBAckedOld != 1 {
		t.Fatalf("expected agent-b old broadcast acknowledged=1, got %d", agentBAckedOld)
	}

	directID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct-fresh",
	})
	if err != nil {
		t.Fatalf("send fresh direct: %v", err)
	}

	agentBVisible, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-b after fresh direct: %v", err)
	}
	if len(agentBVisible) != 1 || agentBVisible[0].MessageID != directID {
		t.Fatalf("expected agent-b to see only fresh direct, got %+v", agentBVisible)
	}

	agentCVisible, err := store.PollMessages(ctx, workspaceID, "agent-c", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-c after fresh direct: %v", err)
	}
	if len(agentCVisible) != 1 || agentCVisible[0].MessageID != broadcastID {
		t.Fatalf("expected agent-c to see only old broadcast, got %+v", agentCVisible)
	}

	agentBAckedDirect, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{directID})
	if err != nil {
		t.Fatalf("ack agent-b fresh direct: %v", err)
	}
	if agentBAckedDirect != 1 {
		t.Fatalf("expected agent-b fresh direct acknowledged=1, got %d", agentBAckedDirect)
	}

	agentCAckedBroadcast, err := store.AckMessages(ctx, workspaceID, "agent-c", []string{broadcastID})
	if err != nil {
		t.Fatalf("ack agent-c old broadcast: %v", err)
	}
	if agentCAckedBroadcast != 1 {
		t.Fatalf("expected agent-c old broadcast acknowledged=1, got %d", agentCAckedBroadcast)
	}
}

func TestAckMessagesDuplicateMixedVisibilityDoesNotInflateCounts(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-duplicate-mixed-visibility"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	if err != nil {
		t.Fatalf("send broadcast: %v", err)
	}
	directID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "direct",
	})
	if err != nil {
		t.Fatalf("send direct: %v", err)
	}

	agentBAcked, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, directID})
	if err != nil {
		t.Fatalf("ack agent-b mixed visibility set: %v", err)
	}
	if agentBAcked != 2 {
		t.Fatalf("expected agent-b acknowledged=2, got %d", agentBAcked)
	}

	agentCAfterBAck, err := store.PollMessages(ctx, workspaceID, "agent-c", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-c after agent-b ack: %v", err)
	}
	if len(agentCAfterBAck) != 1 || agentCAfterBAck[0].MessageID != broadcastID {
		t.Fatalf("expected agent-c to keep only broadcast visible, got %+v", agentCAfterBAck)
	}

	agentCAcked, err := store.AckMessages(ctx, workspaceID, "agent-c", []string{broadcastID, broadcastID, directID, "missing-id", "", "  "})
	if err != nil {
		t.Fatalf("ack agent-c mixed duplicate set: %v", err)
	}
	if agentCAcked != 1 {
		t.Fatalf("expected agent-c acknowledged=1, got %d", agentCAcked)
	}

	agentCAckedAgain, err := store.AckMessages(ctx, workspaceID, "agent-c", []string{broadcastID, broadcastID, directID, "missing-id"})
	if err != nil {
		t.Fatalf("re-ack agent-c mixed duplicate set: %v", err)
	}
	if agentCAckedAgain != 0 {
		t.Fatalf("expected agent-c re-ack acknowledged=0, got %d", agentCAckedAgain)
	}

	agentBAckedAgain, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, directID, directID, "missing-id"})
	if err != nil {
		t.Fatalf("re-ack agent-b mixed duplicate set: %v", err)
	}
	if agentBAckedAgain != 0 {
		t.Fatalf("expected agent-b re-ack acknowledged=0, got %d", agentBAckedAgain)
	}
}

func TestAckMessagesSelfSentRowsRemainSeparatelyAckableForRecipient(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-self-sent-separate-scope"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "broadcast",
	})
	if err != nil {
		t.Fatalf("send broadcast: %v", err)
	}
	selfSentID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-b",
		ToAgentID:   "agent-c",
		Content:     "self-sent-to-c",
	})
	if err != nil {
		t.Fatalf("send self-sent: %v", err)
	}

	agentBAcked, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, selfSentID})
	if err != nil {
		t.Fatalf("ack agent-b sender-visible rows: %v", err)
	}
	if agentBAcked != 2 {
		t.Fatalf("expected agent-b acknowledged=2, got %d", agentBAcked)
	}

	agentCAfterBAck, err := store.PollMessages(ctx, workspaceID, "agent-c", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-c after agent-b self-ack: %v", err)
	}
	if len(agentCAfterBAck) != 2 {
		t.Fatalf("expected agent-c to keep broadcast+self-sent visible, got %+v", agentCAfterBAck)
	}
	if agentCAfterBAck[0].MessageID != broadcastID || agentCAfterBAck[1].MessageID != selfSentID {
		t.Fatalf("unexpected agent-c visible set after agent-b self-ack: %+v", agentCAfterBAck)
	}

	agentCAcked, err := store.AckMessages(ctx, workspaceID, "agent-c", []string{broadcastID, selfSentID, selfSentID, "missing-id", "", "  "})
	if err != nil {
		t.Fatalf("ack agent-c recipient-visible rows: %v", err)
	}
	if agentCAcked != 2 {
		t.Fatalf("expected agent-c acknowledged=2, got %d", agentCAcked)
	}

	agentCAckedAgain, err := store.AckMessages(ctx, workspaceID, "agent-c", []string{broadcastID, selfSentID, selfSentID, "missing-id"})
	if err != nil {
		t.Fatalf("re-ack agent-c recipient-visible rows: %v", err)
	}
	if agentCAckedAgain != 0 {
		t.Fatalf("expected agent-c re-ack acknowledged=0, got %d", agentCAckedAgain)
	}

	agentBAckedAgain, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, selfSentID, selfSentID, "missing-id"})
	if err != nil {
		t.Fatalf("re-ack agent-b sender-visible rows: %v", err)
	}
	if agentBAckedAgain != 0 {
		t.Fatalf("expected agent-b re-ack acknowledged=0, got %d", agentBAckedAgain)
	}
}

func TestAckMessagesSkipsLegacyHiddenBroadcastButKeepsVisibleDirectAckable(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-ack-legacy-hidden-mixed-visibility"
	seedMessagingWorkspaceAuthority(t, ctx, store, workspaceID)

	broadcastID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		Content:     "legacy-hidden-broadcast",
	})
	if err != nil {
		t.Fatalf("send legacy-hidden broadcast: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agent_messages SET read_at = ? WHERE workspace_id = ? AND message_id = ?`,
		"2026-03-20T09:25:00Z", workspaceID, broadcastID,
	); err != nil {
		t.Fatalf("set legacy read_at on broadcast: %v", err)
	}

	directID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "fresh-direct",
	})
	if err != nil {
		t.Fatalf("send fresh direct: %v", err)
	}

	agentBVisible, err := store.PollMessages(ctx, workspaceID, "agent-b", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-b mixed visibility: %v", err)
	}
	if len(agentBVisible) != 1 || agentBVisible[0].MessageID != directID {
		t.Fatalf("expected agent-b to see only fresh direct, got %+v", agentBVisible)
	}

	agentBAcked, err := store.AckMessages(ctx, workspaceID, "agent-b", []string{broadcastID, directID, directID, "missing-id"})
	if err != nil {
		t.Fatalf("ack agent-b mixed legacy/direct set: %v", err)
	}
	if agentBAcked != 1 {
		t.Fatalf("expected agent-b acknowledged=1, got %d", agentBAcked)
	}

	agentCAfterBAck, err := store.PollMessages(ctx, workspaceID, "agent-c", "", 10, 24)
	if err != nil {
		t.Fatalf("poll agent-c after agent-b ack: %v", err)
	}
	if len(agentCAfterBAck) != 0 {
		t.Fatalf("expected agent-c to see no legacy-hidden broadcast, got %+v", agentCAfterBAck)
	}

	agentCAcked, err := store.AckMessages(ctx, workspaceID, "agent-c", []string{broadcastID, directID, broadcastID, "missing-id"})
	if err != nil {
		t.Fatalf("ack agent-c legacy-hidden mixed set: %v", err)
	}
	if agentCAcked != 0 {
		t.Fatalf("expected agent-c acknowledged=0, got %d", agentCAcked)
	}
}
