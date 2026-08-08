package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (r *Runtime) syncMessageCursor(ctx context.Context, cursor string) error {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return nil
	}

	now := time.Now().UTC()
	if inbox := r.messageInbox(); inbox != nil {
		if err := inbox.SetLastSyncedCursor(cursor, now); err != nil {
			return err
		}
	}
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.MessageCursor = cursor
	})
}

func (r *Runtime) clearMessageCursor(ctx context.Context) error {
	now := time.Now().UTC()
	if inbox := r.messageInbox(); inbox != nil {
		if err := inbox.ClearLastSyncedCursor(now); err != nil {
			return err
		}
	}
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.MessageCursor = ""
	})
}

func messagePollInvalidCursorError(err error) bool {
	if err == nil {
		return false
	}
	if isRhizomeRPCErrorCode(err, rhizomeRPCCodeInvalidPollCursor) {
		return true
	}
	return strings.Contains(err.Error(), "after_created_at must be a valid poll cursor")
}

func (r *Runtime) reconcileLocalInbox(ctx context.Context) error {
	if _, err := r.replayLocalInbox(ctx); err != nil {
		return err
	}
	return r.retryPendingInboxAcks(ctx)
}

func (r *Runtime) replayLocalInbox(ctx context.Context) (messageBatchOutcome, error) {
	inbox := r.messageInbox()
	if inbox == nil {
		return messageBatchOutcome{}, nil
	}
	pending, err := inbox.PendingMessages()
	if err != nil {
		return messageBatchOutcome{}, err
	}
	if len(pending) == 0 {
		return messageBatchOutcome{}, nil
	}
	return r.processMessageList(ctx, pending, false, inbox.LastSyncedCursor())
}

func (r *Runtime) retryPendingInboxAcks(ctx context.Context) error {
	inbox := r.messageInbox()
	if inbox == nil {
		return nil
	}
	unacked, err := inbox.UnackedMessages()
	if err != nil {
		return err
	}
	if len(unacked) == 0 {
		return nil
	}

	messageIDs := make([]string, 0, len(unacked))
	for _, message := range unacked {
		messageIDs = append(messageIDs, message.MessageID)
	}
	messageIDs = uniqueStrings(messageIDs)
	if len(messageIDs) == 0 {
		return nil
	}

	now := time.Now().UTC()
	if err := inbox.MarkAckAttempt(messageIDs, now); err != nil {
		return err
	}
	if err := r.client.AckMessages(ctx, MessageAckInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		MessageIDs:  messageIDs,
	}); err != nil {
		if markErr := inbox.MarkAckFailure(messageIDs, now, err); markErr != nil {
			return fmt.Errorf("%w; mark inbox ack failure: %v", err, markErr)
		}
		return err
	}
	return inbox.MarkAcked(messageIDs, now)
}

func (r *Runtime) processMessageList(ctx context.Context, messages []MessageRecord, recordIncoming bool, cursorHint string) (messageBatchOutcome, error) {
	outcome := messageBatchOutcome{}
	if len(messages) == 0 {
		return outcome, nil
	}

	now := time.Now().UTC()
	inbox := r.messageInbox()
	if recordIncoming && inbox != nil {
		if err := inbox.RecordBatch(messages, now, cursorHint); err != nil {
			return outcome, err
		}
	}

	items := append([]MessageRecord(nil), messages...)
	sortMessagesForDelivery(items)

	ackIDs := make([]string, 0, len(items))
	handledCount := 0
	lastCursor := strings.TrimSpace(cursorHint)
	var failureErr error

	for _, message := range items {
		messageID := strings.TrimSpace(message.MessageID)
		if messageID == "" {
			continue
		}
		if inbox != nil {
			handled, acked, exists := inbox.MessageStatus(messageID)
			if exists {
				if acked {
					if cursor := messageCursorForRecord(message); cursor != "" {
						lastCursor = cursor
					}
					continue
				}
				if handled {
					ackIDs = append(ackIDs, messageID)
					if cursor := messageCursorForRecord(message); cursor != "" {
						lastCursor = cursor
					}
					continue
				}
				if err := inbox.MarkDeliveryAttempt(messageID, now); err != nil {
					return outcome, err
				}
			}
		}

		if err := r.handleInboundMessage(ctx, message); err != nil {
			failureErr = fmt.Errorf("message %s: %w", messageID, err)
			outcome.hadError = true
			if inbox != nil {
				if markErr := inbox.MarkDeliveryFailure(messageID, time.Now().UTC(), err); markErr != nil {
					return outcome, markErr
				}
			}
			if cursor := messageCursorForRecord(message); cursor != "" {
				lastCursor = cursor
			}
			break
		}

		handledCount++
		if inbox != nil {
			if err := inbox.MarkHandled(messageID, time.Now().UTC()); err != nil {
				return outcome, err
			}
		}
		ackIDs = append(ackIDs, messageID)
		if cursor := messageCursorForRecord(message); cursor != "" {
			lastCursor = cursor
		}
	}

	outcome.handled = handledCount
	outcome.nextCursor = firstNonEmpty(lastCursor, cursorHint)

	if len(ackIDs) == 0 {
		return outcome, failureErr
	}

	ackIDs = uniqueStrings(ackIDs)
	if inbox != nil {
		if err := inbox.MarkAckAttempt(ackIDs, time.Now().UTC()); err != nil {
			return outcome, err
		}
	}
	if err := r.client.AckMessages(ctx, MessageAckInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		MessageIDs:  ackIDs,
	}); err != nil {
		if inbox != nil {
			if markErr := inbox.MarkAckFailure(ackIDs, time.Now().UTC(), err); markErr != nil {
				return outcome, fmt.Errorf("%w; mark inbox ack failure: %v", err, markErr)
			}
		}
		return outcome, err
	}
	if inbox != nil {
		if err := inbox.MarkAcked(ackIDs, time.Now().UTC()); err != nil {
			return outcome, err
		}
	}

	return outcome, failureErr
}
