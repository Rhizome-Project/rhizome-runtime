package living

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
)

// ── Triage types ─────────────────────────────────────────────────────

// TriageAction represents the action the triager recommends.
type TriageAction string

const (
	TriageRespond    TriageAction = "RESPOND"
	TriageCreateTask TriageAction = "CREATE_TASK"
	TriageForward    TriageAction = "FORWARD"
	TriageRemember   TriageAction = "REMEMBER"
	TriageIgnore     TriageAction = "IGNORE"
)

// TriageResult holds the outcome of triaging a general message.
type TriageResult struct {
	Action  TriageAction
	Reply   string // for RESPOND
	Title   string // for CREATE_TASK
	Desc    string // for CREATE_TASK
	Target  string // for FORWARD
	Content string // for REMEMBER
}

// ── Interfaces ───────────────────────────────────────────────────────

// TaskLookup finds active tasks by ID.
type TaskLookup interface {
	ActiveTaskByID(taskID string) *TaskState
}

// MessageTriager abstracts the LLM triage call for testability.
type MessageTriager interface {
	Triage(ctx context.Context, msg Message, agentRole string) (*TriageResult, error)
}

// ── MessageHandler ───────────────────────────────────────────────────

var taskIDPattern = regexp.MustCompile(`^\[task:([^\]]+)\]`)

// MessageHandler processes incoming messages from Rhizome. It categorises
// them as task-bound (content starts with [task:TASKID]) or general and
// handles them accordingly.
type MessageHandler struct {
	config          Config
	rhizome         RhizomeClient
	taskLookup      TaskLookup
	triager         MessageTriager
	lastMessageTime time.Time
}

// NewMessageHandler creates a MessageHandler with the given dependencies.
func NewMessageHandler(config Config, rhizome RhizomeClient, taskLookup TaskLookup, triager MessageTriager) *MessageHandler {
	return &MessageHandler{
		config:     config,
		rhizome:    rhizome,
		taskLookup: taskLookup,
		triager:    triager,
	}
}

// HandleMessages processes a batch of incoming messages. Task-bound
// messages are injected into the corresponding TaskState; general
// messages are triaged via the MessageTriager.
func (h *MessageHandler) HandleMessages(ctx context.Context, messages []Message) error {
	for _, msg := range messages {
		taskID := extractTaskID(msg.Content)

		if taskID != "" {
			h.handleTaskBound(ctx, msg, taskID)
		} else {
			h.handleGeneral(ctx, msg)
		}

		h.updateLastMessageTime(msg.CreatedAt)
	}
	return nil
}

// handleTaskBound injects the message into the active task's conversation
// and resumes the task if it was waiting for this message.
func (h *MessageHandler) handleTaskBound(ctx context.Context, msg Message, taskID string) {
	task := h.taskLookup.ActiveTaskByID(taskID)
	if task == nil {
		log.Printf("[message_handler] message %s references unknown task %s, ignoring", msg.MessageID, taskID)
		return
	}

	injected := llm.NewUserMessage(fmt.Sprintf("[MESSAGE from %s]: %s", msg.FromAgentID, msg.Content))
	task.Messages = append(task.Messages, injected)

	if task.Status == TaskStatusWaiting && (task.WaitingForMessage == "" || task.WaitingForMessage == msg.FromAgentID) {
		if err := task.Resume(); err != nil {
			log.Printf("[message_handler] failed to resume task %s: %v", taskID, err)
			return
		}
		recordSessionEventIfAvailable(ctx, h.rhizome, resumedSessionKeepalive(task, h.config.ID, taskID, msg.FromAgentID))
	}
}

// handleGeneral triages a general message and acts on the result.
func (h *MessageHandler) handleGeneral(ctx context.Context, msg Message) {
	result, err := h.triager.Triage(ctx, msg, h.config.ID)
	if err != nil {
		log.Printf("[message_handler] triage failed for message %s: %v", msg.MessageID, err)
		return
	}

	switch result.Action {
	case TriageRespond:
		if err := h.rhizome.SendMessage(ctx, h.config.ID, msg.FromAgentID, result.Reply, ""); err != nil {
			log.Printf("[message_handler] failed to send reply to %s: %v", msg.FromAgentID, err)
		}
	case TriageCreateTask:
		log.Printf("[message_handler] CREATE_TASK requested: title=%q desc=%q (not yet implemented)", result.Title, result.Desc)
	case TriageForward:
		if err := h.rhizome.SendMessage(ctx, h.config.ID, result.Target, msg.Content, ""); err != nil {
			log.Printf("[message_handler] failed to forward message to %s: %v", result.Target, err)
		}
	case TriageRemember:
		log.Printf("[message_handler] REMEMBER requested: %q (memory integration pending)", result.Content)
	case TriageIgnore:
		// do nothing
	}
}

// updateLastMessageTime updates lastMessageTime if the given timestamp
// string parses to a time after the current value.
func (h *MessageHandler) updateLastMessageTime(createdAt string) {
	t, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return
	}
	if t.After(h.lastMessageTime) {
		h.lastMessageTime = t
	}
}

// LastMessageTime returns the timestamp of the most recently processed message.
func (h *MessageHandler) LastMessageTime() time.Time {
	return h.lastMessageTime
}

// extractTaskID extracts a task ID from content that starts with [task:TASKID].
// Returns "" if the pattern is not found.
func extractTaskID(content string) string {
	matches := taskIDPattern.FindStringSubmatch(content)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}
