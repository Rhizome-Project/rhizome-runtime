package living

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/llm"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// SituationLLM abstracts the LLM call for situation assessment.
type SituationLLM interface {
	Assess(ctx context.Context, situationReport string) (string, error)
}

// TaskAssessment represents the outcome of evaluating a single task's health.
type TaskAssessment string

const (
	AssessmentHealthy  TaskAssessment = "HEALTHY"
	AssessmentStuck    TaskAssessment = "STUCK"
	AssessmentBlocked  TaskAssessment = "BLOCKED"
	AssessmentEscalate TaskAssessment = "ESCALATE"
)

// AssessmentResult is the per-task result returned by the LLM.
type AssessmentResult struct {
	TaskID     string         `json:"task_id"`
	Assessment TaskAssessment `json:"assessment"`
	Action     string         `json:"action"`
	Details    string         `json:"details"`
}

// SituationAssessor periodically evaluates all active tasks for health,
// using an LLM to determine whether tasks are stuck, blocked, or need
// escalation.
type SituationAssessor struct {
	config     Config
	rhizome    RhizomeClient
	taskLookup TaskLookup
	tasks      func() []*TaskState
	llm        SituationLLM
}

// NewSituationAssessor creates a SituationAssessor wired with the given
// dependencies.
func NewSituationAssessor(config Config, rhizome RhizomeClient, taskLookup TaskLookup, activeTasks func() []*TaskState, llm SituationLLM) *SituationAssessor {
	return &SituationAssessor{
		config:     config,
		rhizome:    rhizome,
		taskLookup: taskLookup,
		tasks:      activeTasks,
		llm:        llm,
	}
}

// Assess builds a situation report for all active tasks, asks the LLM to
// evaluate each one, and takes corrective actions based on the results.
func (sa *SituationAssessor) Assess(ctx context.Context) error {
	active := sa.tasks()
	if len(active) == 0 {
		return nil
	}

	// Build situation report
	report := sa.buildSituationReport(active)

	// Call LLM
	response, err := sa.llm.Assess(ctx, report)
	if err != nil {
		log.Printf("[situation] LLM assessment failed: %v", err)
		return nil // non-fatal
	}

	// Parse response
	var results []AssessmentResult
	if err := json.Unmarshal([]byte(response), &results); err != nil {
		log.Printf("[situation] failed to parse assessment response: %v", err)
		return nil // non-fatal
	}

	// Execute actions
	for _, r := range results {
		task := sa.taskLookup.ActiveTaskByID(r.TaskID)
		if task == nil {
			log.Printf("[situation] assessment for unknown task %s, skipping", r.TaskID)
			continue
		}

		switch r.Assessment {
		case AssessmentHealthy:
			// no action
		case AssessmentStuck:
			// Inject advice
			task.Messages = append(task.Messages, llm.NewUserMessage(
				fmt.Sprintf("[SITUATION ASSESSMENT]: %s", r.Details),
			))
		case AssessmentBlocked:
			// Send update about blocked task
			sa.rhizome.SendUpdate(ctx, sa.config.ID, "", "blocked",
				fmt.Sprintf("Task %s is blocked: %s", r.TaskID, r.Details), "")
			keepFalse := false
			recordSessionEventIfAvailable(ctx, sa.rhizome, SessionEventInput{
				EventType:         model.SessionEventBlocked,
				SessionID:         strings.TrimSpace(task.SessionID),
				AgentID:           sa.config.ID,
				TaskID:            r.TaskID,
				Summary:           clipTaskSessionSummary(r.Details, 240),
				BlockedOn:         taskBlockedRef("situation_assessment", r.Details),
				KeepSessionActive: &keepFalse,
			})
		case AssessmentEscalate:
			// Escalate via Rhizome
			sa.rhizome.EscalateTask(ctx, r.TaskID, r.Details)
			keepFalse := false
			recordSessionEventIfAvailable(ctx, sa.rhizome, SessionEventInput{
				EventType:          model.SessionEventDecisionNeeded,
				SessionID:          strings.TrimSpace(task.SessionID),
				AgentID:            sa.config.ID,
				TaskID:             r.TaskID,
				Summary:            clipTaskSessionSummary(r.Details, 240),
				DecisionNeededFrom: "human",
				DecisionType:       "situation_assessment",
				KeepSessionActive:  &keepFalse,
			})
		}
	}

	return nil
}

// buildSituationReport formats all active tasks into a markdown report
// suitable for LLM consumption.
func (sa *SituationAssessor) buildSituationReport(tasks []*TaskState) string {
	var b strings.Builder
	b.WriteString("# Active Tasks Situation Report\n\n")
	for _, t := range tasks {
		fmt.Fprintf(&b, "## Task: %s\n", t.TaskID)
		fmt.Fprintf(&b, "- Status: %s\n", t.Status)
		fmt.Fprintf(&b, "- Iterations: %d\n", t.IterationCount)
		fmt.Fprintf(&b, "- Progress: %s\n", t.ProgressSummary)
		if t.Error != "" {
			fmt.Fprintf(&b, "- Last Error: %s\n", t.Error)
		}
		fmt.Fprintf(&b, "- Messages: %d\n", len(t.Messages))
		fmt.Fprintf(&b, "- Updated: %s ago\n\n", time.Since(t.UpdatedAt).Truncate(time.Second))
	}
	return b.String()
}
