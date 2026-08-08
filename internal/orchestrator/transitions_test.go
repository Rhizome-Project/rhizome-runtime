package orchestrator

import (
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestValidateNodeTransition(t *testing.T) {
	t.Parallel()

	valid := []struct {
		from string
		to   string
	}{
		{model.NodeStatusPending, model.NodeStatusAwaitingFunds},
		{model.NodeStatusPending, model.NodeStatusRunning},
		{model.NodeStatusBlocked, model.NodeStatusAwaitingFunds},
		{model.NodeStatusBlocked, model.NodeStatusPending},
		{model.NodeStatusAwaitingFunds, model.NodeStatusRunning},
		{model.NodeStatusRunning, model.NodeStatusResolved},
		{model.NodeStatusFailed, model.NodeStatusPending},
		{model.NodeStatusResolved, model.NodeStatusResolved},
	}

	for _, tc := range valid {
		if err := ValidateNodeTransition(tc.from, tc.to); err != nil {
			t.Fatalf("expected valid transition %s -> %s, got %v", tc.from, tc.to, err)
		}
	}

	invalid := []struct {
		from string
		to   string
	}{
		{model.NodeStatusResolved, model.NodeStatusRunning},
		{model.NodeStatusCancelled, model.NodeStatusPending},
		{model.NodeStatusPending, model.NodeStatusResolved},
		{"UNKNOWN", model.NodeStatusPending},
	}

	for _, tc := range invalid {
		if err := ValidateNodeTransition(tc.from, tc.to); !errors.Is(err, ErrInvalidNodeStatusTransition) {
			t.Fatalf("expected invalid transition error for %s -> %s, got %v", tc.from, tc.to, err)
		}
	}
}

func TestValidateApprovalTransition(t *testing.T) {
	t.Parallel()

	valid := []struct {
		from string
		to   string
	}{
		{model.ApprovalStatusCreated, model.ApprovalStatusPendingOperator},
		{model.ApprovalStatusPendingOperator, model.ApprovalStatusApproved},
		{model.ApprovalStatusPendingOperator, model.ApprovalStatusRejected},
		{model.ApprovalStatusPendingOperator, model.ApprovalStatusExpired},
		{model.ApprovalStatusApproved, model.ApprovalStatusApproved},
	}

	for _, tc := range valid {
		if err := ValidateApprovalTransition(tc.from, tc.to); err != nil {
			t.Fatalf("expected valid transition %s -> %s, got %v", tc.from, tc.to, err)
		}
	}

	invalid := []struct {
		from string
		to   string
	}{
		{model.ApprovalStatusExpired, model.ApprovalStatusApproved},
		{model.ApprovalStatusRejected, model.ApprovalStatusPendingOperator},
		{model.ApprovalStatusCreated, model.ApprovalStatusApproved},
		{"UNKNOWN", model.ApprovalStatusApproved},
	}

	for _, tc := range invalid {
		if err := ValidateApprovalTransition(tc.from, tc.to); !errors.Is(err, ErrInvalidApprovalStatusTransition) {
			t.Fatalf("expected invalid transition error for %s -> %s, got %v", tc.from, tc.to, err)
		}
	}
}
