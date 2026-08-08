package orchestrator

import (
	"errors"
	"fmt"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

var (
	ErrInvalidNodeStatusTransition     = errors.New("invalid node status transition")
	ErrInvalidApprovalStatusTransition = errors.New("invalid approval status transition")
)

var nodeTransitions = map[string]map[string]struct{}{
	model.NodeStatusPending: {
		model.NodeStatusAwaitingFunds: {},
		model.NodeStatusRunning:       {},
		model.NodeStatusFailed:        {},
		model.NodeStatusCancelled:     {},
	},
	model.NodeStatusBlocked: {
		model.NodeStatusAwaitingFunds: {},
		model.NodeStatusPending:       {},
		model.NodeStatusFailed:        {},
		model.NodeStatusCancelled:     {},
	},
	model.NodeStatusAwaitingFunds: {
		model.NodeStatusRunning:   {},
		model.NodeStatusFailed:    {},
		model.NodeStatusCancelled: {},
	},
	model.NodeStatusRunning: {
		model.NodeStatusPending:   {},
		model.NodeStatusBlocked:   {},
		model.NodeStatusResolved:  {},
		model.NodeStatusFailed:    {},
		model.NodeStatusCancelled: {},
	},
	model.NodeStatusResolved: {},
	model.NodeStatusFailed: {
		model.NodeStatusPending:   {},
		model.NodeStatusCancelled: {},
	},
	model.NodeStatusCancelled: {},
}

var approvalTransitions = map[string]map[string]struct{}{
	model.ApprovalStatusCreated: {
		model.ApprovalStatusPendingOperator: {},
		model.ApprovalStatusExpired:         {},
	},
	model.ApprovalStatusPendingOperator: {
		model.ApprovalStatusApproved: {},
		model.ApprovalStatusRejected: {},
		model.ApprovalStatusExpired:  {},
	},
	model.ApprovalStatusApproved: {},
	model.ApprovalStatusRejected: {},
	model.ApprovalStatusExpired:  {},
}

func ValidateNodeTransition(from, to string) error {
	if !model.ValidNodeStatus(from) || !model.ValidNodeStatus(to) {
		return fmt.Errorf("%w: unknown node status %q -> %q", ErrInvalidNodeStatusTransition, from, to)
	}

	if from == to {
		return nil
	}

	if _, ok := nodeTransitions[from][to]; !ok {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidNodeStatusTransition, from, to)
	}

	return nil
}

func ValidateApprovalTransition(from, to string) error {
	if !model.ValidApprovalStatus(from) || !model.ValidApprovalStatus(to) {
		return fmt.Errorf("%w: unknown approval status %q -> %q", ErrInvalidApprovalStatusTransition, from, to)
	}

	if from == to {
		return nil
	}

	if _, ok := approvalTransitions[from][to]; !ok {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidApprovalStatusTransition, from, to)
	}

	return nil
}
