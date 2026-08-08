package model

const (
	TaskStatusPending   = "PENDING"
	TaskStatusRunning   = "RUNNING"
	TaskStatusResolved  = "RESOLVED"
	TaskStatusFailed    = "FAILED"
	TaskStatusCancelled = "CANCELLED"
)

const (
	NodeStatusPending       = "PENDING"
	NodeStatusBlocked       = "BLOCKED"
	NodeStatusAwaitingFunds = "AWAITING_FUNDS"
	NodeStatusRunning       = "RUNNING"
	NodeStatusResolved      = "RESOLVED"
	NodeStatusFailed        = "FAILED"
	NodeStatusCancelled     = "CANCELLED"
)

const (
	ApprovalStatusCreated         = "CREATED"
	ApprovalStatusPendingOperator = "PENDING_OPERATOR"
	ApprovalStatusApproved        = "APPROVED"
	ApprovalStatusRejected        = "REJECTED"
	ApprovalStatusExpired         = "EXPIRED"
)

var validNodeStatuses = map[string]struct{}{
	NodeStatusPending:       {},
	NodeStatusBlocked:       {},
	NodeStatusAwaitingFunds: {},
	NodeStatusRunning:       {},
	NodeStatusResolved:      {},
	NodeStatusFailed:        {},
	NodeStatusCancelled:     {},
}

var validApprovalStatuses = map[string]struct{}{
	ApprovalStatusCreated:         {},
	ApprovalStatusPendingOperator: {},
	ApprovalStatusApproved:        {},
	ApprovalStatusRejected:        {},
	ApprovalStatusExpired:         {},
}

func ValidNodeStatus(status string) bool {
	_, ok := validNodeStatuses[status]
	return ok
}

func ValidApprovalStatus(status string) bool {
	_, ok := validApprovalStatuses[status]
	return ok
}
