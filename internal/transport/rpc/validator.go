package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidJSON       = errors.New("invalid json")
	ErrInvalidJSONRPC    = errors.New("invalid jsonrpc version")
	ErrInvalidMethod     = errors.New("invalid method")
	ErrMissingID         = errors.New("missing id")
	ErrInvalidParams     = errors.New("invalid params")
	ErrMissingField      = errors.New("missing required field")
	ErrInvalidFieldValue = errors.New("invalid field value")
)

var requiredFinopsFields = []string{
	"request_id",
	"task_id",
	"node_id",
	"owner_user_id",
	"resource_type",
	"service_id",
	"estimated_cost_usd",
	"justification",
	"idempotency_key",
}

type rpcEnvelope struct {
	JSONRPC string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
	ID      any            `json:"id"`
}

func ValidateFinopsRequestPayload(raw []byte) error {
	var req rpcEnvelope
	if err := json.Unmarshal(raw, &req); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}

	if req.JSONRPC != "2.0" {
		return ErrInvalidJSONRPC
	}
	if req.Method != "finops.request_resource" {
		return ErrInvalidMethod
	}
	if req.ID == nil {
		return ErrMissingID
	}
	if req.Params == nil {
		return ErrInvalidParams
	}

	for _, field := range requiredFinopsFields {
		v, ok := req.Params[field]
		if !ok {
			return fmt.Errorf("%w: %s", ErrMissingField, field)
		}
		if v == nil {
			return fmt.Errorf("%w: %s is nil", ErrInvalidFieldValue, field)
		}

		// Required string fields must be non-empty.
		switch field {
		case "request_id", "task_id", "node_id", "owner_user_id", "resource_type", "service_id", "justification", "idempotency_key":
			s, ok := v.(string)
			if !ok || strings.TrimSpace(s) == "" {
				return fmt.Errorf("%w: %s", ErrInvalidFieldValue, field)
			}
		case "estimated_cost_usd":
			switch v.(type) {
			case float64, float32, int, int32, int64:
				// valid
			default:
				return fmt.Errorf("%w: estimated_cost_usd", ErrInvalidFieldValue)
			}
		}
	}

	return nil
}
