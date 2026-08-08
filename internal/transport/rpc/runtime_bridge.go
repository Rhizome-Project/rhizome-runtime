package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidRuntimeEnvelope = errors.New("invalid runtime rpc envelope")
	ErrInvalidRuntimeResult   = errors.New("invalid runtime rpc result")
	ErrInvalidRuntimeError    = errors.New("invalid runtime rpc error")
	ErrMismatchedID           = errors.New("mismatched id")
)

type ExecutorArtifactRef struct {
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	ContentType string `json:"content_type"`
}

type ExecutorRunNodeResult struct {
	Status      string                `json:"status"`
	ExitCode    int                   `json:"exit_code"`
	DurationSec float64               `json:"duration_sec"`
	Stdout      string                `json:"stdout"`
	Stderr      string                `json:"stderr"`
	Artifacts   []ExecutorArtifactRef `json:"artifacts"`
	Metrics     map[string]any        `json:"metrics"`
	TraceID     string                `json:"trace_id"`
}

type ExecutorRunNodeError struct {
	Code    int                        `json:"code"`
	Message string                     `json:"message"`
	Details ExecutorRunNodeErrorDetail `json:"details"`
}

type ExecutorRunNodeErrorDetail struct {
	Status       string  `json:"status"`
	ExitCode     int     `json:"exit_code"`
	DurationSec  float64 `json:"duration_sec"`
	ErrorMessage string  `json:"error_message"`
	Stderr       string  `json:"stderr"`
	TraceID      string  `json:"trace_id"`
}

type ExecutorRunNodeResponse struct {
	JSONRPC string                 `json:"jsonrpc"`
	ID      any                    `json:"id"`
	Result  *ExecutorRunNodeResult `json:"result,omitempty"`
	Error   *ExecutorRunNodeError  `json:"error,omitempty"`
}

func ParseExecutorRunNodeResponse(raw []byte, expectedID string) (ExecutorRunNodeResponse, error) {
	var resp ExecutorRunNodeResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return ExecutorRunNodeResponse{}, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}

	if resp.JSONRPC != "2.0" {
		return ExecutorRunNodeResponse{}, ErrInvalidRuntimeEnvelope
	}
	if resp.ID == nil {
		return ExecutorRunNodeResponse{}, ErrMissingID
	}
	if gotID, ok := resp.ID.(string); !ok || gotID != expectedID {
		return ExecutorRunNodeResponse{}, fmt.Errorf("%w: expected %q, got %s", ErrMismatchedID, expectedID, formatRuntimeResponseID(resp.ID))
	}

	hasResult := resp.Result != nil
	hasError := resp.Error != nil
	if hasResult == hasError {
		return ExecutorRunNodeResponse{}, ErrInvalidRuntimeEnvelope
	}

	if hasResult {
		if err := validateRunNodeResult(*resp.Result); err != nil {
			return ExecutorRunNodeResponse{}, err
		}
		return resp, nil
	}

	if err := validateRunNodeError(*resp.Error); err != nil {
		return ExecutorRunNodeResponse{}, err
	}
	return resp, nil
}

func formatRuntimeResponseID(id any) string {
	raw, err := json.Marshal(id)
	if err != nil {
		return fmt.Sprintf("%v", id)
	}
	return string(raw)
}

func validateRunNodeResult(result ExecutorRunNodeResult) error {
	if strings.TrimSpace(result.Status) == "" {
		return fmt.Errorf("%w: missing status", ErrInvalidRuntimeResult)
	}
	if strings.TrimSpace(result.Status) != "SUCCESS" {
		return fmt.Errorf("%w: unexpected status %q", ErrInvalidRuntimeResult, result.Status)
	}
	for i, artifact := range result.Artifacts {
		if strings.TrimSpace(artifact.Path) == "" {
			return fmt.Errorf("%w: artifact[%d] missing path", ErrInvalidRuntimeResult, i)
		}
		if strings.TrimSpace(artifact.ContentType) == "" {
			return fmt.Errorf("%w: artifact[%d] missing content_type", ErrInvalidRuntimeResult, i)
		}
	}
	return nil
}

func validateRunNodeError(errPayload ExecutorRunNodeError) error {
	if errPayload.Code == 0 {
		return fmt.Errorf("%w: missing error code", ErrInvalidRuntimeError)
	}
	if strings.TrimSpace(errPayload.Message) == "" {
		return fmt.Errorf("%w: missing error message", ErrInvalidRuntimeError)
	}
	if strings.TrimSpace(errPayload.Details.Status) == "" {
		return fmt.Errorf("%w: missing details.status", ErrInvalidRuntimeError)
	}
	if strings.TrimSpace(errPayload.Details.ErrorMessage) == "" {
		return fmt.Errorf("%w: missing details.error_message", ErrInvalidRuntimeError)
	}
	return nil
}

func MapExecutorRuntimeErrorCode(code int, message string) string {
	switch code {
	case -32017:
		return "executor_timeout"
	case -32018:
		return "executor_runtime_error"
	case -32019:
		return "state_transition_invalid"
	}

	msg := strings.TrimSpace(strings.ToLower(message))
	if msg == "" {
		return "internal_error"
	}
	return msg
}
