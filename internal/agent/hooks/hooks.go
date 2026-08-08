package hooks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// Point represents a hook lifecycle point.
type Point string

const (
	BeforeTool    Point = "before_tool"
	AfterTool     Point = "after_tool"
	OnStop        Point = "on_stop"
	BeforeCompact Point = "before_compact"
	SessionEnd    Point = "session_end"
)

// Context carries data relevant to each hook point.
type Context struct {
	Point        Point
	AgentID      string
	SessionID    string
	ToolName     string
	ToolInput    json.RawMessage
	ToolOutput   string
	ToolError    error
	StopReason   string
	MessageCount int
	TotalTokens  int
	Error        error
}

// Result is returned by hooks to modify agent behavior.
type Result struct {
	ModifiedInput       json.RawMessage
	InjectSystemMessage string
	PreventStop         bool
}

// Hook defines the interface for agent lifecycle hooks.
type Hook interface {
	Name() string
	Points() []Point
	Run(ctx context.Context, hctx Context) (Result, error)
}

// Runner holds registered hooks and executes them at hook points.
type Runner struct {
	byPoint map[Point][]Hook
}

// NewRunner creates a new hook runner.
func NewRunner() *Runner {
	return &Runner{
		byPoint: make(map[Point][]Hook),
	}
}

// Register adds a hook for all points it declares.
func (r *Runner) Register(hook Hook) {
	for _, p := range hook.Points() {
		r.byPoint[p] = append(r.byPoint[p], hook)
	}
}

// Run executes all hooks registered for the given point, merging results.
func (r *Runner) Run(ctx context.Context, point Point, hctx Context) (Result, error) {
	hctx.Point = point
	hooks := r.byPoint[point]
	if len(hooks) == 0 {
		return Result{}, nil
	}

	var merged Result
	var injectParts []string

	for _, h := range hooks {
		res, err := r.safeRun(ctx, h, hctx)
		if err != nil {
			log.Printf("[agent-hook] hook %q at %s returned error: %v", h.Name(), point, err)
			continue
		}
		if res.ModifiedInput != nil {
			merged.ModifiedInput = res.ModifiedInput
		}
		if res.InjectSystemMessage != "" {
			injectParts = append(injectParts, res.InjectSystemMessage)
		}
		if res.PreventStop {
			merged.PreventStop = true
		}
	}

	if len(injectParts) > 0 {
		merged.InjectSystemMessage = strings.Join(injectParts, "\n")
	}

	return merged, nil
}

func (r *Runner) safeRun(ctx context.Context, h Hook, hctx Context) (result Result, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[agent-hook] hook %q panicked: %v", h.Name(), rec)
			err = fmt.Errorf("hook %q panicked: %v", h.Name(), rec)
		}
	}()
	return h.Run(ctx, hctx)
}
