package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
)

// Tool is the interface every tool implements.
type Tool interface {
	Name() string
	Description() string
	Parameters() map[string]any
	Execute(ctx context.Context, args map[string]any) *ToolResult
}

// ToolResult is what a tool execution returns.
type ToolResult struct {
	Output  string
	IsError bool
}

// ToolRegistry holds all registered tools.
type ToolRegistry struct {
	tools       map[string]Tool
	diagnostics []ToolRegistrationDiagnostic
}

func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]Tool)}
}

// ToolRegistrationDiagnostic records a quarantined tool registration.
type ToolRegistrationDiagnostic struct {
	Code          string
	ToolName      string
	ExistingType  string
	DuplicateType string
	Message       string
}

func (r *ToolRegistry) Register(t Tool) {
	for _, diagnostic := range r.RegisterWithDiagnostics(t) {
		log.Printf("[tools] %s", diagnostic.Message)
	}
}

func (r *ToolRegistry) RegisterWithDiagnostics(t Tool) []ToolRegistrationDiagnostic {
	if r == nil {
		return []ToolRegistrationDiagnostic{invalidToolRegistrationDiagnostic(t, "nil_tool_registry")}
	}
	if r.tools == nil {
		r.tools = make(map[string]Tool)
	}
	if toolIsNil(t) {
		diagnostic := invalidToolRegistrationDiagnostic(t, "nil_tool_registration")
		r.diagnostics = append(r.diagnostics, diagnostic)
		return []ToolRegistrationDiagnostic{diagnostic}
	}
	name := t.Name()
	if strings.TrimSpace(name) == "" {
		diagnostic := invalidToolRegistrationDiagnostic(t, "empty_tool_name")
		r.diagnostics = append(r.diagnostics, diagnostic)
		return []ToolRegistrationDiagnostic{diagnostic}
	}
	if existing, ok := r.tools[name]; ok {
		diagnostic := ToolRegistrationDiagnostic{
			Code:          "duplicate_tool_registration",
			ToolName:      name,
			ExistingType:  fmt.Sprintf("%T", existing),
			DuplicateType: fmt.Sprintf("%T", t),
		}
		diagnostic.Message = fmt.Sprintf("duplicate tool registration quarantined: %q already registered by %s; rejected %s", name, diagnostic.ExistingType, diagnostic.DuplicateType)
		r.diagnostics = append(r.diagnostics, diagnostic)
		return []ToolRegistrationDiagnostic{diagnostic}
	}
	r.tools[name] = t
	return nil
}

func toolIsNil(t Tool) bool {
	if t == nil {
		return true
	}
	value := reflect.ValueOf(t)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func invalidToolRegistrationDiagnostic(t Tool, code string) ToolRegistrationDiagnostic {
	diagnostic := ToolRegistrationDiagnostic{
		Code:          code,
		DuplicateType: fmt.Sprintf("%T", t),
	}
	switch code {
	case "nil_tool_registry":
		diagnostic.Message = fmt.Sprintf("tool registration quarantined: nil registry rejected %s", diagnostic.DuplicateType)
	case "empty_tool_name":
		diagnostic.Message = fmt.Sprintf("tool registration quarantined: empty tool name rejected %s", diagnostic.DuplicateType)
	default:
		diagnostic.Message = fmt.Sprintf("tool registration quarantined: nil tool rejected %s", diagnostic.DuplicateType)
	}
	return diagnostic
}

func (r *ToolRegistry) Get(name string) (Tool, bool) {
	if r == nil {
		return nil, false
	}
	t, ok := r.tools[name]
	return t, ok
}

func (r *ToolRegistry) Diagnostics() []ToolRegistrationDiagnostic {
	if r == nil {
		return nil
	}
	return append([]ToolRegistrationDiagnostic(nil), r.diagnostics...)
}

func (r *ToolRegistry) Filter(allow func(string) bool) *ToolRegistry {
	filtered := NewToolRegistry()
	if r == nil {
		return filtered
	}
	for name, tool := range r.tools {
		if allow == nil || allow(name) {
			filtered.Register(tool)
		}
	}
	return filtered
}

// Definitions returns OpenAI-format tool definitions.
func (r *ToolRegistry) Definitions() []ToolDef {
	if r == nil {
		return nil
	}
	defs := make([]ToolDef, 0, len(r.tools))
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := r.tools[name]
		defs = append(defs, ToolDef{
			Type: "function",
			Function: ToolFunctionDef{
				Name:        t.Name(),
				Description: t.Description(),
				Parameters:  t.Parameters(),
			},
		})
	}
	return defs
}

const maxIterations = defaultToolLoopIterations

type ToolLoopExecutor func(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult

type ToolLoopObserver interface {
	OnLLMResponse(iteration int, response *LLMResponse)
	OnToolResult(iteration int, call ToolCall, result ToolResult)
}

type ToolLoopRun struct {
	Content     string
	Messages    []Message
	ToolResults []ToolLoopToolResult
}

// ToolLoopToolResult is the runtime-side trace of one executed tool call.
// It is intentionally not injected back into the LLM transcript; heartbeat and
// watchdog code use it to validate machine-readable contracts without trusting
// model prose.
type ToolLoopToolResult struct {
	Iteration       int
	ToolCallID      string
	ToolName        string
	IsError         bool
	Status          string
	ContractVersion string
	ArtifactRoot    string
	ArtifactPaths   []string
	ArtifactRefs    []string
	ArtifactHash    string
	ArtifactHashes  []string
	ScenarioID      string
	ScenarioIDs     []string
	StateID         string
	StateIDs        []string
	ViewportID      string
	ViewportIDs     []string
	RawOutput       string
}

// RunToolLoop executes the default autonomous LLM to tools loop.
func RunToolLoop(ctx context.Context, llm ChatLLM, registry *ToolRegistry, messages []Message) (string, error) {
	run, err := RunToolLoopDetailed(ctx, llm, registry, messages, nil, nil)
	if err != nil {
		return "", err
	}
	return run.Content, nil
}

// RunToolLoopWithHooks exposes custom tool execution and tracing hooks for the runtime.
func RunToolLoopWithHooks(ctx context.Context, llm ChatLLM, registry *ToolRegistry, messages []Message, executor ToolLoopExecutor, observer ToolLoopObserver) (string, error) {
	run, err := RunToolLoopDetailed(ctx, llm, registry, messages, executor, observer)
	if err != nil {
		return "", err
	}
	return run.Content, nil
}

// RunToolLoopDetailed executes the tool loop and returns the updated message transcript.
func RunToolLoopDetailed(ctx context.Context, llm ChatLLM, registry *ToolRegistry, messages []Message, executor ToolLoopExecutor, observer ToolLoopObserver) (*ToolLoopRun, error) {
	return runToolLoopDetailed(ctx, llm, registry, messages, executor, observer, maxIterations)
}

// RunToolLoopDetailedWithLimit executes the tool loop with a specific ceiling.
func RunToolLoopDetailedWithLimit(ctx context.Context, llm ChatLLM, registry *ToolRegistry, messages []Message, executor ToolLoopExecutor, observer ToolLoopObserver, iterationLimit int) (*ToolLoopRun, error) {
	if iterationLimit <= 0 {
		iterationLimit = maxIterations
	}
	return runToolLoopDetailed(ctx, llm, registry, messages, executor, observer, iterationLimit)
}

func runToolLoopDetailed(ctx context.Context, llm ChatLLM, registry *ToolRegistry, messages []Message, executor ToolLoopExecutor, observer ToolLoopObserver, iterationLimit int) (*ToolLoopRun, error) {
	toolDefs := registry.Definitions()
	if executor == nil {
		executor = defaultToolLoopExecutor
	}
	transcript := append([]Message(nil), messages...)
	toolResults := []ToolLoopToolResult{}
	// TE-10 prompt-view compaction: snapshot the process-wide config once per
	// loop so a mid-run flag flip cannot change view shape between iterations.
	// The canonical transcript is always appended in full; only the per-call
	// prompt view sent to the backend is compacted (default off).
	compaction := CurrentToolLoopCompaction()
	// TE-40 progress guard: snapshot the process-wide config once per loop and
	// build a per-run detector. It is fed each iteration's (call, result) pairs
	// below and, when enabled, stops the loop early with a durable blocked
	// result (transcript + ToolResults preserved) instead of burning to the
	// iteration cap on zero-durable-effect repetition. Default off.
	progressGuard := newToolLoopProgressGuard(CurrentToolLoopProgressGuard())

	for i := 0; i < iterationLimit; i++ {
		promptView := CompactToolLoopPromptView(transcript, compaction)

		// TE-02 prompt telemetry: compute per-iteration prompt section bytes
		// from the exact messages+tools sent this iteration (the compacted
		// prompt view when compaction is on) and surface them to observers
		// that opt in via promptSectionObserver. This is a recomputation from
		// the same inputs the backend uses (no shared recorder), so it is
		// race-free across concurrent agents and covers every backend
		// uniformly. The backend-specific static preamble length is added so
		// codex/qwen rows match the exact built prompt.
		if observer != nil {
			if sectionObserver, ok := observer.(promptSectionObserver); ok {
				sections := computePromptSectionBytes(
					promptView,
					marshalToolsForTelemetry(toolDefs),
					chatLLMStaticPreambleBytes(llm),
					0,
				)
				sectionObserver.OnPromptSectionBytes(i, sections)
			}
		}

		resp, err := llm.Chat(ctx, promptView, toolDefs)
		if err != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}
		if observer != nil {
			observer.OnLLMResponse(i, resp)
		}

		if len(resp.ToolCalls) == 0 {
			transcript = append(transcript, Message{
				Role:    "assistant",
				Content: resp.Content,
			})
			return &ToolLoopRun{
				Content:     resp.Content,
				Messages:    transcript,
				ToolResults: append([]ToolLoopToolResult(nil), toolResults...),
			}, nil
		}

		transcript = append(transcript, Message{
			Role:      "assistant",
			Content:   resp.Content,
			ToolCalls: resp.ToolCalls,
		})

		results := make([]ToolResult, len(resp.ToolCalls))
		for j, tc := range resp.ToolCalls {
			results[j] = executor(ctx, registry, tc)
		}

		for j, tc := range resp.ToolCalls {
			log.Printf("[tool] %s -> %s", tc.Function.Name, truncate(results[j].Output, 200))
			toolResults = append(toolResults, newToolLoopToolResult(i, tc, results[j]))
			if observer != nil {
				observer.OnToolResult(i, tc, results[j])
			}
			transcript = append(transcript, Message{
				Role:       "tool",
				Content:    results[j].Output,
				ToolCallID: tc.ID,
			})
		}

		// TE-40 progress guard: after recording this iteration's tool outcomes,
		// ask the detector whether the last K iterations produced no novel
		// outcome and no durable mutation. If so, stop the loop early with a
		// durable blocked result rather than re-paying the transcript to the
		// iteration cap. The canonical transcript and ToolResults trace are
		// returned intact so the token trace is preserved.
		if progressGuard.observeIteration(resp.ToolCalls, results) {
			stopContent := toolLoopProgressGuardStopContent(progressGuard.config.Window, i+1)
			transcript = append(transcript, Message{
				Role:    "assistant",
				Content: stopContent,
			})
			return &ToolLoopRun{
				Content:     stopContent,
				Messages:    transcript,
				ToolResults: append([]ToolLoopToolResult(nil), toolResults...),
			}, nil
		}
	}

	return nil, fmt.Errorf("tool loop exceeded %d iterations", iterationLimit)
}

func newToolLoopToolResult(iteration int, call ToolCall, result ToolResult) ToolLoopToolResult {
	trace := ToolLoopToolResult{
		Iteration:  iteration,
		ToolCallID: strings.TrimSpace(call.ID),
		ToolName:   strings.TrimSpace(call.Function.Name),
		IsError:    result.IsError,
		RawOutput:  result.Output,
	}
	parseToolLoopToolResultMetadata(&trace)
	return trace
}

func parseToolLoopToolResultMetadata(trace *ToolLoopToolResult) {
	if trace == nil {
		return
	}
	raw := strings.TrimSpace(trace.RawOutput)
	if raw == "" {
		return
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return
	}
	collectToolLoopMetadata(trace, decoded, "")
	trace.ArtifactPaths = uniqueTrimmedCSVStrings(trace.ArtifactPaths)
	trace.ArtifactRefs = uniqueTrimmedCSVStrings(trace.ArtifactRefs)
	trace.ArtifactHashes = uniqueTrimmedCSVStrings(trace.ArtifactHashes)
	trace.ScenarioIDs = uniqueTrimmedCSVStrings(trace.ScenarioIDs)
	trace.StateIDs = uniqueTrimmedCSVStrings(trace.StateIDs)
	trace.ViewportIDs = uniqueTrimmedCSVStrings(trace.ViewportIDs)
	trace.ArtifactHash = firstNonEmpty(trace.ArtifactHash, firstToolLoopString(trace.ArtifactHashes))
	trace.ScenarioID = firstNonEmpty(trace.ScenarioID, firstToolLoopString(trace.ScenarioIDs))
	trace.StateID = firstNonEmpty(trace.StateID, firstToolLoopString(trace.StateIDs))
	trace.ViewportID = firstNonEmpty(trace.ViewportID, firstToolLoopString(trace.ViewportIDs))
}

func collectToolLoopMetadata(trace *ToolLoopToolResult, value any, contextKey string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, raw := range typed {
			lowerKey := strings.ToLower(strings.TrimSpace(key))
			stringValue, hasString := raw.(string)
			if hasString {
				stringValue = strings.TrimSpace(stringValue)
				switch lowerKey {
				case "status":
					if trace.Status == "" {
						trace.Status = stringValue
					}
				case "contract_version":
					if trace.ContractVersion == "" {
						trace.ContractVersion = stringValue
					}
				case "artifact_root", "artifact_dir", "artifacts_dir", "output_dir":
					if trace.ArtifactRoot == "" {
						trace.ArtifactRoot = stringValue
					}
				case "artifact_path", "screenshot_path", "screenshot_file", "file_path", "local_path":
					trace.ArtifactPaths = append(trace.ArtifactPaths, stringValue)
				case "artifact_ref", "screenshot_ref":
					trace.ArtifactRefs = append(trace.ArtifactRefs, stringValue)
				case "artifact_hash", "screenshot_sha256", "sha256":
					trace.ArtifactHashes = append(trace.ArtifactHashes, stringValue)
				case "scenario_id":
					trace.ScenarioIDs = append(trace.ScenarioIDs, stringValue)
				case "state_id":
					trace.StateIDs = append(trace.StateIDs, stringValue)
				case "viewport_id":
					trace.ViewportIDs = append(trace.ViewportIDs, stringValue)
				}
				if toolLoopMetadataContextIsArtifact(contextKey) {
					switch lowerKey {
					case "path":
						trace.ArtifactPaths = append(trace.ArtifactPaths, stringValue)
					case "ref":
						trace.ArtifactRefs = append(trace.ArtifactRefs, stringValue)
					case "hash":
						trace.ArtifactHashes = append(trace.ArtifactHashes, stringValue)
					case "scenario":
						trace.ScenarioIDs = append(trace.ScenarioIDs, stringValue)
					case "state":
						trace.StateIDs = append(trace.StateIDs, stringValue)
					}
				}
				if toolLoopMetadataContextIsViewport(contextKey) {
					switch lowerKey {
					case "id", "viewport":
						trace.ViewportIDs = append(trace.ViewportIDs, stringValue)
					}
				}
			}
			nextContext := lowerKey
			if contextKey != "" {
				nextContext = contextKey + "." + lowerKey
			}
			collectToolLoopMetadata(trace, raw, nextContext)
		}
	case []any:
		for _, item := range typed {
			collectToolLoopMetadata(trace, item, contextKey)
		}
	}
}

func toolLoopMetadataContextIsArtifact(contextKey string) bool {
	contextKey = strings.ToLower(strings.TrimSpace(contextKey))
	return strings.Contains(contextKey, "artifact") || strings.Contains(contextKey, "screenshot")
}

func toolLoopMetadataContextIsViewport(contextKey string) bool {
	contextKey = strings.ToLower(strings.TrimSpace(contextKey))
	return strings.Contains(contextKey, "viewport")
}

func firstToolLoopString(values []string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// RunToolLoopWithLimit executes the default autonomous LLM to tools loop with a specific ceiling.
func RunToolLoopWithLimit(ctx context.Context, llm ChatLLM, registry *ToolRegistry, messages []Message, executor ToolLoopExecutor, observer ToolLoopObserver, iterationLimit int) (string, error) {
	run, err := RunToolLoopDetailedWithLimit(ctx, llm, registry, messages, executor, observer, iterationLimit)
	if err != nil {
		return "", err
	}
	return run.Content, nil
}

func defaultToolLoopExecutor(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
	return executeToolCall(ctx, registry, call)
}

func executeToolCall(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
	tool, ok := registry.Get(call.Function.Name)
	if !ok {
		return ToolResult{Output: fmt.Sprintf("unknown tool: %s", call.Function.Name), IsError: true}
	}

	var args map[string]any
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
		return ToolResult{Output: fmt.Sprintf("invalid arguments: %v", err), IsError: true}
	}
	if invalidRaw, ok := args[codexInvalidArgumentsJSONKey]; ok {
		parseErr := ""
		if value, ok := args[codexInvalidArgumentsErrorKey].(string); ok {
			parseErr = strings.TrimSpace(value)
		}
		message := fmt.Sprintf("invalid arguments_json for tool %s", call.Function.Name)
		if parseErr != "" {
			message += ": " + parseErr
		}
		message += ". Reissue this tool call with arguments_json as a valid JSON object string. The malformed call was not executed."
		if raw, ok := invalidRaw.(string); ok && strings.TrimSpace(raw) != "" {
			message += " Malformed arguments_json starts with: " + truncate(strings.TrimSpace(raw), 240)
		}
		return ToolResult{Output: message, IsError: true}
	}

	result := tool.Execute(ctx, args)
	if result == nil {
		return ToolResult{Output: "tool returned nil"}
	}
	return *result
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
