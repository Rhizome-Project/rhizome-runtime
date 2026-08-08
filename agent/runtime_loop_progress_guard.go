package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Tool-loop progress guard.
//
// MOTIVATION: a 20-30 iteration tool loop with zero durable effect is the most
// expensive failure shape -- each iteration re-pays the whole transcript. The
// loop today only stops on natural completion or the iteration cap, so a loop
// that re-issues the same read-only calls forever burns to the cap before any
// outcome is recorded. This guard stops such a loop EARLY with a durable
// blocked/yield result (transcript and ToolResults trace preserved), instead of
// burning to the cap.
//
// Novelty rule v1 (mechanical, no model trust):
//   - Each tool call is fingerprinted as sha256(tool name | normalized args |
//     output digest | IsError). "Normalized args" is the canonical-JSON form of
//     the arguments object (object keys sorted) so semantically identical calls
//     with reordered keys collapse to one fingerprint.
//   - An iteration is "no-progress" iff EVERY call in it has a fingerprint that
//     was already seen earlier in the run AND no call in it is in a mutating
//     class (reusing successfulToolCallIsDurableProgress, the existing durable
//     classification). A purely-read-only iteration whose every call repeats a
//     prior outcome contributes no new durable effect and no novel outcome.
//   - K (default 3) consecutive no-progress iterations triggers an early stop.
//
// PLACEMENT: the detector is fed from the shared loop (runToolLoopDetailed in
// tools.go) through an optional observer-extension interface
// (toolLoopProgressObserver), mirroring the promptSectionObserver hook already
// wired there. The loop owns no guard policy; it only asks the detector after
// each iteration whether to stop. Default OFF behind SetDefaultToolLoopProgressGuard
// (analogous to compaction's SetDefaultToolLoopCompaction), so existing tests
// and live behavior are unchanged until the flag is flipped on.

const (
	defaultProgressGuardWindow = 3

	// toolLoopProgressGuardMarker is the machine-greppable marker emitted in the
	// synthetic blocked result when the guard stops a loop early. Watchdog /
	// audit code can grep for it without trusting model prose.
	toolLoopProgressGuardMarker = "tool-loop-progress-guard: no novel outcomes in last K iterations"
)

// ToolLoopProgressGuardConfig controls the progress guard.
type ToolLoopProgressGuardConfig struct {
	Enabled bool
	// Window is the number of consecutive no-progress iterations that triggers
	// the early stop (K). Default defaultProgressGuardWindow.
	Window int
}

func DefaultToolLoopProgressGuardConfig() ToolLoopProgressGuardConfig {
	return ToolLoopProgressGuardConfig{
		Enabled: false,
		Window:  defaultProgressGuardWindow,
	}
}

func (c ToolLoopProgressGuardConfig) normalized() ToolLoopProgressGuardConfig {
	if c.Window <= 0 {
		c.Window = defaultProgressGuardWindow
	}
	return c
}

// Process-wide default, set once at startup by runtime flag wiring before any
// agent loop starts; tool loops snapshot it per call so a mid-run flip cannot
// change guard shape between iterations (same discipline as compaction).
var (
	toolLoopProgressGuardMu      sync.RWMutex
	toolLoopProgressGuardDefault = DefaultToolLoopProgressGuardConfig()
)

func SetDefaultToolLoopProgressGuard(config ToolLoopProgressGuardConfig) {
	toolLoopProgressGuardMu.Lock()
	defer toolLoopProgressGuardMu.Unlock()
	toolLoopProgressGuardDefault = config.normalized()
}

func CurrentToolLoopProgressGuard() ToolLoopProgressGuardConfig {
	toolLoopProgressGuardMu.RLock()
	defer toolLoopProgressGuardMu.RUnlock()
	return toolLoopProgressGuardDefault
}

// toolLoopProgressGuard is the per-run novelty/progress detector. It is not safe
// for concurrent use; one instance is created per tool loop run.
type toolLoopProgressGuard struct {
	config            ToolLoopProgressGuardConfig
	seen              map[string]struct{}
	noProgressStreak  int
	lastStopIteration int
	stopped           bool
}

func newToolLoopProgressGuard(config ToolLoopProgressGuardConfig) *toolLoopProgressGuard {
	config = config.normalized()
	return &toolLoopProgressGuard{
		config: config,
		seen:   make(map[string]struct{}),
	}
}

// observeIteration records one loop iteration's (tool call, result) pairs and
// reports whether the guard should stop the loop now. calls and results are
// index-aligned (results[j] answers calls[j]). An empty iteration (no tool
// calls) never triggers a stop and resets the streak: the loop is converging on
// a terminal text answer, which is handled by the loop's own no-tool-calls exit.
func (g *toolLoopProgressGuard) observeIteration(calls []ToolCall, results []ToolResult) bool {
	if g == nil || !g.config.Enabled {
		return false
	}
	if len(calls) == 0 {
		g.noProgressStreak = 0
		return false
	}

	iterationHadMutatingCall := false
	iterationHadNovelOutcome := false
	for j, call := range calls {
		var result ToolResult
		if j < len(results) {
			result = results[j]
		}
		if toolCallIsMutatingClass(call) {
			iterationHadMutatingCall = true
		}
		fingerprint := toolCallOutcomeFingerprint(call, result)
		if _, ok := g.seen[fingerprint]; !ok {
			iterationHadNovelOutcome = true
			g.seen[fingerprint] = struct{}{}
		}
	}

	if iterationHadMutatingCall || iterationHadNovelOutcome {
		g.noProgressStreak = 0
		return false
	}

	g.noProgressStreak++
	if g.noProgressStreak >= g.config.Window {
		g.stopped = true
		return true
	}
	return false
}

// toolCallIsMutatingClass reports whether a tool call belongs to a durable
// mutating class. It reuses successfulToolCallIsDurableProgress (the existing
// durable-progress classification used by the no-progress continue guard) so the
// two guards share one mutating-tool definition. write_file is treated as
// mutating unless its path is ephemeral progress output (matching
// taskRunTraceSuccessfulToolName's write_file:ephemeral carve-out).
func toolCallIsMutatingClass(call ToolCall) bool {
	name := strings.ToLower(strings.TrimSpace(call.Function.Name))
	if name == "" {
		return false
	}
	if name == "write_file" {
		var args struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err == nil {
			if writeFilePathIsEphemeralProgress(args.Path) {
				return false
			}
		}
		return true
	}
	return successfulToolCallIsDurableProgress(name)
}

// toolCallOutcomeFingerprint hashes the (tool name, normalized args, output
// digest, IsError) tuple so identical repeated calls collapse to one key.
func toolCallOutcomeFingerprint(call ToolCall, result ToolResult) string {
	name := strings.ToLower(strings.TrimSpace(call.Function.Name))
	normArgs := normalizeToolCallArguments(call.Function.Arguments)
	outputDigest := shortDigest(result.Output)
	errFlag := "0"
	if result.IsError {
		errFlag = "1"
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{name, normArgs, outputDigest, errFlag}, "\x1f")))
	return hex.EncodeToString(sum[:])
}

// normalizeToolCallArguments returns a canonical form of a tool call's argument
// string: if it parses as a JSON object, keys are sorted recursively so that
// reordered-but-equivalent argument objects produce the same string; otherwise
// the trimmed raw string is returned.
func normalizeToolCallArguments(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return raw
	}
	var b strings.Builder
	writeCanonicalJSON(&b, decoded)
	return b.String()
}

func writeCanonicalJSON(b *strings.Builder, value any) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			keyJSON, _ := json.Marshal(k)
			b.Write(keyJSON)
			b.WriteByte(':')
			writeCanonicalJSON(b, typed[k])
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, item := range typed {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonicalJSON(b, item)
		}
		b.WriteByte(']')
	default:
		leaf, _ := json.Marshal(typed)
		b.Write(leaf)
	}
}

// toolLoopProgressGuardStopContent builds the synthetic final Content emitted
// when the guard stops a loop early. It is a blocked-outcome structured result
// JSON (the contract resolveStructuredTaskResult already parses) so the runtime
// records a durable blocked/yield outcome rather than a fake success, and it
// carries the machine-greppable marker for audit/watchdog code.
func toolLoopProgressGuardStopContent(window, iterations int) string {
	marker := strings.Replace(toolLoopProgressGuardMarker, "last K iterations", fmt.Sprintf("last %d iterations", window), 1)
	summary := fmt.Sprintf("%s; stopping at iteration %d to avoid burning to the iteration cap with zero durable effect", marker, iterations)
	details := "The tool loop repeated read-only tool calls whose (tool, args, output, error) outcomes were already seen, " +
		"with no file write, state mutation, or other durable effect, for " + fmt.Sprintf("%d", window) +
		" consecutive iterations. The runtime stopped the loop early and is yielding/blocking this task instead of " +
		"reconstructing the same expensive context next cycle. Resume only with a concrete, novel action (a mutating tool " +
		"call or a genuinely new query), or park this lane with a typed blocker."
	payload := map[string]any{
		"outcome": "blocked",
		"summary": summary,
		"details": details,
		"blocked_on": []map[string]string{
			{
				"kind":   "no_loop_progress",
				"detail": marker,
			},
		},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		// Fallback: still emit the greppable marker so the stop is durable.
		return marker
	}
	return string(encoded)
}
