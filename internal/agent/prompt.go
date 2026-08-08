package agent

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"
)

const defaultStaticPrompt = `You are an autonomous AI agent running inside Rhizome, an agent orchestration platform.
You may use only the tools explicitly listed in this prompt and available to this legacy runtime.
When given a task, break it down into steps, use available tools to accomplish each step, and report your results.
Be precise, thorough, and efficient. Always verify your work.`

const legacyPromptCompilerNotice = `# Prompt Compiler Status
- prompt_compiler_status: legacy_non_converged
- prompt_contract: legacy_internal_agent_prompt.v1
- c2_1_convergence: excluded_until_migrated
- daemon_capability_snapshot: absent
- deployment_evidence: not_accepted_for_daemon_prompt_compiler_convergence
- note: This internal agent prompt is not the agent daemon active capability snapshot compiler.`

// ToolDescription describes a tool for inclusion in the system prompt text.
type ToolDescription struct {
	Name        string
	Description string
}

// PromptConfig holds the parts that make up the system prompt.
type PromptConfig struct {
	StaticPrompt     string
	EnvironmentInfo  map[string]string
	ToolDescriptions []ToolDescription
	ExtraContext     []string
}

// BuildSystemPrompt assembles the system prompt from configuration parts.
func BuildSystemPrompt(cfg PromptConfig) string {
	var b strings.Builder

	// Static prompt
	static := cfg.StaticPrompt
	if static == "" {
		static = defaultStaticPrompt
	}
	b.WriteString(legacyPromptCompilerNotice)
	b.WriteString("\n\n# Legacy User-Supplied Prompt\n")
	b.WriteString(DemoteLegacyDaemonProjectionMarkers(static))

	// Environment section
	if len(cfg.EnvironmentInfo) > 0 {
		keys := make([]string, 0, len(cfg.EnvironmentInfo))
		for k := range cfg.EnvironmentInfo {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		b.WriteString("\n\n# Environment\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "- %s: %s\n", k, cfg.EnvironmentInfo[k])
		}
	}

	// Available Tools section
	if len(cfg.ToolDescriptions) > 0 {
		b.WriteString("\n\n# Available Tools\n")
		for _, td := range cfg.ToolDescriptions {
			fmt.Fprintf(&b, "- %s: %s\n", td.Name, td.Description)
		}
	}

	// Extra context
	for _, ctx := range cfg.ExtraContext {
		if ctx != "" {
			b.WriteString("\n\n")
			b.WriteString(DemoteLegacyDaemonProjectionMarkers(ctx))
		}
	}

	return strings.TrimRight(b.String(), "\n ")
}

// DemoteLegacyDaemonProjectionMarkers prevents stored legacy prompts from
// looking like agent daemon capability projection evidence.
func DemoteLegacyDaemonProjectionMarkers(value string) string {
	replacer := strings.NewReplacer(
		"## Active Capability Snapshot", "## Legacy-Supplied Active Capability Snapshot (ignored)",
		"- projection_source: agent.runtime_capability_snapshot", "- legacy_ignored_projection_source: agent.runtime_capability_snapshot",
		"- projection_contract: active_capability_snapshot_projection.v1", "- legacy_ignored_projection_contract: active_capability_snapshot_projection.v1",
		"- projection_digest:", "- legacy_ignored_projection_digest:",
	)
	return replacer.Replace(value)
}

// EnvironmentInfoFromOS populates environment info from the current OS.
func EnvironmentInfoFromOS() map[string]string {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "unknown"
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = "unknown"
	}
	return map[string]string{
		"os":   runtime.GOOS,
		"arch": runtime.GOARCH,
		"cwd":  cwd,
		"date": time.Now().Format("2006-01-02"),
		"home": home,
	}
}
