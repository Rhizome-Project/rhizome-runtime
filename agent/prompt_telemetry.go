package main

// Prompt telemetry (lane TE-02 "Prompt Telemetry").
//
// This file emits per-LLM-call prompt composition byte counts so a byte-budget
// table can rank optimization work. It is measurement only: it does not change
// prompt content, message flow, or tool loop behavior.
//
// FROZEN SECTION SCHEMA (names fixed by the metrics contract owner; do not rename):
//   static_preamble     - backend-injected static instruction text (the
//                          "You are the reasoning backend..." header + decision
//                          rules in buildCodexExecPrompt / buildQwenExecPrompt).
//                          For the OpenAI HTTP backend this is 0.
//   system              - bytes of messages with role "system".
//   task_packet         - bytes of the FIRST non-system user message
//                          (the task+work-packet message).
//   history             - bytes of all remaining messages EXCEPT tool-role
//                          messages (assistant turns, later user turns),
//                          excluding tool_output_history. For assistant
//                          messages with tool calls, the marshaled tool-call
//                          arguments bytes are counted into history as well.
//   tool_output_history - bytes of all role "tool" messages' Content.
//   tools_json          - bytes of marshaled tool definitions JSON as actually
//                          sent.
//   total               - total prompt bytes actually sent to the backend
//                          (for codex/qwen: len of the built stdin prompt
//                          string; for OpenAI: len of marshaled request
//                          messages+tools).
//
// EMISSION DESIGN (chosen: loop-side computation + backend exact-byte logging).
//
// The provider-agnostic shared tool loop (runToolLoopDetailed) already holds the
// exact messages+tools it sends every iteration, so it computes section bytes
// there and surfaces them to the runtime observer via an OPTIONAL type-asserted
// hook (promptSectionObserver). This keeps the ToolLoopObserver interface
// unchanged (additive only) and covers every backend uniformly, including codex
// and qwen, without per-agent shared mutable state. The codex/qwen backend
// static preamble length is a measurable constant (codexStaticPreambleBytes /
// qwenStaticPreambleBytes) that the loop adds in for the static_preamble row and
// the codex/qwen total.
//
// Separately, the backend prompt builders (buildCodexExecPrompt /
// buildQwenExecPrompt) and the OpenAI Chat path log one exact "[prompt-bytes]"
// line per call from the precise bytes they actually marshal/stream. The
// loop-side computation is a recomputation from the same inputs (no shared
// recorder), so there is no cross-agent race to guard.

import (
	"encoding/json"
	"log"
	"strings"
)

// promptSectionBytesLogPrefix is the stable greppable prefix for the per-call
// prompt composition log line. Keep it stable; downstream tooling greps it.
const promptSectionBytesLogPrefix = "[prompt-bytes]"

// logPromptSectionBytes emits one stable, greppable log line per LLM call with
// the prompt section byte breakdown for the given backend ("codex", "qwen", or
// "openai").
func logPromptSectionBytes(backend string, sections PromptSectionBytes) {
	log.Printf("%s backend=%s total=%d static_preamble=%d system=%d task_packet=%d history=%d tool_output_history=%d tools_json=%d",
		promptSectionBytesLogPrefix,
		backend,
		sections.Total,
		sections.StaticPreamble,
		sections.System,
		sections.TaskPacket,
		sections.History,
		sections.ToolOutputHistory,
		sections.ToolsJSON,
	)
}

// PromptSectionBytes is the per-call prompt composition byte breakdown.
// JSON tags are the frozen section names; do not rename.
type PromptSectionBytes struct {
	StaticPreamble    int `json:"static_preamble"`
	System            int `json:"system"`
	TaskPacket        int `json:"task_packet"`
	History           int `json:"history"`
	ToolOutputHistory int `json:"tool_output_history"`
	ToolsJSON         int `json:"tools_json"`
	Total             int `json:"total"`
}

// promptSectionObserver is an OPTIONAL extension implemented by tool-loop
// observers that want per-iteration prompt section bytes. It is intentionally
// NOT part of the ToolLoopObserver interface so existing observers keep
// compiling; the loop discovers it with a type assertion.
type promptSectionObserver interface {
	OnPromptSectionBytes(iteration int, sections PromptSectionBytes)
}

// messageContentBytes returns the byte length attributable to a single message
// for the history/tool_output sections: the Content bytes plus, for assistant
// messages carrying tool calls, the marshaled tool-call arguments bytes. The
// arguments are the model-generated payload that actually grows history.
func messageToolCallArgumentBytes(msg Message) int {
	if len(msg.ToolCalls) == 0 {
		return 0
	}
	total := 0
	for _, call := range msg.ToolCalls {
		total += len(call.Function.Name)
		total += len(call.Function.Arguments)
	}
	return total
}

// computePromptSectionBytes attributes message and tool-definition bytes to the
// frozen sections. staticPreamble is the backend's static instruction byte
// count (0 for OpenAI). total is the exact total prompt bytes the backend sent;
// when total <= 0 the function derives a lower-bound total from the section
// sum so callers without an exact figure still get a consistent row.
func computePromptSectionBytes(messages []Message, toolsJSON []byte, staticPreamble int, total int) PromptSectionBytes {
	sections := PromptSectionBytes{
		StaticPreamble: staticPreamble,
		ToolsJSON:      len(toolsJSON),
	}

	taskPacketAssigned := false
	for _, msg := range messages {
		role := strings.ToLower(strings.TrimSpace(msg.Role))
		switch role {
		case "system":
			sections.System += len(msg.Content)
		case "tool":
			sections.ToolOutputHistory += len(msg.Content)
		case "user":
			if !taskPacketAssigned {
				// FIRST non-system user message is the task+work-packet message.
				sections.TaskPacket += len(msg.Content)
				taskPacketAssigned = true
			} else {
				sections.History += len(msg.Content)
			}
		default:
			// assistant turns and any other non-tool roles count as history,
			// including marshaled tool-call argument bytes.
			sections.History += len(msg.Content) + messageToolCallArgumentBytes(msg)
		}
	}

	sectionSum := sections.StaticPreamble + sections.System + sections.TaskPacket +
		sections.History + sections.ToolOutputHistory + sections.ToolsJSON
	if total > sectionSum {
		sections.Total = total
	} else {
		sections.Total = sectionSum
	}
	return sections
}

// chatLLMStaticPreambleBytes returns the backend-injected static preamble byte
// length for the concrete backend behind a ChatLLM, used for loop-side
// (provider-agnostic) section attribution. OpenAI injects no preamble (0); the
// codex and qwen exec backends prepend their measured static instruction
// header. Unknown backends default to 0.
func chatLLMStaticPreambleBytes(llm ChatLLM) int {
	switch llm.(type) {
	case *CodexExecLLM:
		return len(codexStaticPreamble())
	case *QwenExecLLM:
		return len(qwenStaticPreamble())
	default:
		return 0
	}
}

// marshalToolsForTelemetry marshals tool definitions the same way the OpenAI
// backend serializes them on the wire, for the tools_json section byte count.
// It is best-effort: a marshal error yields zero bytes rather than failing the
// call.
func marshalToolsForTelemetry(tools []ToolDef) []byte {
	if len(tools) == 0 {
		return nil
	}
	raw, err := json.Marshal(tools)
	if err != nil {
		return nil
	}
	return raw
}
