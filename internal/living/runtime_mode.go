package living

import (
	"fmt"
	"net/url"
	"strings"
)

type RuntimeMode string

const (
	RuntimeModeObserveOnly    RuntimeMode = "observe_only"
	RuntimeModeLocalExecution RuntimeMode = "local_execution"
	RuntimeModeRhizomeNative  RuntimeMode = "rhizome_native"
)

func normalizeRuntimeMode(raw string) RuntimeMode {
	switch RuntimeMode(strings.ToLower(strings.TrimSpace(raw))) {
	case RuntimeModeLocalExecution:
		return RuntimeModeLocalExecution
	case RuntimeModeRhizomeNative:
		return RuntimeModeRhizomeNative
	default:
		return RuntimeModeObserveOnly
	}
}

func validRuntimeMode(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(RuntimeModeObserveOnly), string(RuntimeModeLocalExecution), string(RuntimeModeRhizomeNative):
		return true
	default:
		return false
	}
}

func supportedEmbeddedRhizomeURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(parsed.Scheme)) {
	case "direct", "embedded", "sqlite", "file":
		return true
	default:
		return false
	}
}

func ValidateRunContract(cfg Config) error {
	mode := cfg.RuntimeMode()
	switch mode {
	case RuntimeModeObserveOnly:
		if !supportedEmbeddedRhizomeURL(cfg.RhizomeURL) {
			return fmt.Errorf(
				"living mode %q requires direct/embedded rhizome_url; remote topology %q is not supported by current CLI",
				mode,
				strings.TrimSpace(cfg.RhizomeURL),
			)
		}
		return nil
	case RuntimeModeLocalExecution:
		return fmt.Errorf("living mode %q is not wired in current CLI; supported mode today is %q", mode, RuntimeModeObserveOnly)
	case RuntimeModeRhizomeNative:
		return fmt.Errorf("living mode %q is not implemented yet", mode)
	default:
		return fmt.Errorf("unsupported living runtime mode %q", mode)
	}
}

func ValidateDependencyContract(cfg Config, deps *BrainDeps) error {
	mode := cfg.RuntimeMode()
	switch mode {
	case RuntimeModeObserveOnly:
		return nil
	case RuntimeModeLocalExecution:
		missing := make([]string, 0, 6)
		if deps == nil || deps.TaskRunner == nil {
			missing = append(missing, "task_runner")
		}
		if deps == nil || deps.WorkerRunner == nil {
			missing = append(missing, "worker_runner")
		}
		if deps == nil || deps.Triager == nil {
			missing = append(missing, "triager")
		}
		if deps == nil || deps.Evaluator == nil {
			missing = append(missing, "evaluator")
		}
		if deps == nil || deps.ReflectionLLM == nil {
			missing = append(missing, "reflection_llm")
		}
		if deps == nil || deps.SituationLLM == nil {
			missing = append(missing, "situation_llm")
		}
		if len(missing) > 0 {
			return fmt.Errorf("living mode %q requires explicit non-noop dependencies: %s", mode, strings.Join(missing, ", "))
		}
		return nil
	case RuntimeModeRhizomeNative:
		return fmt.Errorf("living mode %q is not implemented yet", mode)
	default:
		return fmt.Errorf("unsupported living runtime mode %q", mode)
	}
}
