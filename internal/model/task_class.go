package model

import "strings"

const (
	TaskClassUnknown     = "UNKNOWN"
	TaskClassProof       = "PROOF"
	TaskClassExploration = "EXPLORATION"
	TaskClassIntegration = "INTEGRATION"
	TaskClassIncident    = "INCIDENT"
)

const (
	TaskClassSourceUnset             = "UNSET"
	TaskClassSourceExplicit          = "EXPLICIT"
	TaskClassSourceTemplateDefault   = "TEMPLATE_DEFAULT"
	TaskClassSourceHeuristicFallback = "HEURISTIC_FALLBACK"
)

var validTaskClasses = map[string]struct{}{
	TaskClassUnknown:     {},
	TaskClassProof:       {},
	TaskClassExploration: {},
	TaskClassIntegration: {},
	TaskClassIncident:    {},
}

var validTaskClassSources = map[string]struct{}{
	TaskClassSourceUnset:             {},
	TaskClassSourceExplicit:          {},
	TaskClassSourceTemplateDefault:   {},
	TaskClassSourceHeuristicFallback: {},
}

func NormalizeTaskClass(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if _, ok := validTaskClasses[value]; ok {
		return value
	}
	return ""
}

func ValidTaskClass(value string) bool {
	_, ok := validTaskClasses[NormalizeTaskClass(value)]
	return ok
}

func NormalizeTaskClassSource(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if _, ok := validTaskClassSources[value]; ok {
		return value
	}
	return ""
}

func ValidTaskClassSource(value string) bool {
	_, ok := validTaskClassSources[NormalizeTaskClassSource(value)]
	return ok
}
