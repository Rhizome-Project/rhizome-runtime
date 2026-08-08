package sqlite

// Read-surface budgets keep expensive report entrypoints bounded without
// changing their core semantics. Defaults remain conservative, but callers
// cannot ask for arbitrarily large windows that would bloat responses or
// amplify read-path work.
const (
	readSurfaceReplayLimitDefault   = 500
	readSurfaceReplayLimitMax       = 500
	readSurfaceReportLimitDefault   = 20
	readSurfaceReportLimitMax       = 128
	readSurfaceFrontierLimitDefault = 3
	readSurfaceFrontierLimitMax     = 10
	readSurfaceClusterLimitDefault  = 20
	readSurfaceClusterLimitMax      = 128
)

func clampReadSurfaceLimit(limit, defaultLimit, maxLimit int) int {
	if limit <= 0 {
		limit = defaultLimit
	}
	if maxLimit > 0 && limit > maxLimit {
		return maxLimit
	}
	return limit
}
