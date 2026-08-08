package sqlite

// Read-surface policy makes the heavy-read contract explicit.
// The helper fields stay internal so the runtime keeps the same JSON shape,
// but the strategy is visible in code and testable without guessing.
type ReadSurfacePolicy struct {
	ConsumerClass   string            `json:"consumer_class"`
	Materialization string            `json:"materialization"`
	Budget          ReadSurfaceBudget `json:"budget"`
	Shedding        []string          `json:"shedding,omitempty"`
	Notes           []string          `json:"notes,omitempty"`
}

type ReadSurfaceBudget struct {
	ReplayEvents  int `json:"replay_events,omitempty"`
	ClusterItems  int `json:"cluster_items,omitempty"`
	FrontierItems int `json:"frontier_items,omitempty"`
	TensionItems  int `json:"tension_items,omitempty"`
}

const (
	readSurfaceConsumerReplay                 = "replay"
	readSurfaceConsumerObserver               = "observer"
	readSurfaceConsumerOperator               = "operator"
	readSurfaceConsumerDashboard              = "dashboard"
	readSurfaceMaterializationLiveWindow      = "live_window"
	readSurfaceMaterializationDerivedSnapshot = "derived_snapshot"
	readSurfaceMaterializationFrontierCapped  = "frontier_capped_snapshot"
	readSurfaceMaterializationBasisSnapshot   = "basis_snapshot"
	readSurfaceSheddingTruncateTail           = "truncate_tail"
	readSurfaceSheddingCapClusters            = "cap_clusters"
	readSurfaceSheddingCapFrontier            = "cap_frontier"
	readSurfaceSheddingSuppressVerboseDetails = "suppress_verbose_details"
	readSurfaceSheddingExcludeSynthetic       = "exclude_synthetic"
)

func runtimeReplayReadSurfacePolicy(filter RuntimeReplayFilter) ReadSurfacePolicy {
	budget := ReadSurfaceBudget{ReplayEvents: clampReadSurfaceLimit(filter.Limit, readSurfaceReplayLimitDefault, readSurfaceReplayLimitMax)}
	shedding := []string{readSurfaceSheddingTruncateTail}
	if filter.ExcludeSynthetic {
		shedding = append(shedding, readSurfaceSheddingExcludeSynthetic)
	}
	return ReadSurfacePolicy{
		ConsumerClass:   readSurfaceConsumerReplay,
		Materialization: readSurfaceMaterializationLiveWindow,
		Budget:          budget,
		Shedding:        shedding,
		Notes: []string{
			"returns a bounded live window rather than an unbounded journal dump",
		},
	}
}

func instrumentationReadSurfacePolicy(filter InstrumentationReportFilter) ReadSurfacePolicy {
	return ReadSurfacePolicy{
		ConsumerClass:   readSurfaceConsumerDashboard,
		Materialization: readSurfaceMaterializationDerivedSnapshot,
		Budget: ReadSurfaceBudget{
			ReplayEvents: clampReadSurfaceLimit(filter.Limit, readSurfaceReplayLimitDefault, readSurfaceReplayLimitMax),
			ClusterItems: clampReadSurfaceLimit(filter.ClusterLimit, readSurfaceClusterLimitDefault, readSurfaceClusterLimitMax),
		},
		Shedding: []string{
			readSurfaceSheddingTruncateTail,
			readSurfaceSheddingCapClusters,
			readSurfaceSheddingExcludeSynthetic,
		},
		Notes: []string{
			"dashboard surface is derived from replay plus cluster projection",
			"cluster detail is capped before materialization to avoid wide fan-out",
		},
	}
}

func controlReadSurfacePolicy(filter ControlReportFilter, clusterLimit int) ReadSurfacePolicy {
	return ReadSurfacePolicy{
		ConsumerClass:   readSurfaceConsumerOperator,
		Materialization: readSurfaceMaterializationDerivedSnapshot,
		Budget: ReadSurfaceBudget{
			ReplayEvents: clampReadSurfaceLimit(controlReadRuntimeEventWindow, readSurfaceReplayLimitDefault, readSurfaceReplayLimitMax),
			ClusterItems: clusterLimit,
			TensionItems: controlReadTensionWindow,
		},
		Shedding: []string{
			readSurfaceSheddingTruncateTail,
			readSurfaceSheddingCapClusters,
		},
		Notes: []string{
			"operator-facing control report prefers the highest-pressure clusters and trims the rest",
		},
	}
}

func corridorReadinessPolicy(filter CorridorReadinessFilter, clusterLimit int) ReadSurfacePolicy {
	return ReadSurfacePolicy{
		ConsumerClass:   readSurfaceConsumerObserver,
		Materialization: readSurfaceMaterializationDerivedSnapshot,
		Budget: ReadSurfaceBudget{
			ReplayEvents: clampReadSurfaceLimit(corridorReadRuntimeEventWindow, readSurfaceReplayLimitDefault, readSurfaceReplayLimitMax),
			ClusterItems: clusterLimit,
		},
		Shedding: []string{
			readSurfaceSheddingTruncateTail,
			readSurfaceSheddingCapClusters,
		},
		Notes: []string{
			"observer-facing corridor readiness stays cluster-local and avoids broad history scans",
		},
	}
}

func clusterControlStatePolicy(filter ClusterControlStateFilter) ReadSurfacePolicy {
	return ReadSurfacePolicy{
		ConsumerClass:   readSurfaceConsumerOperator,
		Materialization: readSurfaceMaterializationBasisSnapshot,
		Budget: ReadSurfaceBudget{
			ReplayEvents: clampReadSurfaceLimit(clusterControlTickLimit(filter.ProtoClusterID), readSurfaceReplayLimitDefault, readSurfaceReplayLimitMax),
			ClusterItems: clampReadSurfaceLimit(filter.Limit, readSurfaceReportLimitDefault, readSurfaceReportLimitMax),
		},
		Shedding: []string{
			readSurfaceSheddingTruncateTail,
			readSurfaceSheddingCapClusters,
		},
		Notes: []string{
			"state report materializes from the advisory control basis rather than a full live scan",
		},
	}
}

func unifiedControlReadSurfacePolicy(filter UnifiedControlReportFilter) ReadSurfacePolicy {
	return ReadSurfacePolicy{
		ConsumerClass:   readSurfaceConsumerOperator,
		Materialization: readSurfaceMaterializationFrontierCapped,
		Budget: ReadSurfaceBudget{
			FrontierItems: clampReadSurfaceLimit(filter.FrontierLimit, readSurfaceFrontierLimitDefault, readSurfaceFrontierLimitMax),
		},
		Shedding: []string{
			readSurfaceSheddingCapFrontier,
			readSurfaceSheddingSuppressVerboseDetails,
		},
		Notes: []string{
			"unified control keeps only a bounded frontier of candidate actions",
			"advisory-only control is a deliberate shedding choice under load",
		},
	}
}
