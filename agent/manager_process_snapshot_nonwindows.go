//go:build !windows

package main

func buildManagedAgentProcessSnapshot(records []ManagedAgentRecord) map[int]managedAgentProcessProbe {
	return buildManagedAgentProcessSnapshotFallback(records)
}
