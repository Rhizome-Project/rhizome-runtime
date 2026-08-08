//go:build windows

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

const managedAgentWindowsProcessSnapshotTimeout = 4 * time.Second

type managedAgentWindowsProcessSnapshotRow struct {
	ProcessID      int    `json:"ProcessId"`
	ExecutablePath string `json:"ExecutablePath"`
	CommandLine    string `json:"CommandLine"`
}

func buildManagedAgentProcessSnapshot(records []ManagedAgentRecord) map[int]managedAgentProcessProbe {
	out := map[int]managedAgentProcessProbe{}
	for _, record := range records {
		state := LoadAgentProcessState(normalizeManagedAgentRecord(record).Workdir)
		if state.PID <= 0 {
			continue
		}
		if _, ok := out[state.PID]; ok {
			continue
		}
		out[state.PID] = managedAgentProcessProbe{PID: state.PID}
	}
	if len(out) == 0 {
		return out
	}
	pids := make([]int, 0, len(out))
	for pid := range out {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	filterParts := make([]string, 0, len(pids))
	for _, pid := range pids {
		filterParts = append(filterParts, fmt.Sprintf("ProcessId=%d", pid))
	}
	script := fmt.Sprintf(
		`$ErrorActionPreference='Stop'; Get-CimInstance Win32_Process -Filter %q | Select-Object ProcessId,ExecutablePath,CommandLine | ConvertTo-Json -Compress`,
		strings.Join(filterParts, " OR "),
	)
	ctx, cancel := context.WithTimeout(context.Background(), managedAgentWindowsProcessSnapshotTimeout)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "powershell", "-NoProfile", "-NonInteractive", "-Command", script).Output()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		for pid, probe := range out {
			probe.LookupErr = err
			out[pid] = probe
		}
		return out
	}
	rows, err := parseManagedAgentWindowsProcessSnapshotRows(strings.TrimSpace(string(raw)))
	if err != nil {
		for pid, probe := range out {
			probe.LookupErr = err
			out[pid] = probe
		}
		return out
	}
	for _, row := range rows {
		probe := out[row.ProcessID]
		probe.PID = row.ProcessID
		probe.Exists = true
		probe.ExecutablePath = strings.TrimSpace(row.ExecutablePath)
		probe.CommandLine = strings.TrimSpace(row.CommandLine)
		out[row.ProcessID] = probe
	}
	return out
}

func parseManagedAgentWindowsProcessSnapshotRows(raw string) ([]managedAgentWindowsProcessSnapshotRow, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "[") {
		var rows []managedAgentWindowsProcessSnapshotRow
		if err := json.Unmarshal([]byte(raw), &rows); err != nil {
			return nil, err
		}
		return rows, nil
	}
	var row managedAgentWindowsProcessSnapshotRow
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		return nil, err
	}
	if row.ProcessID == 0 {
		return nil, nil
	}
	return []managedAgentWindowsProcessSnapshotRow{row}, nil
}
