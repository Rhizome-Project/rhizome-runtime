package main

import (
	"os"
	"testing"
)

func trustedAgentProcessStateForTest(t *testing.T, record ManagedAgentRecord, pid int) AgentProcessState {
	t.Helper()

	record = normalizeManagedAgentRecord(record)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error: %v", err)
	}
	executableHash, err := managerFileSHA256(executable)
	if err != nil {
		t.Fatalf("managerFileSHA256() error: %v", err)
	}
	args := managedAgentDaemonArgs(record)
	return AgentProcessState{
		PID:                 pid,
		Executable:          executable,
		ExecutableSHA256:    executableHash,
		Mode:                string(RuntimeModeDaemon),
		Workdir:             record.Workdir,
		Args:                args,
		ArgsDigest:          managedAgentArgsDigest(args),
		RuntimeConfigDigest: managedAgentRuntimeConfigDigest(managedAgentStartRuntimeConfig(record)),
		BuildRevision:       managerCurrentRuntimeBuildIdentity().VCSRevision,
	}
}

func saveTrustedAgentProcessStateForTest(t *testing.T, record ManagedAgentRecord, pid int) {
	t.Helper()

	if err := SaveAgentProcessState(record.Workdir, trustedAgentProcessStateForTest(t, record, pid)); err != nil {
		t.Fatalf("SaveAgentProcessState() error: %v", err)
	}
}
