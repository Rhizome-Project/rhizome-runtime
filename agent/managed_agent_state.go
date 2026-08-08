package main

import "fmt"

func SaveManagedAgentRecordAndLocalRuntime(record ManagedAgentRecord, profile LocalRuntimeProfile) error {
	record = normalizeManagedAgentRecord(record)
	if record.AgentID == "" || record.Workdir == "" {
		return fmt.Errorf("managed agent requires agent_id and workdir")
	}

	root := agentRuntimeConfigRoot()
	if root == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	runtimePath := localRuntimeProfilePath(record.Workdir)
	if runtimePath == "" {
		return fmt.Errorf("local runtime profile path is unavailable")
	}

	return withManagerStateLock(root, true, func() error {
		if err := validateProviderReference(record.ProviderID); err != nil {
			return err
		}
		registry, err := loadBotRegistryFromDisk(botRegistryPath())
		if err != nil {
			return err
		}
		registry = normalizeBotRegistry(registry)
		upsertManagedAgentInRegistry(&registry, record)

		registryRaw, _, err := marshalBotRegistryForWrite(registry)
		if err != nil {
			return err
		}
		runtimeRaw, _, err := marshalLocalRuntimeProfileForWrite(profile)
		if err != nil {
			return err
		}

		registryPayloadPath, err := prepareManagerStatePayloadFile(root, "mat-bot-registry-", registryRaw, 0o600)
		if err != nil {
			return err
		}
		runtimePayloadPath, err := prepareManagerStatePayloadFile(root, "mat-local-runtime-", runtimeRaw, 0o600)
		if err != nil {
			return err
		}
		if err := writeLocalRuntimeMaterializationMarker(runtimePath, runtimeRaw); err != nil {
			return err
		}

		return materializeManagerStateEntriesLocked(root, "save_managed_agent_state", []managerStateMaterializeEntry{
			{
				TargetPath:  botRegistryPath(),
				PayloadPath: registryPayloadPath,
				Perm:        0o600,
			},
			{
				TargetPath:  runtimePath,
				PayloadPath: runtimePayloadPath,
				Perm:        0o600,
			},
		})
	})
}
