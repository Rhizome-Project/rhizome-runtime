package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type RhizomeConnectionProfile struct {
	RPCEndpoint       string `json:"rpc_endpoint"`
	HostURL           string `json:"host_url"`
	WorkspaceID       string `json:"workspace_id"`
	WorkspaceName     string `json:"workspace_name,omitempty"`
	WorkspacePassword string `json:"workspace_password,omitempty"`
	AgentID           string `json:"agent_id,omitempty"`
	AgentToken        string `json:"agent_token,omitempty"`
	OwnerUserID       string `json:"owner_user_id,omitempty"`
	UpdatedAt         string `json:"updated_at,omitempty"`
}

func rhizomeProfilePath() string {
	return agentRuntimeConfigPath("rhizome_profile.json")
}

func LoadRhizomeProfile() RhizomeConnectionProfile {
	data, err := os.ReadFile(rhizomeProfilePath())
	if err != nil {
		return RhizomeConnectionProfile{}
	}
	var profile RhizomeConnectionProfile
	if err := json.Unmarshal(data, &profile); err != nil {
		return RhizomeConnectionProfile{}
	}
	if strings.TrimSpace(profile.HostURL) == "" {
		profile.HostURL = hostURLForRPC(profile.RPCEndpoint)
	}
	if strings.TrimSpace(profile.RPCEndpoint) == "" {
		profile.RPCEndpoint = defaultRPCEndpoint(profile.HostURL)
	}
	return profile
}

func SaveRhizomeProfile(profile RhizomeConnectionProfile) error {
	path := rhizomeProfilePath()
	if path == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	raw, _, err := marshalRhizomeProfileForWrite(profile)
	if err != nil {
		return err
	}
	return writeManagerStateBytes("save_rhizome_profile", path, raw, 0o600)
}

func marshalRhizomeProfileForWrite(profile RhizomeConnectionProfile) ([]byte, RhizomeConnectionProfile, error) {
	if strings.TrimSpace(profile.HostURL) == "" {
		profile.HostURL = hostURLForRPC(profile.RPCEndpoint)
	}
	if strings.TrimSpace(profile.RPCEndpoint) == "" {
		profile.RPCEndpoint = defaultRPCEndpoint(profile.HostURL)
	}
	profile.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return nil, RhizomeConnectionProfile{}, err
	}
	return raw, profile, nil
}

func defaultRPCEndpoint(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if strings.HasSuffix(host, "/rpc") {
		return host
	}
	return strings.TrimRight(host, "/") + "/rpc"
}

func hostURLForRPC(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if strings.HasSuffix(endpoint, "/rpc") {
		return strings.TrimSuffix(endpoint, "/rpc")
	}
	return endpoint
}
