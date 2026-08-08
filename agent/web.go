package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type managerWebServer struct{}

const managerWebProcessActionRefreshTimeout = 2 * time.Second
const managerWebBulkProcessCleanupTimeout = 8 * time.Second

type managerWebOverview struct {
	Command         string                    `json:"command"`
	Defaults        BotManagerDefaults        `json:"defaults"`
	Agents          []managerWebAgentRow      `json:"agents"`
	Substrate       managerWebSubstrateStatus `json:"substrate"`
	Providers       []ProviderRecord          `json:"providers"`
	ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
	ProvidersError  string                    `json:"providers_error,omitempty"`
	CreateDefault   managerWebCreateDefault   `json:"create_default"`
}

type managerWebOverviewErrorResponse struct {
	Error string `json:"error"`
	managerWebOverview
}

type managerWebDefaultsResponse struct {
	BotManagerDefaults
	Command         string                    `json:"command"`
	Defaults        BotManagerDefaults        `json:"defaults"`
	Agents          []managerWebAgentRow      `json:"agents"`
	Substrate       managerWebSubstrateStatus `json:"substrate"`
	Providers       []ProviderRecord          `json:"providers"`
	ProviderCatalog []SupportedProviderOption `json:"provider_catalog"`
	ProvidersError  string                    `json:"providers_error,omitempty"`
	CreateDefault   managerWebCreateDefault   `json:"create_default"`
}

type managerWebAgentRow struct {
	Record            ManagedAgentRecord          `json:"record"`
	Process           ManagedAgentProcessStatus   `json:"process"`
	EffectiveIdentity managerWebEffectiveIdentity `json:"effective_identity"`
}

type managerWebAgentDetail struct {
	Record            ManagedAgentRecord          `json:"record"`
	Process           ManagedAgentProcessStatus   `json:"process"`
	LocalRuntime      LocalRuntimeProfile         `json:"local_runtime"`
	EffectiveIdentity managerWebEffectiveIdentity `json:"effective_identity"`
	LocalChatContract LocalChatContract           `json:"local_chat_contract"`
	Profile           AgentProfile                `json:"profile"`
	Live              managerLiveRuntimeStatus    `json:"live"`
	Catalog           managerWorkspaceCatalog     `json:"catalog"`
	Logs              ManagedAgentLogTail         `json:"logs"`
	managerWebOverview
}

type managerWebEffectiveIdentity struct {
	Source          string   `json:"source"`
	AgentID         string   `json:"agent_id,omitempty"`
	WorkspaceID     string   `json:"workspace_id,omitempty"`
	DisplayName     string   `json:"display_name,omitempty"`
	OwnerUserID     string   `json:"owner_user_id,omitempty"`
	Role            string   `json:"role,omitempty"`
	ProtocolVersion string   `json:"protocol_version,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
}

type managerWebCreateDefault struct {
	ParentDir  string `json:"parent_dir"`
	FolderName string `json:"folder_name"`
	Workdir    string `json:"workdir"`
}

type managerWebActionRequest struct {
	Action                          string                             `json:"action"`
	SkipOverview                    bool                               `json:"skip_overview,omitempty"`
	RepositoryDirtyPathAllowlist    []string                           `json:"repository_dirty_path_allowlist,omitempty"`
	ResumeContinuationWaiver        managedRunResumeContinuationWaiver `json:"resume_continuation_waiver,omitempty"`
	AllowResumeDirtyProjectCheckout bool                               `json:"allow_resume_dirty_project_checkout,omitempty"`
	AllowResumeLivePatchQueue       bool                               `json:"allow_resume_live_patch_queue,omitempty"`
	AllowResumeAgentRequests        bool                               `json:"allow_resume_agent_requests,omitempty"`
	AllowResumePendingTriggers      bool                               `json:"allow_resume_pending_triggers,omitempty"`
}

type managerWebBulkProcessRequest struct {
	Action                          string                             `json:"action"`
	AgentIDs                        []string                           `json:"agent_ids,omitempty"`
	Parallelism                     int                                `json:"parallelism,omitempty"`
	SkipOverview                    bool                               `json:"skip_overview,omitempty"`
	RepositoryDirtyPathAllowlist    []string                           `json:"repository_dirty_path_allowlist,omitempty"`
	ResumeContinuationWaiver        managedRunResumeContinuationWaiver `json:"resume_continuation_waiver,omitempty"`
	AllowResumeDirtyProjectCheckout bool                               `json:"allow_resume_dirty_project_checkout,omitempty"`
	AllowResumeLivePatchQueue       bool                               `json:"allow_resume_live_patch_queue,omitempty"`
	AllowResumeAgentRequests        bool                               `json:"allow_resume_agent_requests,omitempty"`
	AllowResumePendingTriggers      bool                               `json:"allow_resume_pending_triggers,omitempty"`
}

type managerWebBulkProcessAgentResult struct {
	AgentID  string                    `json:"agent_id"`
	OK       bool                      `json:"ok"`
	Message  string                    `json:"message,omitempty"`
	Error    string                    `json:"error,omitempty"`
	Warnings []string                  `json:"warnings,omitempty"`
	Process  ManagedAgentProcessStatus `json:"process"`
}

type managerWebBulkProcessTarget struct {
	Index  int
	Record ManagedAgentRecord
}

type managerWebControlRequest struct {
	Method  string `json:"method"`
	Payload any    `json:"payload"`
}

type managerWebAgentEditRequest struct {
	DisplayName *string `json:"display_name"`
	Workdir     *string `json:"workdir"`
	GroupID     *string `json:"group_id"`
	Role        *string `json:"role"`
	Tags        *string `json:"tags"`
	SoulPrompt  *string `json:"soul_prompt"`
	Remove      bool    `json:"remove"`
}

type managerWebDefaultUpdate struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

type managerWebOnboardRequest struct {
	ParentDir                string `json:"parent_dir"`
	FolderName               string `json:"folder_name"`
	Workdir                  string `json:"workdir"`
	AgentID                  string `json:"agent_id"`
	DisplayName              string `json:"display_name"`
	OwnerUserID              string `json:"owner_user_id"`
	ProviderID               string `json:"provider_id"`
	GroupID                  string `json:"group_id"`
	HostURL                  string `json:"host_url"`
	WorkspaceID              string `json:"workspace_id"`
	WorkspacePassword        string `json:"workspace_password"`
	Role                     string `json:"role"`
	PrimarySpecialization    string `json:"primary_specialization"`
	SecondarySpecializations string `json:"secondary_specializations"`
	DomainScope              string `json:"domain_scope"`
	Mission                  string `json:"mission"`
	LLMBackend               string `json:"llm_backend"`
	Model                    string `json:"model"`
}

func runWeb(args []string) error {
	flags := flag.NewFlagSet("web", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	host := flags.String("host", "127.0.0.1", "Host interface for the local dashboard")
	port := flags.Int("port", 0, "Port for the local dashboard (0 chooses a free port)")
	noOpen := flags.Bool("no-open", false, "Do not open the dashboard in a browser")
	if err := flags.Parse(args); err != nil {
		return err
	}

	hostValue := strings.TrimSpace(*host)
	if !isLoopbackDashboardHost(hostValue) {
		return fmt.Errorf("refusing non-loopback dashboard host %q; the manager web UI is local-only", hostValue)
	}

	addr := managerWebListenAddress(hostValue, *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen web dashboard: %w", err)
	}
	defer listener.Close()

	server := &http.Server{
		Handler:           managerWebLoopbackRequestGate(newManagerWebServer().routes(), listener.Addr()),
		ReadHeaderTimeout: 10 * time.Second,
	}

	url := "http://" + listener.Addr().String()
	fmt.Printf("%s web dashboard: %s\n", appCommandName, url)
	if !*noOpen {
		_ = openBrowserURL(url)
	}

	return server.Serve(listener)
}

func newManagerWebServer() *managerWebServer {
	return &managerWebServer{}
}

func (s *managerWebServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleDashboard)
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/defaults", s.handleDefaults)
	mux.HandleFunc("/api/onboard", s.handleOnboard)
	mux.HandleFunc("/api/providers", s.handleProviders)
	mux.HandleFunc("/api/providers/", s.handleProviderRoute)
	mux.HandleFunc("/api/agents/bulk_process", s.handleAgentsBulkProcess)
	mux.HandleFunc("/api/agents/", s.handleAgentRoute)
	mux.HandleFunc("/api/fs/list", s.handleFSList)
	return managerWebSecurityHeaders(mux)
}

func managerWebSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setManagerWebSecurityHeaders(w)
		next.ServeHTTP(w, r)
	})
}

func (s *managerWebServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(managerWebDashboardHTML()))
}

func (s *managerWebServer) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeManagerWebOverviewError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, managerWebCurrentOverview())
}

func managerWebCurrentOverview() managerWebOverview {
	registry := LoadBotRegistry()
	providerRegistry, providerErr := LoadProviderRegistryWithError()
	rows := make([]managerWebAgentRow, 0, len(registry.Agents))
	processSnapshot := managedAgentProcessSnapshotFunc(registry.Agents)
	for _, agent := range registry.Agents {
		localRuntime := LoadLocalRuntimeProfile(agent.Workdir)
		process := InspectManagedAgentProcessWithSnapshot(agent, processSnapshot)
		if process.Stale {
			if _, err := CleanupStaleManagedAgentProcessState(agent); err == nil {
				process = InspectManagedAgentProcess(agent)
			}
		}
		rows = append(rows, managerWebAgentRow{
			Record:            normalizeManagedAgentRecord(agent),
			Process:           process,
			EffectiveIdentity: managerWebEffectiveIdentityForRecord(agent, localRuntime),
		})
	}
	createDefault := managerWebCreateDefault{
		ParentDir:  defaultManagerCreateParentDir(registry, 0),
		FolderName: suggestNewAgentFolderName(registry),
	}
	if workdir, err := resolveManagerCreateWorkdir(createDefault.ParentDir, createDefault.FolderName); err == nil {
		createDefault.Workdir = workdir
	}
	providersError := ""
	if providerErr != nil {
		providersError = providerErr.Error()
	}
	return managerWebOverview{
		Command:         appCommandName,
		Defaults:        redactBotManagerDefaults(registry.Defaults),
		Agents:          rows,
		Substrate:       managerWebCurrentSubstrateForOverview(registry, providerRegistry, providerErr),
		Providers:       providerRegistry.Providers,
		ProviderCatalog: managerSupportedProviderCatalog(),
		ProvidersError:  providersError,
		CreateDefault:   createDefault,
	}
}

func writeManagerWebOverviewError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, managerWebOverviewErrorResponse{
		Error:              message,
		managerWebOverview: managerWebCurrentOverview(),
	})
}

func (s *managerWebServer) handleDefaults(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusOK, managerWebDefaultsResponse{
			BotManagerDefaults: overview.Defaults,
			Command:            overview.Command,
			Defaults:           overview.Defaults,
			Agents:             overview.Agents,
			Substrate:          overview.Substrate,
			Providers:          overview.Providers,
			ProviderCatalog:    overview.ProviderCatalog,
			ProvidersError:     overview.ProvidersError,
			CreateDefault:      overview.CreateDefault,
		})
	case http.MethodPost:
		var req managerWebDefaultUpdate
		if err := decodeJSONBody(r, &req); err != nil {
			writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := SetManagerDefault(req.Field, req.Value); err != nil {
			writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
			return
		}
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":               true,
			"message":          "updated default",
			"defaults":         overview.Defaults,
			"command":          overview.Command,
			"agents":           overview.Agents,
			"providers":        overview.Providers,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
	default:
		writeManagerWebOverviewError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *managerWebServer) handleProviders(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		registry, err := LoadProviderRegistryWithError()
		if err != nil {
			writeManagerWebOverviewError(w, http.StatusInternalServerError, err.Error())
			return
		}
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusOK, map[string]any{
			"providers":        registry.Providers,
			"command":          overview.Command,
			"defaults":         overview.Defaults,
			"agents":           overview.Agents,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
	case http.MethodPost:
		var record ProviderRecord
		if err := decodeJSONBody(r, &record); err != nil {
			writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := validateProviderCatalogSelection(record); err != nil {
			writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := UpsertProviderRecord(record); err != nil {
			writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
			return
		}
		saved, _ := FindProviderRecord(record.ProviderID)
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":               true,
			"message":          "saved provider",
			"provider":         saved,
			"providers":        overview.Providers,
			"command":          overview.Command,
			"defaults":         overview.Defaults,
			"agents":           overview.Agents,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
	default:
		writeManagerWebOverviewError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *managerWebServer) handleProviderRoute(w http.ResponseWriter, r *http.Request) {
	providerID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/providers/"), "/")
	if providerID == "" {
		writeManagerWebOverviewError(w, http.StatusNotFound, "provider id is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		record, ok := FindProviderRecord(providerID)
		if !ok {
			writeManagerWebOverviewError(w, http.StatusNotFound, fmt.Sprintf("unknown provider %q", providerID))
			return
		}
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusOK, map[string]any{
			"provider":         record,
			"command":          overview.Command,
			"defaults":         overview.Defaults,
			"agents":           overview.Agents,
			"providers":        overview.Providers,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
	case http.MethodDelete:
		if err := RemoveProviderRecord(providerID); err != nil {
			writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
			return
		}
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":               true,
			"message":          fmt.Sprintf("removed provider %s", providerID),
			"providers":        overview.Providers,
			"command":          overview.Command,
			"defaults":         overview.Defaults,
			"agents":           overview.Agents,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
	default:
		writeManagerWebOverviewError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *managerWebServer) handleOnboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeManagerWebOverviewError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req managerWebOnboardRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateProviderReference(req.ProviderID); err != nil {
		writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
		return
	}

	workdir, err := s.resolveWebOnboardWorkdir(req)
	if err != nil {
		writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
		return
	}
	workdirExisted := false
	if info, statErr := os.Stat(workdir); statErr == nil && info.IsDir() {
		workdirExisted = true
	}
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		writeManagerWebOverviewError(w, http.StatusBadRequest, fmt.Sprintf("create workdir: %v", err))
		return
	}

	globalProfile := LoadRhizomeProfile()
	registry := LoadBotRegistry()
	localRuntime := LoadLocalRuntimeProfile(workdir)
	agentProfile := LoadAgentProfile(workdir)
	state := buildOnboardState(workdir, registry.Defaults, globalProfile, localRuntime, agentProfile)
	applyWebOnboardRequest(&state, req)

	registeredCfg, err := persistAndRegisterOnboardState(state)
	if err != nil {
		_ = recoverWebOnboardingFailure(workdir, workdirExisted, err)
		writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
		return
	}
	_ = clearWebOnboardingMarker(workdir)
	record := managedAgentRecordFromConfig(registeredCfg)
	localRuntime = LoadLocalRuntimeProfile(workdir)
	live := loadManagerLiveRuntimeStatus(r.Context(), record)
	catalog := loadManagerWorkspaceCatalog(r.Context(), record)
	overview := managerWebCurrentOverview()

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"message":            "agent registered; confirmed executor identity is now current",
		"record":             record,
		"local_runtime":      redactLocalRuntimeProfile(localRuntime),
		"effective_identity": managerWebEffectiveIdentityForRecord(record, localRuntime),
		"live":               live,
		"catalog":            catalog,
		"process":            InspectManagedAgentProcess(record),
		"command":            overview.Command,
		"defaults":           overview.Defaults,
		"agents":             overview.Agents,
		"providers":          overview.Providers,
		"provider_catalog":   overview.ProviderCatalog,
		"providers_error":    overview.ProvidersError,
		"create_default":     overview.CreateDefault,
	})
}

func (s *managerWebServer) resolveWebOnboardWorkdir(req managerWebOnboardRequest) (string, error) {
	if strings.TrimSpace(req.Workdir) != "" {
		return validateManagedAgentWorkdirTarget(strings.TrimSpace(req.Workdir))
	}
	parentDir := strings.TrimSpace(req.ParentDir)
	folderName := strings.TrimSpace(req.FolderName)
	if parentDir == "" || folderName == "" {
		return "", fmt.Errorf("parent_dir and folder_name are required")
	}
	return resolveManagerCreateWorkdir(parentDir, folderName)
}

func applyWebOnboardRequest(state *onboardState, req managerWebOnboardRequest) {
	if state == nil {
		return
	}
	state.Runtime.Workdir = firstNonEmpty(strings.TrimSpace(state.Runtime.Workdir))
	state.Runtime.AgentID = firstNonEmpty(strings.TrimSpace(req.AgentID), state.Runtime.AgentID)
	state.Runtime.DisplayName = firstNonEmpty(strings.TrimSpace(req.DisplayName), state.Runtime.DisplayName, humanizeAgentID(state.Runtime.AgentID))
	state.Runtime.OwnerUserID = firstNonEmpty(strings.TrimSpace(req.OwnerUserID), state.Runtime.OwnerUserID)
	state.Runtime.ProviderID = firstNonEmpty(strings.TrimSpace(req.ProviderID), state.Runtime.ProviderID)
	state.Runtime.ModelOverride = firstNonEmpty(strings.TrimSpace(req.Model), state.Runtime.ModelOverride)
	state.Runtime.GroupID = firstNonEmpty(strings.TrimSpace(req.GroupID), state.Runtime.GroupID)
	state.Runtime.RhizomeHost = firstNonEmpty(strings.TrimSpace(req.HostURL), state.Runtime.RhizomeHost)
	state.Runtime.WorkspaceID = firstNonEmpty(strings.TrimSpace(req.WorkspaceID), state.Runtime.WorkspaceID)
	state.Runtime.WorkspacePassword = firstNonEmpty(strings.TrimSpace(req.WorkspacePassword), state.Runtime.WorkspacePassword)
	state.Runtime.Role = firstNonEmpty(strings.TrimSpace(req.Role), state.Runtime.Role)
	state.Runtime.LLMBackend = firstNonEmpty(strings.TrimSpace(req.LLMBackend), state.Runtime.LLMBackend)
	state.Runtime.Model = firstNonEmpty(strings.TrimSpace(req.Model), state.Runtime.Model)
	state.Runtime.GroupID, state.Runtime.LLMBackend, state.Runtime.Model = applyProviderBinding(state.Runtime.ProviderID, state.Runtime.ModelOverride, state.Runtime.GroupID, state.Runtime.LLMBackend, state.Runtime.Model)
	state.Runtime.RhizomeRPC = defaultRPCEndpoint(state.Runtime.RhizomeHost)
	state.Runtime.Mode = RuntimeModeDaemon
	state.Runtime.ApplyDefaults()

	state.AgentProfile.AgentID = state.Runtime.AgentID
	state.AgentProfile.DisplayName = state.Runtime.DisplayName
	state.AgentProfile.GroupID = state.Runtime.GroupID
	state.AgentProfile.Role = state.Runtime.Role
	state.AgentProfile.PrimarySpecialization = firstNonEmpty(strings.TrimSpace(req.PrimarySpecialization), state.AgentProfile.PrimarySpecialization, state.Runtime.Role)
	if strings.TrimSpace(req.SecondarySpecializations) != "" {
		state.AgentProfile.SecondarySpecializations = uniqueTrimmedCSVStrings([]string{req.SecondarySpecializations})
	}
	if strings.TrimSpace(req.DomainScope) != "" {
		state.AgentProfile.DomainScope = uniqueTrimmedCSVStrings([]string{req.DomainScope})
	}
	if strings.TrimSpace(req.Mission) != "" {
		state.AgentProfile.Mission = strings.TrimSpace(req.Mission)
	}
	state.AgentProfile = normalizeAgentProfile(state.AgentProfile)
}

func (s *managerWebServer) handleAgentRoute(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/agents/")
	path = strings.Trim(path, "/")
	if path == "" {
		writeManagerWebOverviewError(w, http.StatusNotFound, "agent id is required")
		return
	}
	parts := strings.Split(path, "/")
	agentID := strings.TrimSpace(parts[0])
	record, err := ResolveManagedAgentReference(agentID)
	if err != nil {
		writeManagerWebOverviewError(w, http.StatusNotFound, err.Error())
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			writeJSONManagedAgentStateError(r.Context(), w, http.StatusMethodNotAllowed, record, "method not allowed")
			return
		}
		s.handleAgentDetail(w, r, record)
		return
	}

	switch parts[1] {
	case "process":
		s.handleAgentProcess(w, r, record)
	case "control":
		s.handleAgentControl(w, r, record)
	case "edit":
		s.handleAgentEdit(w, r, record)
	case "settings":
		s.handleAgentSettings(w, r, record)
	case "messages":
		s.handleAgentMessages(w, r, record)
	case "activity":
		s.handleAgentActivity(w, r, record)
	case "local_chats":
		if len(parts) == 2 {
			if r.Method == http.MethodGet {
				handleListLocalChats(w, r, record)
			} else if r.Method == http.MethodPost {
				handleCreateLocalChat(w, r, record)
			} else {
				writeLocalChatRouteError(w, http.StatusMethodNotAllowed, "method not allowed", record)
			}
		} else if len(parts) == 3 {
			if r.Method == http.MethodGet {
				handleGetLocalChat(w, r, record, parts[2])
			} else if r.Method == http.MethodDelete {
				handleDeleteLocalChat(w, r, record, parts[2])
			} else {
				writeLocalChatRouteError(w, http.StatusMethodNotAllowed, "method not allowed", record)
			}
		} else if len(parts) == 4 && parts[3] == "archive" && r.Method == http.MethodPost {
			handleArchiveLocalChat(w, r, record, parts[2])
		} else if len(parts) == 4 && parts[3] == "message" && r.Method == http.MethodPost {
			handleSendLocalChatMessage(w, r, record)
		} else {
			writeLocalChatRouteError(w, http.StatusNotFound, "unknown local_chat route", record)
		}
	default:
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusNotFound, record, "unknown agent route")
	}
}

func (s *managerWebServer) handleAgentDetail(w http.ResponseWriter, r *http.Request, record ManagedAgentRecord) {
	lines := managerLogTailLines
	if raw := strings.TrimSpace(r.URL.Query().Get("lines")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			lines = parsed
		}
	}
	localRuntime := LoadLocalRuntimeProfile(record.Workdir)
	detail := managerWebAgentDetail{
		Record:             normalizeManagedAgentRecord(record),
		Process:            InspectManagedAgentProcess(record),
		LocalRuntime:       redactLocalRuntimeProfile(localRuntime),
		EffectiveIdentity:  managerWebEffectiveIdentityForRecord(record, localRuntime),
		LocalChatContract:  localChatContractForRecord(record),
		Profile:            LoadAgentProfile(record.Workdir),
		Live:               loadManagerLiveRuntimeStatus(r.Context(), record),
		Catalog:            loadManagerWorkspaceCatalog(r.Context(), record),
		managerWebOverview: managerWebCurrentOverview(),
	}
	if tail, err := TailManagedAgentLogs(record, lines); err == nil {
		detail.Logs = tail
	} else {
		detail.Logs.Error = err.Error()
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *managerWebServer) handleAgentProcess(w http.ResponseWriter, r *http.Request, record ManagedAgentRecord) {
	if r.Method != http.MethodPost {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusMethodNotAllowed, record, "method not allowed")
		return
	}
	var req managerWebActionRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
		return
	}

	action := strings.ToLower(strings.TrimSpace(req.Action))
	preflightOptions := managerWebActionRequestPreflightOptions(req)
	var msg string
	var warnings []string
	var startedState AgentProcessState
	started := false
	switch action {
	case "start":
		state, err := StartManagedAgentWithOptions(record, preflightOptions)
		if err != nil {
			live := loadManagerLiveRuntimeStatusWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout)
			catalog := loadManagerWorkspaceCatalogWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout)
			status := http.StatusBadRequest
			if isManagedRunPreflightBlockedError(err) {
				status = http.StatusConflict
			}
			writeJSONProcessActionError(w, status, record, live, catalog, err.Error())
			return
		}
		startedState = state
		started = true
		msg = fmt.Sprintf("started %s pid=%d", record.AgentID, state.PID)
	case "stop":
		err := StopManagedAgent(record)
		if err != nil {
			live := loadManagerLiveRuntimeStatusWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout)
			catalog := loadManagerWorkspaceCatalogWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout)
			writeJSONProcessActionError(w, http.StatusBadRequest, record, live, catalog, err.Error())
			return
		}
		msg = fmt.Sprintf("stopped %s", record.AgentID)
		cleanupCtx, cancel := managerWebStopCleanupContext(false)
		cleanup, err := closeManagedAgentActiveSessionsAfterStop(cleanupCtx, record)
		cancel()
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("session cleanup skipped: %v", err))
		} else {
			warnings = append(warnings, cleanup.Warnings...)
			if cleanup.SessionsEnded > 0 || cleanup.TaskClaimsReleased > 0 || cleanup.ExecutionRunsCancelled > 0 || cleanup.ExecutionStepsCancelled > 0 || cleanup.BudgetReservationsReleased > 0 {
				msg = formatManagedAgentStopCleanupMessage(record.AgentID, cleanup)
			}
		}
	case "restart":
		state, err := RestartManagedAgentWithOptions(record, preflightOptions)
		if err != nil {
			live := loadManagerLiveRuntimeStatusWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout)
			catalog := loadManagerWorkspaceCatalogWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout)
			status := http.StatusBadRequest
			if isManagedRunPreflightBlockedError(err) {
				status = http.StatusConflict
			}
			writeJSONProcessActionError(w, status, record, live, catalog, err.Error())
			return
		}
		startedState = state
		started = true
		msg = fmt.Sprintf("restarted %s pid=%d", record.AgentID, state.PID)
	default:
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, "unsupported process action")
		return
	}

	if req.SkipOverview {
		process := InspectManagedAgentProcess(record)
		if started {
			process = ManagedAgentProcessStatusFromStartedState(record, startedState)
		}
		payload := map[string]any{
			"ok":      true,
			"message": msg,
			"process": process,
		}
		if len(warnings) > 0 {
			payload["warnings"] = warnings
		}
		writeJSON(w, http.StatusOK, payload)
		return
	}

	overview := managerWebCurrentOverview()
	payload := map[string]any{
		"ok":               true,
		"message":          msg,
		"live":             loadManagerLiveRuntimeStatusWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout),
		"catalog":          loadManagerWorkspaceCatalogWithTimeout(r.Context(), record, managerWebProcessActionRefreshTimeout),
		"process":          InspectManagedAgentProcess(record),
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	}
	if len(warnings) > 0 {
		payload["warnings"] = warnings
	}
	writeJSON(w, http.StatusOK, payload)
}

func (s *managerWebServer) handleAgentsBulkProcess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeManagerWebOverviewError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req managerWebBulkProcessRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeManagerWebOverviewError(w, http.StatusBadRequest, err.Error())
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action != "start" && action != "stop" && action != "restart" {
		writeManagerWebOverviewError(w, http.StatusBadRequest, "unsupported process action")
		return
	}
	targets, results := resolveManagerWebBulkProcessRecords(req.AgentIDs)
	parallelism := req.Parallelism
	if parallelism <= 0 {
		parallelism = 8
	}
	if parallelism > 16 {
		parallelism = 16
	}
	if parallelism > len(targets) && len(targets) > 0 {
		parallelism = len(targets)
	}

	if len(targets) > 0 {
		preflightOptions := managerWebBulkProcessRequestPreflightOptions(req)
		if action == "start" {
			// CT-13: roster start is atomic. Admit (preflight) every target before
			// starting any process so a partially-started roster can never register
			// remote coordination state before the whole roster is admitted.
			runManagerWebRosterStart(targets, results, preflightOptions)
		} else {
			jobs := make(chan managerWebBulkProcessTarget)
			var wg sync.WaitGroup
			for i := 0; i < parallelism; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for item := range jobs {
						results[item.Index] = runManagerWebProcessActionWithOptions(r.Context(), item.Record, action, true, preflightOptions)
					}
				}()
			}
			for _, target := range targets {
				jobs <- target
			}
			close(jobs)
			wg.Wait()
		}
	}

	okCount := 0
	for _, result := range results {
		if result.OK {
			okCount++
		}
	}
	if req.SkipOverview {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":          okCount == len(results),
			"action":      action,
			"total":       len(results),
			"ok_count":    okCount,
			"error_count": len(results) - okCount,
			"results":     results,
		})
		return
	}
	overview := managerWebCurrentOverview()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":               okCount == len(results),
		"action":           action,
		"total":            len(results),
		"ok_count":         okCount,
		"error_count":      len(results) - okCount,
		"results":          results,
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	})
}

func resolveManagerWebBulkProcessRecords(agentIDs []string) ([]managerWebBulkProcessTarget, []managerWebBulkProcessAgentResult) {
	registry := LoadBotRegistry()
	rawIDs := make([]string, 0, len(agentIDs))
	if len(agentIDs) == 0 {
		for _, record := range registry.Agents {
			rawIDs = append(rawIDs, record.AgentID)
		}
	} else {
		rawIDs = append(rawIDs, agentIDs...)
	}
	targets := make([]managerWebBulkProcessTarget, 0, len(rawIDs))
	results := []managerWebBulkProcessAgentResult{}
	seen := map[string]struct{}{}
	for _, rawID := range rawIDs {
		id := strings.TrimSpace(rawID)
		if id == "" {
			continue
		}
		key := strings.ToLower(id)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		index := len(results)
		results = append(results, managerWebBulkProcessAgentResult{AgentID: id})
		record, err := ResolveManagedAgentReference(id)
		if err != nil {
			results[index].OK = false
			results[index].Error = err.Error()
			continue
		}
		targets = append(targets, managerWebBulkProcessTarget{Index: index, Record: record})
	}
	return targets, results
}

func runManagerWebProcessAction(ctx context.Context, record ManagedAgentRecord, action string, boundedCleanup bool) managerWebBulkProcessAgentResult {
	return runManagerWebProcessActionWithOptions(ctx, record, action, boundedCleanup, managedRunPreflightOptions{})
}

func runManagerWebProcessActionWithOptions(ctx context.Context, record ManagedAgentRecord, action string, boundedCleanup bool, preflightOptions managedRunPreflightOptions) managerWebBulkProcessAgentResult {
	record = normalizeManagedAgentRecord(record)
	result := managerWebBulkProcessAgentResult{AgentID: record.AgentID}
	var err error
	var process ManagedAgentProcessStatus
	switch action {
	case "start":
		state, startErr := StartManagedAgentWithOptions(record, preflightOptions)
		if startErr != nil {
			err = startErr
		} else {
			result.Message = fmt.Sprintf("started %s pid=%d", record.AgentID, state.PID)
			process = ManagedAgentProcessStatusFromStartedState(record, state)
		}
	case "stop":
		if stopErr := StopManagedAgent(record); stopErr != nil {
			err = stopErr
		} else {
			result.Message = fmt.Sprintf("stopped %s", record.AgentID)
			cleanupCtx, cancel := managerWebStopCleanupContext(boundedCleanup)
			cleanup, cleanupErr := closeManagedAgentActiveSessionsAfterStop(cleanupCtx, record)
			cancel()
			if cleanupErr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("session cleanup skipped: %v", cleanupErr))
				if !managerWebStopCleanupUnavailableError(cleanupErr) {
					err = fmt.Errorf("stopped %s but session cleanup failed: %w", record.AgentID, cleanupErr)
				}
			} else {
				result.Warnings = append(result.Warnings, cleanup.Warnings...)
				if cleanup.SessionsEnded > 0 || cleanup.TaskClaimsReleased > 0 || cleanup.ExecutionRunsCancelled > 0 || cleanup.ExecutionStepsCancelled > 0 || cleanup.BudgetReservationsReleased > 0 {
					result.Message = formatManagedAgentStopCleanupMessage(record.AgentID, cleanup)
				}
				if len(cleanup.Warnings) > 0 {
					err = fmt.Errorf("stopped %s but session cleanup was incomplete: %s", record.AgentID, strings.Join(cleanup.Warnings, "; "))
				}
			}
		}
	case "restart":
		state, restartErr := RestartManagedAgentWithOptions(record, preflightOptions)
		if restartErr != nil {
			err = restartErr
		} else {
			result.Message = fmt.Sprintf("restarted %s pid=%d", record.AgentID, state.PID)
			process = ManagedAgentProcessStatusFromStartedState(record, state)
		}
	default:
		err = fmt.Errorf("unsupported process action")
	}
	if strings.TrimSpace(process.State) == "" {
		process = InspectManagedAgentProcess(record)
	}
	result.Process = process
	if err != nil {
		result.OK = false
		result.Error = err.Error()
		return result
	}
	result.OK = true
	return result
}

func managerWebActionRequestPreflightOptions(req managerWebActionRequest) managedRunPreflightOptions {
	return managedRunPreflightOptions{
		RepositoryDirtyPathAllowlist:    req.RepositoryDirtyPathAllowlist,
		ResumeContinuationWaiver:        managerWebResumeContinuationWaiver(req.ResumeContinuationWaiver, req.AllowResumeDirtyProjectCheckout, req.AllowResumeLivePatchQueue, req.AllowResumeAgentRequests, req.AllowResumePendingTriggers),
		AllowResumeDirtyProjectCheckout: req.AllowResumeDirtyProjectCheckout,
		AllowResumeLivePatchQueue:       req.AllowResumeLivePatchQueue,
		AllowResumeAgentRequests:        req.AllowResumeAgentRequests,
		AllowResumePendingTriggers:      req.AllowResumePendingTriggers,
	}
}

func managerWebBulkProcessRequestPreflightOptions(req managerWebBulkProcessRequest) managedRunPreflightOptions {
	return managedRunPreflightOptions{
		RepositoryDirtyPathAllowlist:    req.RepositoryDirtyPathAllowlist,
		ResumeContinuationWaiver:        managerWebResumeContinuationWaiver(req.ResumeContinuationWaiver, req.AllowResumeDirtyProjectCheckout, req.AllowResumeLivePatchQueue, req.AllowResumeAgentRequests, req.AllowResumePendingTriggers),
		AllowResumeDirtyProjectCheckout: req.AllowResumeDirtyProjectCheckout,
		AllowResumeLivePatchQueue:       req.AllowResumeLivePatchQueue,
		AllowResumeAgentRequests:        req.AllowResumeAgentRequests,
		AllowResumePendingTriggers:      req.AllowResumePendingTriggers,
	}
}

func managerWebResumeContinuationWaiver(waiver managedRunResumeContinuationWaiver, allowDirtyProjectCheckout, allowLivePatchQueue, allowAgentRequests, allowPendingResumeTriggers bool) managedRunResumeContinuationWaiver {
	if allowDirtyProjectCheckout {
		waiver.AllowDirtyProjectCheckout = true
	}
	if allowLivePatchQueue {
		waiver.AllowLivePatchQueue = true
	}
	if allowAgentRequests {
		waiver.AllowAgentRequests = true
	}
	if allowPendingResumeTriggers {
		waiver.AllowPendingResumeTriggers = true
	}
	return waiver
}

func managerWebStopCleanupContext(bounded bool) (context.Context, context.CancelFunc) {
	if bounded {
		return context.WithTimeout(context.Background(), managerWebBulkProcessCleanupTimeout)
	}
	return context.Background(), func() {}
}

func managerWebStopCleanupUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"workspace id is required",
		"agent id is required",
		"rpc endpoint is required",
		"local agent token is required",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// runManagerWebRosterStart implements CT-13: atomic roster admission. Every target's
// managed-run preflight (admission) is computed first; only if ALL targets are admitted
// does any agent process start. Splitting admit (read-only preflight) from start (process
// spawn that registers remote checkouts/branches/sessions) guarantees that no partial
// roster registers remote coordination state before the whole roster has been admitted.
func runManagerWebRosterStart(targets []managerWebBulkProcessTarget, results []managerWebBulkProcessAgentResult, options managedRunPreflightOptions) {
	if len(targets) == 0 {
		return
	}
	type rosterAdmission struct {
		preflight managedRunPreflightResult
		err       error
	}
	admissions := make([]rosterAdmission, len(targets))
	var admitWG sync.WaitGroup
	for i := range targets {
		admitWG.Add(1)
		go func(i int) {
			defer admitWG.Done()
			record := normalizeManagedAgentRecord(targets[i].Record)
			var pf managedRunPreflightResult
			var err error
			if options.hasOverrides() {
				pf, err = admitManagedRunStartWithOptionsFunc(record, options)
			} else {
				pf, err = admitManagedRunStartFunc(record)
			}
			admissions[i] = rosterAdmission{preflight: pf, err: err}
		}(i)
	}
	admitWG.Wait()

	var blockedAgents []string
	for i := range targets {
		if admissions[i].err != nil {
			blockedAgents = append(blockedAgents, normalizeManagedAgentRecord(targets[i].Record).AgentID)
		}
	}
	if len(blockedAgents) > 0 {
		abortReason := fmt.Sprintf("roster admission aborted: %s blocked (atomic roster start)", strings.Join(blockedAgents, ","))
		for i := range targets {
			idx := targets[i].Index
			record := normalizeManagedAgentRecord(targets[i].Record)
			result := managerWebBulkProcessAgentResult{AgentID: record.AgentID, OK: false}
			if admissions[i].err != nil {
				result.Error = admissions[i].err.Error()
			} else {
				result.Error = abortReason
			}
			result.Process = InspectManagedAgentProcess(record)
			results[idx] = result
		}
		return
	}

	var startWG sync.WaitGroup
	for i := range targets {
		startWG.Add(1)
		go func(i int) {
			defer startWG.Done()
			idx := targets[i].Index
			record := normalizeManagedAgentRecord(targets[i].Record)
			result := managerWebBulkProcessAgentResult{AgentID: record.AgentID}
			state, startErr := startManagedAgent(record, admissions[i].preflight.ChildExecutablePath)
			if startErr != nil {
				result.OK = false
				result.Error = startErr.Error()
				result.Process = InspectManagedAgentProcess(record)
			} else {
				result.OK = true
				result.Message = fmt.Sprintf("started %s pid=%d", record.AgentID, state.PID)
				result.Process = ManagedAgentProcessStatusFromStartedState(record, state)
			}
			results[idx] = result
		}(i)
	}
	startWG.Wait()
}

func loadManagerLiveRuntimeStatusWithTimeout(ctx context.Context, record ManagedAgentRecord, timeout time.Duration) managerLiveRuntimeStatus {
	if timeout <= 0 {
		return loadManagerLiveRuntimeStatus(ctx, record)
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return loadManagerLiveRuntimeStatus(queryCtx, record)
}

func loadManagerWorkspaceCatalogWithTimeout(ctx context.Context, record ManagedAgentRecord, timeout time.Duration) managerWorkspaceCatalog {
	if timeout <= 0 {
		return loadManagerWorkspaceCatalog(ctx, record)
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return loadManagerWorkspaceCatalog(queryCtx, record)
}

type managedAgentStopCleanupResult struct {
	SessionsEnded              int
	TaskClaimsReleased         int
	ExecutionRunsCancelled     int64
	ExecutionStepsCancelled    int64
	BudgetReservationsReleased int
	ScratchStatesCleared       int
	ProjectCheckoutsAbandoned  int
	Warnings                   []string
}

func formatManagedAgentStopCleanupMessage(agentID string, cleanup managedAgentStopCleanupResult) string {
	parts := []string{fmt.Sprintf("stopped %s", agentID)}
	if cleanup.SessionsEnded > 0 {
		parts = append(parts, fmt.Sprintf("ended %d active session(s)", cleanup.SessionsEnded))
	}
	if cleanup.TaskClaimsReleased > 0 {
		parts = append(parts, fmt.Sprintf("released %d task claim(s)", cleanup.TaskClaimsReleased))
	}
	if cleanup.ExecutionRunsCancelled > 0 {
		parts = append(parts, fmt.Sprintf("cancelled %d execution run(s)", cleanup.ExecutionRunsCancelled))
	}
	if cleanup.ExecutionStepsCancelled > 0 {
		parts = append(parts, fmt.Sprintf("cancelled %d execution step(s)", cleanup.ExecutionStepsCancelled))
	}
	if cleanup.BudgetReservationsReleased > 0 {
		parts = append(parts, fmt.Sprintf("released %d budget reservation(s)", cleanup.BudgetReservationsReleased))
	}
	if cleanup.ScratchStatesCleared > 0 {
		parts = append(parts, fmt.Sprintf("cleared %d runtime scratch state(s)", cleanup.ScratchStatesCleared))
	}
	if cleanup.ProjectCheckoutsAbandoned > 0 {
		parts = append(parts, fmt.Sprintf("abandoned %d active project checkout(s)", cleanup.ProjectCheckoutsAbandoned))
	}
	return strings.Join(parts, "; ")
}

func closeManagedAgentActiveSessionsAfterStop(ctx context.Context, record ManagedAgentRecord) (managedAgentStopCleanupResult, error) {
	record = normalizeManagedAgentRecord(record)
	local := LoadLocalRuntimeProfile(record.Workdir)
	workspaceID := firstNonEmpty(local.effectiveWorkspaceID(), record.WorkspaceID)
	agentID := firstNonEmpty(record.AgentID, local.effectiveAgentID())
	endpoint := firstNonEmpty(local.RPCEndpoint, defaultRPCEndpoint(local.HostURL), defaultRPCEndpoint(record.HostURL))
	token := strings.TrimSpace(local.AgentToken)
	if strings.TrimSpace(workspaceID) == "" {
		return managedAgentStopCleanupResult{}, fmt.Errorf("workspace id is required")
	}
	if strings.TrimSpace(agentID) == "" {
		return managedAgentStopCleanupResult{}, fmt.Errorf("agent id is required")
	}
	if strings.TrimSpace(endpoint) == "" {
		return managedAgentStopCleanupResult{}, fmt.Errorf("rpc endpoint is required")
	}
	if token == "" {
		return managedAgentStopCleanupResult{}, fmt.Errorf("local agent token is required")
	}

	cleanupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	client := NewRhizomeClient(endpoint, token)
	taskLookup := newManagedAgentStopTaskLookup(client)
	keepInactive := false
	result := managedAgentStopCleanupResult{}
	releasedTasks := map[string]struct{}{}
	sessions, err := client.ListSessions(cleanupCtx, workspaceID, true, 100)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("active session listing skipped: %v", err))
		cleanupManagedAgentScratchActiveSessionAfterStop(cleanupCtx, client, taskLookup, workspaceID, agentID, releasedTasks, &result)
	} else {
		for _, session := range sessions {
			cleanupManagedAgentSessionAfterStop(cleanupCtx, client, taskLookup, workspaceID, agentID, session, releasedTasks, keepInactive, &result)
		}
	}
	cleanupManagedAgentClaimOnlyTasksAfterStop(cleanupCtx, client, taskLookup, workspaceID, agentID, releasedTasks, &result)
	cleanupManagedAgentExecutionRunsAfterStop(cleanupCtx, client, workspaceID, agentID, &result)
	cleanupManagedAgentOpenBudgetReservationsAfterStop(cleanupCtx, client, local, workspaceID, agentID, &result)
	cleanupManagedAgentRuntimeScratchAfterStop(cleanupCtx, client, workspaceID, agentID, &result)
	cleanupManagedAgentProjectCheckoutsAfterStop(cleanupCtx, client, workspaceID, agentID, &result)
	return result, nil
}

func cleanupManagedAgentScratchActiveSessionAfterStop(ctx context.Context, client *RhizomeClient, lookup *managedAgentStopTaskLookup, workspaceID, agentID string, releasedTasks map[string]struct{}, result *managedAgentStopCleanupResult) {
	if client == nil || result == nil {
		return
	}
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || agentID == "" {
		return
	}
	value, ok, err := client.StateGet(ctx, workspaceID, agentID, runtimeScratchStateKey)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("scratch active session cleanup skipped for %s: %v", agentID, err))
		return
	}
	if !ok || strings.TrimSpace(value) == "" {
		return
	}
	var state RuntimeScratchState
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("scratch active session cleanup skipped for %s: %v", agentID, err))
		return
	}
	sessionID := strings.TrimSpace(state.ActiveSessionID)
	taskID := strings.TrimSpace(state.ActiveTaskID)
	if sessionID == "" {
		return
	}
	cleanupManagedAgentSessionAfterStop(ctx, client, lookup, workspaceID, agentID, AgentSessionStateRecord{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		Status:      "ACTIVE",
		Summary:     "active session recovered from runtime scratch after session-list failure",
	}, releasedTasks, false, result)
}

func cleanupManagedAgentSessionAfterStop(ctx context.Context, client *RhizomeClient, lookup *managedAgentStopTaskLookup, workspaceID, agentID string, session AgentSessionStateRecord, releasedTasks map[string]struct{}, keepInactive bool, result *managedAgentStopCleanupResult) {
	if client == nil || result == nil {
		return
	}
	if strings.TrimSpace(session.AgentID) != strings.TrimSpace(agentID) {
		return
	}
	if strings.EqualFold(strings.TrimSpace(session.Status), "ENDED") {
		return
	}
	sessionID := strings.TrimSpace(session.SessionID)
	if sessionID == "" {
		return
	}
	sessionWorkspaceID := firstNonEmpty(session.WorkspaceID, workspaceID)
	taskID := strings.TrimSpace(session.TaskID)
	if taskID != "" {
		releaseKey := sessionWorkspaceID + "\x00" + taskID
		if _, seen := releasedTasks[releaseKey]; !seen {
			releasedTasks[releaseKey] = struct{}{}
			preserveBlockedClaim := strings.EqualFold(strings.TrimSpace(session.Status), "BLOCKED")
			if !preserveBlockedClaim && lookup != nil {
				if task, ok, lookupErr := lookup.Get(ctx, sessionWorkspaceID, taskID); lookupErr == nil && ok {
					preserveBlockedClaim = managedAgentStopPreserveBlockedTaskClaim(task, agentID)
				}
			}
			if !preserveBlockedClaim {
				if err := client.ReleaseTask(ctx, TaskReleaseInput{
					WorkspaceID: sessionWorkspaceID,
					AgentID:     session.AgentID,
					TaskID:      taskID,
					Reason:      fmt.Sprintf("Released by rhizome-bot web stop for agent %s", agentID),
				}); err != nil {
					if managedAgentStopTaskReleaseNoOp(err) {
						result.TaskClaimsReleased++
					} else {
						result.Warnings = append(result.Warnings, fmt.Sprintf("task claim cleanup skipped for %s: %v", taskID, err))
					}
				} else {
					result.TaskClaimsReleased++
				}
			}
		}
	}
	summary := fmt.Sprintf("Ended by rhizome-bot web stop for agent %s", agentID)
	if previous := strings.TrimSpace(session.Summary); previous != "" {
		summary += "; previous: " + previous
	}
	if _, err := client.SessionEvent(ctx, "agent.session.end", SessionEventInput{
		WorkspaceID:        sessionWorkspaceID,
		SessionID:          sessionID,
		AgentID:            session.AgentID,
		TaskID:             taskID,
		Summary:            summary,
		Status:             "ENDED",
		KeepSessionActive:  &keepInactive,
		BlockedOn:          nil,
		DecisionNeededFrom: "",
		DecisionType:       "",
	}); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("active session cleanup skipped for %s: %v", sessionID, err))
		return
	}
	result.SessionsEnded++
}

type managedAgentStopTaskLookup struct {
	client *RhizomeClient
	tasks  map[string]map[string]WorkspaceTaskRecord
	errs   map[string]error
}

func newManagedAgentStopTaskLookup(client *RhizomeClient) *managedAgentStopTaskLookup {
	return &managedAgentStopTaskLookup{
		client: client,
		tasks:  map[string]map[string]WorkspaceTaskRecord{},
		errs:   map[string]error{},
	}
}

func (l *managedAgentStopTaskLookup) Get(ctx context.Context, workspaceID, taskID string) (WorkspaceTaskRecord, bool, error) {
	if l == nil || l.client == nil {
		return WorkspaceTaskRecord{}, false, nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" || taskID == "" {
		return WorkspaceTaskRecord{}, false, nil
	}
	if err, ok := l.errs[workspaceID]; ok {
		return WorkspaceTaskRecord{}, false, err
	}
	if _, ok := l.tasks[workspaceID]; !ok {
		tasks, err := l.client.ListTasks(ctx, workspaceID)
		if err != nil {
			l.errs[workspaceID] = err
			return WorkspaceTaskRecord{}, false, err
		}
		byID := make(map[string]WorkspaceTaskRecord, len(tasks))
		for _, task := range tasks {
			if id := strings.TrimSpace(task.TaskID); id != "" {
				byID[id] = task
			}
		}
		l.tasks[workspaceID] = byID
	}
	task, ok := l.tasks[workspaceID][taskID]
	return task, ok, nil
}

func (l *managedAgentStopTaskLookup) List(ctx context.Context, workspaceID string) ([]WorkspaceTaskRecord, error) {
	if l == nil || l.client == nil {
		return nil, nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, nil
	}
	if err, ok := l.errs[workspaceID]; ok {
		return nil, err
	}
	if _, ok := l.tasks[workspaceID]; !ok {
		tasks, err := l.client.ListTasks(ctx, workspaceID)
		if err != nil {
			l.errs[workspaceID] = err
			return nil, err
		}
		byID := make(map[string]WorkspaceTaskRecord, len(tasks))
		for _, task := range tasks {
			if id := strings.TrimSpace(task.TaskID); id != "" {
				byID[id] = task
			}
		}
		l.tasks[workspaceID] = byID
	}
	tasks := make([]WorkspaceTaskRecord, 0, len(l.tasks[workspaceID]))
	for _, task := range l.tasks[workspaceID] {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func cleanupManagedAgentClaimOnlyTasksAfterStop(ctx context.Context, client *RhizomeClient, lookup *managedAgentStopTaskLookup, workspaceID, agentID string, handledTasks map[string]struct{}, result *managedAgentStopCleanupResult) {
	if client == nil || result == nil || lookup == nil {
		return
	}
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || agentID == "" {
		return
	}
	tasks, err := lookup.List(ctx, workspaceID)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("claim-only task cleanup skipped: %v", err))
		return
	}
	for _, task := range tasks {
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" || strings.TrimSpace(taskPointerString(task.ClaimAgentID)) != agentID {
			continue
		}
		if managedAgentStopPreserveBlockedTaskClaim(task, agentID) {
			continue
		}
		if !taskClaimStatusIsActiveOwnership(taskPointerString(task.ClaimStatus)) {
			continue
		}
		taskWorkspaceID := workspaceID
		releaseKey := taskWorkspaceID + "\x00" + taskID
		if _, seen := handledTasks[releaseKey]; seen {
			continue
		}
		handledTasks[releaseKey] = struct{}{}
		if err := client.ReleaseTask(ctx, TaskReleaseInput{
			WorkspaceID:           taskWorkspaceID,
			AgentID:               agentID,
			TaskID:                taskID,
			Reason:                fmt.Sprintf("Released claim-only task by rhizome-bot web stop for agent %s", agentID),
			SessionTransitionKind: "reclaim_release",
		}); err != nil {
			if managedAgentStopTaskReleaseNoOp(err) {
				result.TaskClaimsReleased++
				continue
			}
			if managedAgentStopClaimReleaseNoOpAfterReadback(ctx, client, taskWorkspaceID, taskID, agentID) {
				continue
			}
			result.Warnings = append(result.Warnings, fmt.Sprintf("claim-only task cleanup skipped for %s: %v", taskID, err))
			continue
		}
		result.TaskClaimsReleased++
	}
}

func managedAgentStopPreserveBlockedTaskClaim(task WorkspaceTaskRecord, agentID string) bool {
	return strings.EqualFold(taskClaimStatus(task), "BLOCKED") &&
		strings.EqualFold(strings.TrimSpace(taskPointerString(task.ClaimAgentID)), strings.TrimSpace(agentID))
}

func managedAgentStopTaskReleaseNoOp(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "task claim transition is stale or duplicate")
}

func managedAgentStopClaimReleaseNoOpAfterReadback(ctx context.Context, client *RhizomeClient, workspaceID, taskID, agentID string) bool {
	if client == nil {
		return false
	}
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || taskID == "" || agentID == "" {
		return false
	}
	tasks, err := client.ListTasks(ctx, workspaceID)
	if err != nil {
		return false
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) != taskID {
			continue
		}
		claimStatus := taskClaimStatus(task)
		claimAgentID := strings.TrimSpace(taskPointerString(task.ClaimAgentID))
		if claimAgentID == "" || !taskClaimStatusIsActiveOwnership(claimStatus) {
			return true
		}
		if claimAgentID != agentID {
			return false
		}
		return !taskClaimStatusIsActiveOwnership(claimStatus)
	}
	return true
}

func cleanupManagedAgentExecutionRunsAfterStop(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, result *managedAgentStopCleanupResult) {
	if client == nil || result == nil {
		return
	}
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || agentID == "" {
		return
	}
	cancelled, err := client.CancelExecutionRunsForAgentStop(ctx, ExecutionAgentRunsCancelInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Summary:     fmt.Sprintf("Cancelled by rhizome-bot web stop for agent %s", agentID),
		Outcome:     "STOPPED_BY_MANAGER",
	})
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("execution run cleanup skipped for %s: %v", agentID, err))
		return
	}
	result.ExecutionRunsCancelled += cancelled.RunsCancelled
	result.ExecutionStepsCancelled += cancelled.StepsCancelled
}

func taskPointerString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func cleanupManagedAgentOpenBudgetReservationsAfterStop(ctx context.Context, client *RhizomeClient, local LocalRuntimeProfile, workspaceID, agentID string, result *managedAgentStopCleanupResult) {
	if result == nil || client == nil || !managedAgentBudgetCleanupConfigured(local) {
		return
	}
	reservations, err := listManagedAgentOpenBudgetReservationsForStop(ctx, client, local, workspaceID, agentID)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("budget reservation cleanup skipped: %v", err))
		return
	}
	for _, reservation := range reservations {
		if strings.TrimSpace(reservation.ReservationID) == "" {
			continue
		}
		if strings.TrimSpace(reservation.WorkspaceID) != strings.TrimSpace(workspaceID) || strings.TrimSpace(reservation.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		remaining := budgetReservationRemainingMicros(reservation)
		if remaining <= 0 {
			continue
		}
		if _, err := client.ReleaseBudget(ctx, BudgetReleaseInput{
			EntryID:        managedAgentBudgetStopReleaseEntryID(reservation),
			IdempotencyKey: managedAgentBudgetStopReleaseIdempotencyKey(reservation),
			AccountID:      reservation.AccountID,
			ReservationID:  reservation.ReservationID,
			WorkspaceID:    reservation.WorkspaceID,
			AgentID:        reservation.AgentID,
			TaskID:         reservation.TaskID,
			RunID:          reservation.RunID,
			ProviderID:     reservation.ProviderID,
			Model:          reservation.Model,
			AmountMicros:   remaining,
			Reason:         "rhizome_bot_web_stop_cleanup",
		}); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("budget reservation cleanup skipped for %s: %v", reservation.ReservationID, err))
			continue
		}
		result.BudgetReservationsReleased++
	}
}

func cleanupManagedAgentRuntimeScratchAfterStop(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, result *managedAgentStopCleanupResult) {
	if client == nil || result == nil {
		return
	}
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || agentID == "" {
		return
	}
	value, ok, err := client.StateGet(ctx, workspaceID, agentID, runtimeScratchStateKey)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime scratch cleanup skipped for %s: %v", agentID, err))
		return
	}
	if !ok || strings.TrimSpace(value) == "" {
		return
	}
	var state RuntimeScratchState
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime scratch cleanup skipped for %s: %v", agentID, err))
		return
	}
	if !managedAgentRuntimeScratchHasStopResidue(state) {
		return
	}
	managedAgentClearRuntimeScratchStopResidue(&state, agentID)
	if err := saveScratchState(ctx, client, workspaceID, agentID, state); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime scratch cleanup skipped for %s: %v", agentID, err))
		return
	}
	result.ScratchStatesCleared++
}

func managedAgentRuntimeScratchHasStopResidue(state RuntimeScratchState) bool {
	return strings.TrimSpace(state.ActiveTaskID) != "" ||
		strings.TrimSpace(state.ActiveSessionID) != "" ||
		strings.TrimSpace(state.ActiveRunID) != "" ||
		strings.TrimSpace(state.PendingTrigger) != "" ||
		strings.TrimSpace(state.PendingTriggerTask) != "" ||
		strings.TrimSpace(state.PendingTriggerSession) != "" ||
		strings.TrimSpace(state.ContinuationHoldTaskID) != "" ||
		strings.TrimSpace(state.ContinuationHoldSessionID) != "" ||
		strings.TrimSpace(state.ContinuationHoldRunID) != "" ||
		strings.TrimSpace(state.NoopContinueTaskID) != "" ||
		strings.TrimSpace(state.NoopContinueSessionID) != "" ||
		strings.TrimSpace(state.NoopContinueRunID) != "" ||
		strings.TrimSpace(state.RequiredTransitionTaskID) != "" ||
		strings.TrimSpace(state.RequiredTransitionSessionID) != "" ||
		strings.TrimSpace(state.RequiredTransitionRunID) != "" ||
		strings.TrimSpace(state.ImmediateProjectResumeTaskID) != "" ||
		strings.TrimSpace(state.ImmediateProjectResumeSessionID) != "" ||
		strings.TrimSpace(state.ImmediateProjectResumeRunID) != ""
}

func managedAgentClearRuntimeScratchStopResidue(state *RuntimeScratchState, agentID string) {
	if state == nil {
		return
	}
	state.ActiveTaskID = ""
	state.ActiveSessionID = ""
	state.ActiveRunID = ""
	clearPendingTriggerFields(state)
	state.ContinuationHoldTaskID = ""
	state.ContinuationHoldSessionID = ""
	state.ContinuationHoldRunID = ""
	state.ContinuationHoldUntil = ""
	state.ContinuationHoldSummary = ""
	state.ContinuationHoldCount = 0
	state.NoopContinueTaskID = ""
	state.NoopContinueSessionID = ""
	state.NoopContinueRunID = ""
	state.NoopContinueSignature = ""
	state.NoopContinueSummary = ""
	state.NoopContinueAt = ""
	state.NoopContinueCount = 0
	state.RequiredTransitionTaskID = ""
	state.RequiredTransitionSessionID = ""
	state.RequiredTransitionRunID = ""
	state.RequiredTransitionTool = ""
	state.RequiredTransitionSummary = ""
	state.RequiredTransitionAt = ""
	state.RequiredTransitionCount = 0
	state.ImmediateProjectResumeTaskID = ""
	state.ImmediateProjectResumeSessionID = ""
	state.ImmediateProjectResumeRunID = ""
	state.ImmediateProjectResumeSignature = ""
	state.ImmediateProjectResumeAt = ""
	state.ImmediateProjectResumeCount = 0
	now := time.Now().UTC().Format(time.RFC3339Nano)
	state.LastWakeReason = "manager_stop_cleanup"
	state.LastWakeSummary = fmt.Sprintf("Runtime scratch active bindings cleared by rhizome-bot web stop for agent %s.", strings.TrimSpace(agentID))
	state.LastWakeAt = now
	state.LastSummary = state.LastWakeSummary
}

func cleanupManagedAgentProjectCheckoutsAfterStop(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, result *managedAgentStopCleanupResult) {
	if client == nil || result == nil {
		return
	}
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || agentID == "" {
		return
	}
	projects, err := client.ListProjects(ctx, workspaceID)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("project checkout cleanup skipped for %s: %v", agentID, err))
		return
	}
	for _, project := range projects {
		projectID := strings.TrimSpace(project.ProjectID)
		if projectID == "" {
			continue
		}
		coordination, err := client.GetProjectCoordination(ctx, workspaceID, projectID)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("project checkout cleanup skipped for %s/%s: %v", agentID, projectID, err))
			continue
		}
		for _, checkout := range coordination.Checkouts {
			if strings.TrimSpace(checkout.AgentID) != agentID || !managedAgentProjectCheckoutNeedsStopAbandon(checkout) {
				continue
			}
			if _, err := client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
				WorkspaceID:   workspaceID,
				ProjectID:     firstNonEmpty(strings.TrimSpace(checkout.ProjectID), projectID),
				ActorID:       agentID,
				CheckoutID:    strings.TrimSpace(checkout.CheckoutID),
				RepoID:        strings.TrimSpace(checkout.RepoID),
				MachineID:     strings.TrimSpace(checkout.MachineID),
				MachineLabel:  strings.TrimSpace(checkout.MachineLabel),
				OwnerUserID:   strings.TrimSpace(checkout.OwnerUserID),
				AgentID:       agentID,
				LocalPath:     strings.TrimSpace(checkout.LocalPath),
				CheckoutKind:  strings.TrimSpace(checkout.CheckoutKind),
				BranchName:    strings.TrimSpace(checkout.BranchName),
				BaseBranch:    strings.TrimSpace(checkout.BaseBranch),
				HeadSHA:       strings.TrimSpace(checkout.HeadSHA),
				BaseSHA:       strings.TrimSpace(checkout.BaseSHA),
				DirtyState:    managedAgentProjectCheckoutStopDirtyState(ctx, checkout),
				ActiveTaskID:  "",
				ActiveClaimID: "",
				Status:        "ABANDONED",
				LastSeenAt:    time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("project checkout cleanup skipped for %s/%s/%s: %v", agentID, projectID, checkout.CheckoutID, err))
				continue
			}
			result.ProjectCheckoutsAbandoned++
		}
	}
}

func managedAgentProjectCheckoutStopDirtyState(ctx context.Context, checkout ProjectCheckoutRecord) string {
	dirtyState := strings.TrimSpace(checkout.DirtyState)
	localPath := strings.TrimSpace(checkout.LocalPath)
	if localPath == "" {
		return dirtyState
	}
	dirty, err := gitWorktreeDirty(ctx, localPath)
	if err != nil {
		return dirtyState
	}
	if dirty {
		return "dirty"
	}
	return "clean"
}

func managedAgentProjectCheckoutNeedsStopAbandon(checkout ProjectCheckoutRecord) bool {
	if strings.TrimSpace(checkout.ActiveTaskID) != "" || strings.TrimSpace(checkout.ActiveClaimID) != "" {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(checkout.Status)) {
	case "ACTIVE", "BLOCKED", "RESERVED":
		return true
	default:
		return false
	}
}

func managedAgentBudgetCleanupConfigured(local LocalRuntimeProfile) bool {
	return strings.TrimSpace(local.BudgetAccountID) != "" || local.BudgetHardLimitMicros > 0 || local.BudgetReserveMicros > 0
}

func managedAgentBudgetAccountIDForStop(local LocalRuntimeProfile, workspaceID, agentID string) string {
	if accountID := strings.TrimSpace(local.BudgetAccountID); accountID != "" {
		return accountID
	}
	return defaultRuntimeBudgetAccountID(workspaceID, agentID)
}

func listManagedAgentOpenBudgetReservationsForStop(ctx context.Context, client *RhizomeClient, local LocalRuntimeProfile, workspaceID, agentID string) ([]BudgetReservationRecord, error) {
	accountID := managedAgentBudgetAccountIDForStop(local, workspaceID, agentID)
	if strings.TrimSpace(accountID) == "" {
		return nil, fmt.Errorf("budget account id is required")
	}
	reservations, err := client.ListBudgetReservations(ctx, BudgetReservationListInput{
		AccountID:   accountID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Status:      "OPEN",
		Limit:       500,
	})
	if err == nil {
		return reservations, nil
	}
	if !strings.Contains(strings.ToLower(err.Error()), "method not found") {
		return nil, err
	}
	return listManagedAgentOpenBudgetReservationsFromLedger(ctx, client, accountID, workspaceID, agentID)
}

func listManagedAgentOpenBudgetReservationsFromLedger(ctx context.Context, client *RhizomeClient, accountID, workspaceID, agentID string) ([]BudgetReservationRecord, error) {
	entries, err := client.ListBudgetLedgerEntries(ctx, BudgetLedgerListInput{
		AccountID:   accountID,
		WorkspaceID: workspaceID,
		Limit:       500,
	})
	if err != nil {
		return nil, err
	}
	groups := map[string]*BudgetReservationRecord{}
	for _, entry := range entries {
		reservationID := strings.TrimSpace(entry.ReservationID)
		if reservationID == "" {
			continue
		}
		reservation := groups[reservationID]
		if reservation == nil {
			reservation = &BudgetReservationRecord{ReservationID: reservationID, AccountID: entry.AccountID}
			groups[reservationID] = reservation
		}
		switch strings.ToUpper(strings.TrimSpace(entry.EntryType)) {
		case "RESERVATION":
			reservation.AccountID = entry.AccountID
			reservation.AmountMicros += entry.AmountMicros
			reservation.WorkspaceID = entry.WorkspaceID
			reservation.AgentID = entry.AgentID
			reservation.TaskID = entry.TaskID
			reservation.RunID = entry.RunID
			reservation.ProviderID = entry.ProviderID
			reservation.Model = entry.Model
			reservation.Reason = entry.Reason
			reservation.CreatedAt = entry.CreatedAt
		case "SPEND":
			reservation.SpentMicros += entry.AmountMicros
		case "RELEASE":
			reservation.ReleasedMicros += entry.AmountMicros
		}
	}
	var reservations []BudgetReservationRecord
	for _, reservation := range groups {
		if strings.TrimSpace(reservation.WorkspaceID) != strings.TrimSpace(workspaceID) || strings.TrimSpace(reservation.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		remaining := budgetReservationRemainingMicros(*reservation)
		if remaining <= 0 {
			continue
		}
		reservation.RemainingMicros = remaining
		reservation.Status = "OPEN"
		reservations = append(reservations, *reservation)
	}
	return reservations, nil
}

func budgetReservationRemainingMicros(reservation BudgetReservationRecord) int64 {
	if reservation.RemainingMicros > 0 {
		return reservation.RemainingMicros
	}
	return reservation.AmountMicros - reservation.SpentMicros - reservation.ReleasedMicros
}

func managedAgentBudgetStopReleaseEntryID(reservation BudgetReservationRecord) string {
	return "release-stop-" + shortHash(reservation.ReservationID)
}

func managedAgentBudgetStopReleaseIdempotencyKey(reservation BudgetReservationRecord) string {
	return "rhizome-bot.stop.release:" + strings.TrimSpace(reservation.ReservationID)
}

func (s *managerWebServer) handleAgentControl(w http.ResponseWriter, r *http.Request, record ManagedAgentRecord) {
	if r.Method != http.MethodPost {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusMethodNotAllowed, record, "method not allowed")
		return
	}
	var req managerWebControlRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
		return
	}
	method := strings.TrimSpace(req.Method)
	if !isAllowedWebControlMethod(method) {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, "unsupported web control method")
		return
	}
	result, err := sendManagedAgentControlRequest(r.Context(), record, method, req.Payload)
	if err != nil {
		live := loadManagerLiveRuntimeStatus(r.Context(), record)
		catalog := loadManagerWorkspaceCatalog(r.Context(), record)
		writeJSONControlActionError(w, http.StatusBadRequest, record, result, live, catalog, err.Error())
		return
	}
	overview := managerWebCurrentOverview()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":               true,
		"message":          fmt.Sprintf("%s -> %s [%s]", record.AgentID, method, firstNonEmpty(result.Status, "submitted")),
		"result":           result,
		"live":             loadManagerLiveRuntimeStatus(r.Context(), record),
		"catalog":          loadManagerWorkspaceCatalog(r.Context(), record),
		"process":          InspectManagedAgentProcess(record),
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	})
}

// req. model for agent settings
type managerWebAgentSettingsRequest struct {
	ProviderID           string `json:"provider_id"`
	LLMBackend           string `json:"llm_backend"`
	Model                string `json:"model"`
	OpenAIApiKeyOverride string `json:"openai_api_key_override"`
	PlannerSec           int    `json:"planner_sec,string"`
	WatchdogSec          int    `json:"watchdog_sec,string"`
}

func (s *managerWebServer) handleAgentSettings(w http.ResponseWriter, r *http.Request, record ManagedAgentRecord) {
	if r.Method != http.MethodPost {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusMethodNotAllowed, record, "method not allowed")
		return
	}
	var req managerWebAgentSettingsRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
		return
	}
	if err := validateProviderReference(req.ProviderID); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
		return
	}
	if strings.TrimSpace(req.OpenAIApiKeyOverride) != "" {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, "provider API keys are not configurable from agent settings yet")
		return
	}

	// Load current profile
	profile := LoadLocalRuntimeProfile(record.Workdir)

	// Update configurable fields
	profile.ProviderID = strings.TrimSpace(req.ProviderID)
	profile.ModelOverride = strings.TrimSpace(req.Model)
	if backend := strings.TrimSpace(req.LLMBackend); backend != "" {
		profile.LLMBackend = backend
	}
	profile.Model = req.Model
	profile.GroupID, profile.LLMBackend, profile.Model = applyProviderBinding(profile.ProviderID, profile.ModelOverride, profile.GroupID, profile.LLMBackend, profile.Model)
	profile.PlannerSec = req.PlannerSec
	profile.WatchdogSec = req.WatchdogSec

	record.ProviderID = profile.ProviderID
	record.ModelOverride = profile.ModelOverride
	record.GroupID = profile.GroupID
	record.LLMBackend = profile.LLMBackend
	record.Model = profile.Model
	if err := SaveManagedAgentRecordAndLocalRuntime(record, profile); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusInternalServerError, record, fmt.Sprintf("failed to persist agent state: %v", err))
		return
	}
	live := loadManagerLiveRuntimeStatus(r.Context(), record)
	catalog := loadManagerWorkspaceCatalog(r.Context(), record)
	overview := managerWebCurrentOverview()

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"message":            "saved local runtime config; restart the agent if provider/model changes should take effect immediately",
		"record":             normalizeManagedAgentRecord(record),
		"local_runtime":      redactLocalRuntimeProfile(profile),
		"effective_identity": managerWebEffectiveIdentityForRecord(record, profile),
		"live":               live,
		"catalog":            catalog,
		"process":            InspectManagedAgentProcess(record),
		"command":            overview.Command,
		"defaults":           overview.Defaults,
		"agents":             overview.Agents,
		"providers":          overview.Providers,
		"provider_catalog":   overview.ProviderCatalog,
		"providers_error":    overview.ProvidersError,
		"create_default":     overview.CreateDefault,
	})
}

func (s *managerWebServer) handleAgentEdit(w http.ResponseWriter, r *http.Request, record ManagedAgentRecord) {
	if r.Method != http.MethodPost {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusMethodNotAllowed, record, "method not allowed")
		return
	}
	var req managerWebAgentEditRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
		return
	}
	status := InspectManagedAgentProcess(record)
	if req.Remove {
		if status.Running {
			live := loadManagerLiveRuntimeStatus(r.Context(), record)
			catalog := loadManagerWorkspaceCatalog(r.Context(), record)
			writeJSONProcessActionError(w, http.StatusBadRequest, record, live, catalog, fmt.Sprintf("stop %s before removing it", record.AgentID))
			return
		}
		if err := RemoveManagedAgent(record.AgentID); err != nil {
			writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
			return
		}
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":               true,
			"removed":          true,
			"message":          fmt.Sprintf("removed %s from registry", record.AgentID),
			"command":          overview.Command,
			"defaults":         overview.Defaults,
			"agents":           overview.Agents,
			"providers":        overview.Providers,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
		return
	}
	if status.Running {
		live := loadManagerLiveRuntimeStatus(r.Context(), record)
		catalog := loadManagerWorkspaceCatalog(r.Context(), record)
		writeJSONProcessActionError(w, http.StatusBadRequest, record, live, catalog, fmt.Sprintf("stop %s before editing registry fields", record.AgentID))
		return
	}

	if req.Workdir != nil && strings.TrimSpace(*req.Workdir) != "" {
		newWorkdir, _, err := moveManagedAgentWorkdir(record.Workdir, *req.Workdir)
		if err != nil {
			writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
			return
		}
		record.Workdir = newWorkdir
	}

	if req.DisplayName != nil {
		record.DisplayName = strings.TrimSpace(*req.DisplayName)
	}
	if req.GroupID != nil {
		record.GroupID = strings.TrimSpace(*req.GroupID)
	}
	if req.Role != nil {
		record.Role = strings.TrimSpace(*req.Role)
	}

	localRuntime := LoadLocalRuntimeProfile(record.Workdir)
	if req.DisplayName != nil {
		localRuntime.DisplayName = record.DisplayName
	}
	if err := SaveLocalRuntimeProfile(record.Workdir, localRuntime); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, fmt.Sprintf("save local runtime profile: %v", err))
		return
	}

	profile := LoadAgentProfile(record.Workdir)
	profileNeedsSave := false

	if req.DisplayName != nil {
		profile.DisplayName = record.DisplayName
		profileNeedsSave = true
	}
	if req.GroupID != nil {
		profile.GroupID = record.GroupID
		profileNeedsSave = true
	}
	if req.Role != nil {
		profile.Role = record.Role
		profileNeedsSave = true
	}
	if req.Tags != nil {
		profile.SecondarySpecializations = uniqueTrimmedCSVStrings([]string{*req.Tags})
		profileNeedsSave = true
	}
	if req.SoulPrompt != nil {
		profile.Mission = *req.SoulPrompt
		profileNeedsSave = true
	}

	if profileNeedsSave {
		if err := SaveAgentProfile(record.Workdir, profile); err != nil {
			writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, fmt.Sprintf("save agent profile: %v", err))
			return
		}
		if err := WriteAgentIdentityFiles(record.Workdir, profile); err != nil {
			writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, fmt.Sprintf("write agent identity files: %v", err))
			return
		}
	}

	if err := UpsertManagedAgent(record); err != nil {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusBadRequest, record, err.Error())
		return
	}
	live := loadManagerLiveRuntimeStatus(r.Context(), record)
	catalog := loadManagerWorkspaceCatalog(r.Context(), record)
	overview := managerWebCurrentOverview()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"message":            fmt.Sprintf("updated %s bootstrap/profile fields; confirmed executor identity remains unchanged until the next successful registration", record.AgentID),
		"record":             normalizeManagedAgentRecord(record),
		"local_runtime":      redactLocalRuntimeProfile(localRuntime),
		"effective_identity": managerWebEffectiveIdentityForRecord(record, localRuntime),
		"live":               live,
		"catalog":            catalog,
		"process":            InspectManagedAgentProcess(record),
		"command":            overview.Command,
		"defaults":           overview.Defaults,
		"agents":             overview.Agents,
		"providers":          overview.Providers,
		"provider_catalog":   overview.ProviderCatalog,
		"providers_error":    overview.ProvidersError,
		"create_default":     overview.CreateDefault,
	})
}

func (s *managerWebServer) handleAgentMessages(w http.ResponseWriter, r *http.Request, record ManagedAgentRecord) {
	if r.Method != http.MethodGet {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusMethodNotAllowed, record, "method not allowed")
		return
	}
	record = normalizeManagedAgentRecord(record)
	client, err := managedAgentControlClientForRecord(record)
	if err != nil {
		live := loadManagerLiveRuntimeStatus(r.Context(), record)
		catalog := loadManagerWorkspaceCatalog(r.Context(), record)
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok":               false,
			"error":            strings.TrimSpace(err.Error()),
			"live":             live,
			"catalog":          catalog,
			"process":          InspectManagedAgentProcess(record),
			"command":          overview.Command,
			"defaults":         overview.Defaults,
			"agents":           overview.Agents,
			"providers":        overview.Providers,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
		return
	}
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, parseErr := strconv.Atoi(raw); parseErr == nil && parsed > 0 {
			limit = parsed
		}
	}
	channel := strings.TrimSpace(r.URL.Query().Get("channel"))
	messages, err := client.Client.ListWorkspaceMessages(r.Context(), client.WorkspaceID, channel, limit)
	if err != nil {
		live := loadManagerLiveRuntimeStatus(r.Context(), record)
		catalog := loadManagerWorkspaceCatalog(r.Context(), record)
		overview := managerWebCurrentOverview()
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok":               false,
			"error":            strings.TrimSpace(err.Error()),
			"live":             live,
			"catalog":          catalog,
			"process":          InspectManagedAgentProcess(record),
			"command":          overview.Command,
			"defaults":         overview.Defaults,
			"agents":           overview.Agents,
			"providers":        overview.Providers,
			"provider_catalog": overview.ProviderCatalog,
			"providers_error":  overview.ProvidersError,
			"create_default":   overview.CreateDefault,
		})
		return
	}
	// Also fetch workspace agents for display name resolution
	agents, _ := client.Client.ListWorkspaceAgents(r.Context(), client.WorkspaceID)
	agentMap := make(map[string]string)
	for _, a := range agents {
		name := strings.TrimSpace(a.DisplayName)
		if name == "" {
			name = a.AgentID
		}
		agentMap[a.AgentID] = name
	}
	overview := managerWebCurrentOverview()
	writeJSON(w, http.StatusOK, map[string]any{
		"messages":         messages,
		"agent_map":        agentMap,
		"agent_id":         record.AgentID,
		"count":            len(messages),
		"live":             loadManagerLiveRuntimeStatus(r.Context(), record),
		"catalog":          loadManagerWorkspaceCatalog(r.Context(), record),
		"process":          InspectManagedAgentProcess(record),
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	})
}

func (s *managerWebServer) handleAgentActivity(w http.ResponseWriter, r *http.Request, record ManagedAgentRecord) {
	if r.Method != http.MethodGet {
		writeJSONManagedAgentStateError(r.Context(), w, http.StatusMethodNotAllowed, record, "method not allowed")
		return
	}
	record = normalizeManagedAgentRecord(record)
	process := InspectManagedAgentProcess(record)
	live := loadManagerLiveRuntimeStatus(r.Context(), record)
	catalog := loadManagerWorkspaceCatalog(r.Context(), record)
	overview := managerWebCurrentOverview()
	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id":         record.AgentID,
		"process":          process,
		"live":             live,
		"catalog":          catalog,
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	})
}

func decodeJSONBody(r *http.Request, out any) error {
	defer r.Body.Close()
	if out == nil {
		return fmt.Errorf("output target is nil")
	}
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		return fmt.Errorf("decode json body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"ok":    false,
		"error": strings.TrimSpace(message),
	})
}

func writeJSONManagedAgentStateError(ctx context.Context, w http.ResponseWriter, status int, record ManagedAgentRecord, message string) {
	live := loadManagerLiveRuntimeStatus(ctx, record)
	catalog := loadManagerWorkspaceCatalog(ctx, record)
	overview := managerWebCurrentOverview()
	writeJSON(w, status, map[string]any{
		"ok":               false,
		"error":            strings.TrimSpace(message),
		"live":             live,
		"catalog":          catalog,
		"process":          InspectManagedAgentProcess(record),
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	})
}

func writeJSONProcessActionError(
	w http.ResponseWriter,
	status int,
	record ManagedAgentRecord,
	live managerLiveRuntimeStatus,
	catalog managerWorkspaceCatalog,
	message string,
) {
	overview := managerWebCurrentOverview()
	writeJSON(w, status, map[string]any{
		"ok":               false,
		"error":            strings.TrimSpace(message),
		"live":             live,
		"catalog":          catalog,
		"process":          InspectManagedAgentProcess(record),
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	})
}

func writeJSONControlActionError(
	w http.ResponseWriter,
	status int,
	record ManagedAgentRecord,
	result managedAgentControlRequestResult,
	live managerLiveRuntimeStatus,
	catalog managerWorkspaceCatalog,
	message string,
) {
	overview := managerWebCurrentOverview()
	payload := map[string]any{
		"ok":               false,
		"error":            strings.TrimSpace(message),
		"live":             live,
		"catalog":          catalog,
		"process":          InspectManagedAgentProcess(record),
		"command":          overview.Command,
		"defaults":         overview.Defaults,
		"agents":           overview.Agents,
		"providers":        overview.Providers,
		"provider_catalog": overview.ProviderCatalog,
		"providers_error":  overview.ProvidersError,
		"create_default":   overview.CreateDefault,
	}
	if hasManagedAgentControlRequestResult(result) {
		payload["result"] = result
	}
	writeJSON(w, status, payload)
}

func openBrowserURL(url string) error {
	var cmd *exec.Cmd
	switch goruntime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}

func isLoopbackDashboardHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1", "[::1]":
		return true
	default:
		return false
	}
}

func managerWebListenAddress(host string, port int) string {
	return net.JoinHostPort(strings.Trim(strings.TrimSpace(host), "[]"), strconv.Itoa(port))
}

func redactBotManagerDefaults(defaults BotManagerDefaults) BotManagerDefaults {
	defaults.WorkspacePassword = ""
	return defaults
}

func redactLocalRuntimeProfile(profile LocalRuntimeProfile) LocalRuntimeProfile {
	profile.WorkspacePassword = ""
	profile.AgentToken = ""
	return profile
}

func managerWebEffectiveIdentityFromLocalRuntime(profile LocalRuntimeProfile) managerWebEffectiveIdentity {
	source := "bootstrap_local_runtime"
	if strings.TrimSpace(profile.RegisteredExecutor.AgentID) != "" {
		source = "registered_executor"
	}
	return managerWebEffectiveIdentity{
		Source:          source,
		AgentID:         profile.effectiveAgentID(),
		WorkspaceID:     profile.effectiveWorkspaceID(),
		DisplayName:     profile.effectiveDisplayName(),
		OwnerUserID:     profile.effectiveOwnerUserID(),
		Role:            profile.effectiveRole(),
		ProtocolVersion: profile.effectiveProtocolVersion(),
		Capabilities:    profile.effectiveCapabilities(),
	}
}

func managerWebEffectiveIdentityForRecord(record ManagedAgentRecord, profile LocalRuntimeProfile) managerWebEffectiveIdentity {
	record = normalizeManagedAgentRecord(record)
	identity := managerWebEffectiveIdentityFromLocalRuntime(profile)
	if strings.TrimSpace(identity.AgentID) == "" {
		identity.AgentID = record.AgentID
	}
	if strings.TrimSpace(identity.WorkspaceID) == "" {
		identity.WorkspaceID = record.WorkspaceID
	}
	if strings.TrimSpace(identity.DisplayName) == "" {
		identity.DisplayName = record.DisplayName
	}
	if strings.TrimSpace(identity.OwnerUserID) == "" {
		identity.OwnerUserID = record.OwnerUserID
	}
	if strings.TrimSpace(identity.Role) == "" {
		identity.Role = record.Role
	}
	if identity.Source == "bootstrap_local_runtime" &&
		strings.TrimSpace(profile.RegisteredExecutor.AgentID) == "" &&
		strings.TrimSpace(profile.AgentID) == "" &&
		strings.TrimSpace(profile.DisplayName) == "" &&
		strings.TrimSpace(profile.WorkspaceID) == "" &&
		strings.TrimSpace(profile.OwnerUserID) == "" &&
		strings.TrimSpace(profile.Role) == "" {
		identity.Source = "registry"
	}
	return identity
}

func isAllowedWebControlMethod(method string) bool {
	switch strings.TrimSpace(method) {
	case "runtime.status", "runtime.refresh", "runtime.pause", "runtime.resume", "runtime.switch_task", "runtime.switch_tension", "model.ask":
		return true
	default:
		return false
	}
}

func recoverWebOnboardingFailure(workdir string, existedBefore bool, cause error) error {
	if !existedBefore {
		return os.RemoveAll(workdir)
	}
	return writeWebOnboardingMarker(workdir, cause)
}

func writeWebOnboardingMarker(workdir string, cause error) error {
	if strings.TrimSpace(workdir) == "" {
		return nil
	}
	path := filepath.Join(workdir, ".rhizome-bot-onboarding-incomplete.json")
	raw, err := json.MarshalIndent(map[string]any{
		"status":     "incomplete",
		"updated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"error":      strings.TrimSpace(fmt.Sprint(cause)),
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

func clearWebOnboardingMarker(workdir string) error {
	if strings.TrimSpace(workdir) == "" {
		return nil
	}
	path := filepath.Join(workdir, ".rhizome-bot-onboarding-incomplete.json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type webFSEntry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
}

type webFSListResponse struct {
	Path    string       `json:"path"`
	Parent  string       `json:"parent"`
	Entries []webFSEntry `json:"entries"`
	managerWebOverview
}

type webFSListErrorResponse struct {
	Error   string       `json:"error"`
	Path    string       `json:"path"`
	Parent  string       `json:"parent"`
	Entries []webFSEntry `json:"entries"`
	managerWebOverview
}

func defaultWebFSListDir() string {
	home, err := os.UserHomeDir()
	if err == nil && home != "" {
		return home
	}
	return "C:\\"
}

func resolveWebFSListDir(raw string) string {
	dir := strings.TrimSpace(raw)
	if dir == "" {
		dir = defaultWebFSListDir()
	}
	return filepath.Clean(dir)
}

func writeManagerWebFSListError(w http.ResponseWriter, status int, message string, dir string) {
	parent := filepath.Dir(dir)
	if parent == dir {
		parent = ""
	}
	writeJSON(w, status, webFSListErrorResponse{
		Error:              strings.TrimSpace(message),
		Path:               dir,
		Parent:             parent,
		Entries:            []webFSEntry{},
		managerWebOverview: managerWebCurrentOverview(),
	})
}

func (s *managerWebServer) handleFSList(w http.ResponseWriter, r *http.Request) {
	dir := resolveWebFSListDir(r.URL.Query().Get("dir"))
	if r.Method != http.MethodGet {
		writeManagerWebFSListError(w, http.StatusMethodNotAllowed, "method not allowed", dir)
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		writeManagerWebFSListError(w, http.StatusInternalServerError, err.Error(), dir)
		return
	}

	var out []webFSEntry
	for _, e := range entries {
		out = append(out, webFSEntry{
			Name:  e.Name(),
			IsDir: e.IsDir(),
		})
	}

	parent := filepath.Dir(dir)
	if parent == dir {
		parent = ""
	}

	writeJSON(w, http.StatusOK, webFSListResponse{
		Path:               dir,
		Parent:             parent,
		Entries:            out,
		managerWebOverview: managerWebCurrentOverview(),
	})
}
