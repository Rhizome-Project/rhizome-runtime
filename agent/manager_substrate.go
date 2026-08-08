package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"
)

var (
	managerSubstrateExecutableFunc          = os.Executable
	managerSubstrateInstalledExecutableFunc = defaultInstalledRhizomeBotPath
	managerSubstrateRepoRootFunc            = managerSubstrateDiscoverRepoRoot
	managerSubstrateCommandOutputFunc       = managerSubstrateCommandOutput
	managerSubstrateRuntimeBuildFunc        = managerCurrentRuntimeBuildIdentity
	managerSubstrateRepositoryFunc          = managerCurrentRepositoryIdentity
	managerRemoteCleanStopBlockersFunc      = managerRemoteCleanStopBlockersForAgent
)

const (
	managerSubstrateCommandTimeout               = 2 * time.Second
	managerSubstrateRepositoryCommandMaxAttempts = 3
)

var managerSubstrateRepositoryCommandRetryDelay = 200 * time.Millisecond

const (
	managerProviderSmokeTimeout          = 5 * time.Second
	managerProviderModelSmokeTimeout     = 45 * time.Second
	managerProviderModelSmokeMaxAttempts = 2
)

const (
	managedRunSubstrateAdmissionContract = "managed_run_substrate_admission.v1"
	managedRunSubstrateAdmissionFilename = "managed-run-substrate-admission.json"
	managedRunFixtureAllowCleanStopEnv   = "RHIZOME_MANAGED_RUN_FIXTURE_ALLOW_CLEAN_STOP"
)

const (
	managerPatchQueueIntegrationAdmittedEventType = "project.patch_queue.integration_admitted"
	managerPatchQueueIntegratedEventType          = "project.patch_queue.integrated"
	managerPatchQueueIntegrationRepairEventType   = "project.patch_queue.integration_repair"
	managerPatchQueueIntegrationEntityType        = "project_patch_queue_item"
)

var managerProviderModelSmokeRetryDelay = 2 * time.Second

type managerWebSubstrateStatus struct {
	Status              string                            `json:"status"`
	Reasons             []string                          `json:"reasons,omitempty"`
	Diagnostics         managerSubstrateDiagnostics       `json:"diagnostics"`
	CurrentExecutable   managerExecutableIdentity         `json:"current_executable"`
	InstalledExecutable managerExecutableIdentity         `json:"installed_executable"`
	Build               managerRuntimeBuildIdentity       `json:"build"`
	Repository          managerRepositoryIdentity         `json:"repository"`
	Providers           []managerProviderRouteReadiness   `json:"providers,omitempty"`
	Browser             managerBrowserReadiness           `json:"browser"`
	CleanStop           managerCleanStopReadiness         `json:"clean_stop"`
	RPCContract         managerRPCContractReadiness       `json:"rpc_contract"`
	ToolBundles         []managerAgentToolBundleReadiness `json:"tool_bundles,omitempty"`
}

type managerSubstrateDiagnostics struct {
	Status      string   `json:"status"`
	ReasonCount int      `json:"reason_count"`
	ReasonCodes []string `json:"reason_codes,omitempty"`
}

type managerSubstrateOptions struct {
	SkipProviderManagedSmoke bool
}

type managerExecutableIdentity struct {
	Path   string `json:"path,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	Error  string `json:"error,omitempty"`
}

type managerRuntimeBuildIdentity struct {
	GoVersion    string `json:"go_version,omitempty"`
	VCSRevision  string `json:"vcs_revision,omitempty"`
	VCSTime      string `json:"vcs_time,omitempty"`
	VCSModified  bool   `json:"vcs_modified,omitempty"`
	BuildInfoOK  bool   `json:"build_info_ok"`
	BuildInfoErr string `json:"build_info_error,omitempty"`
}

type managerRepositoryIdentity struct {
	Root            string   `json:"root,omitempty"`
	Head            string   `json:"head,omitempty"`
	Branch          string   `json:"branch,omitempty"`
	Dirty           bool     `json:"dirty"`
	DirtyEntryCount int      `json:"dirty_entry_count,omitempty"`
	DirtyPreview    []string `json:"dirty_preview,omitempty"`
	Error           string   `json:"error,omitempty"`
}

type managerProviderRouteReadiness struct {
	AgentID     string `json:"agent_id,omitempty"`
	ProviderID  string `json:"provider_id,omitempty"`
	Driver      string `json:"driver,omitempty"`
	Channel     string `json:"channel,omitempty"`
	Model       string `json:"model,omitempty"`
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`
	Executable  string `json:"executable,omitempty"`
	SmokeStatus string `json:"smoke_status,omitempty"`
	SmokeReason string `json:"smoke_reason,omitempty"`
}

type managerAgentToolBundleReadiness struct {
	AgentID         string                          `json:"agent_id,omitempty"`
	Workdir         string                          `json:"workdir,omitempty"`
	Status          string                          `json:"status"`
	Reason          string                          `json:"reason,omitempty"`
	RequiredBundles []string                        `json:"required_bundles,omitempty"`
	Candidates      []ToolBundleReadinessItem       `json:"candidates,omitempty"`
	Suites          []ToolBundleSuiteReadinessItem  `json:"suites,omitempty"`
	Healthchecks    []ToolBundleDiscoveryDiagnostic `json:"healthchecks,omitempty"`
}

type managerBrowserReadiness struct {
	Status string   `json:"status"`
	Reason string   `json:"reason,omitempty"`
	Agents []string `json:"agents,omitempty"`
}

type managerCleanStopReadiness struct {
	Status string   `json:"status"`
	Reason string   `json:"reason,omitempty"`
	Agents []string `json:"agents,omitempty"`
}

type managerRPCContractReadiness struct {
	Status          string                             `json:"status"`
	Reason          string                             `json:"reason,omitempty"`
	RequiredMethods []string                           `json:"required_methods,omitempty"`
	MissingMethods  []string                           `json:"missing_methods,omitempty"`
	Agents          []managerRPCContractAgentReadiness `json:"agents,omitempty"`
}

type managerRPCContractAgentReadiness struct {
	AgentID        string   `json:"agent_id,omitempty"`
	Endpoint       string   `json:"endpoint,omitempty"`
	Status         string   `json:"status"`
	Reason         string   `json:"reason,omitempty"`
	MethodCount    int      `json:"method_count,omitempty"`
	MissingMethods []string `json:"missing_methods,omitempty"`
}

type managedRunSubstrateAdmissionReceipt struct {
	Contract              string                    `json:"contract"`
	Status                string                    `json:"status"`
	Admission             string                    `json:"admission"`
	Reasons               []string                  `json:"reasons,omitempty"`
	WaivedReasons         []string                  `json:"waived_reasons,omitempty"`
	AgentID               string                    `json:"agent_id,omitempty"`
	Workdir               string                    `json:"workdir,omitempty"`
	GeneratedAt           string                    `json:"generated_at"`
	ChildExecutablePath   string                    `json:"child_executable_path,omitempty"`
	ChildExecutableSHA256 string                    `json:"child_executable_sha256,omitempty"`
	Substrate             managerWebSubstrateStatus `json:"substrate"`
}

type managedRunPreflightResult struct {
	Receipt               managedRunSubstrateAdmissionReceipt
	ChildExecutablePath   string
	ChildExecutableSHA256 string
}

type managedRunPreflightOptions struct {
	RepositoryDirtyPathAllowlist    []string
	ResumeContinuationWaiver        managedRunResumeContinuationWaiver
	AllowResumeDirtyProjectCheckout bool
	AllowResumeLivePatchQueue       bool
	AllowResumeAgentRequests        bool
	AllowResumePendingTriggers      bool
}

type managedRunResumeContinuationWaiver struct {
	AllowDirtyProjectCheckout  bool `json:"allow_dirty_project_checkout,omitempty"`
	AllowLivePatchQueue        bool `json:"allow_live_patch_queue,omitempty"`
	AllowAgentRequests         bool `json:"allow_agent_requests,omitempty"`
	AllowLiveProjectBranches   bool `json:"allow_live_project_branches,omitempty"`
	AllowPendingResumeTriggers bool `json:"allow_pending_resume_triggers,omitempty"`
}

func managedRunFullResumeContinuationWaiver() managedRunResumeContinuationWaiver {
	return managedRunResumeContinuationWaiver{
		AllowDirtyProjectCheckout:  true,
		AllowLivePatchQueue:        true,
		AllowAgentRequests:         true,
		AllowLiveProjectBranches:   true,
		AllowPendingResumeTriggers: true,
	}
}

func (waiver managedRunResumeContinuationWaiver) hasAny() bool {
	return waiver.AllowDirtyProjectCheckout || waiver.AllowLivePatchQueue || waiver.AllowAgentRequests || waiver.AllowLiveProjectBranches || waiver.AllowPendingResumeTriggers
}

func (options managedRunPreflightOptions) resumeContinuationWaiver() managedRunResumeContinuationWaiver {
	waiver := options.ResumeContinuationWaiver
	if options.AllowResumeDirtyProjectCheckout {
		waiver.AllowDirtyProjectCheckout = true
	}
	if options.AllowResumeLivePatchQueue {
		waiver.AllowLivePatchQueue = true
	}
	if options.AllowResumeAgentRequests {
		waiver.AllowAgentRequests = true
	}
	if options.AllowResumePendingTriggers {
		waiver.AllowPendingResumeTriggers = true
	}
	return waiver
}

func (options managedRunPreflightOptions) hasOverrides() bool {
	return len(options.RepositoryDirtyPathAllowlist) > 0 || options.resumeContinuationWaiver().hasAny()
}

type managedRunPreflightBlockedError struct {
	Reasons []string
}

func (e *managedRunPreflightBlockedError) Error() string {
	reasons := uniqueTrimmedCSVStrings(e.Reasons)
	if len(reasons) == 0 {
		return "managed run substrate admission blocked"
	}
	return "managed run substrate admission blocked: " + strings.Join(reasons, "; ")
}

func isManagedRunPreflightBlockedError(err error) bool {
	var blocked *managedRunPreflightBlockedError
	return errors.As(err, &blocked)
}

var admitManagedRunStartFunc = admitManagedRunStart
var admitManagedRunStartWithOptionsFunc = admitManagedRunStartWithOptions

func managerWebCurrentSubstrate(registry BotRegistry, providerRegistry ProviderRegistry, providerErr error) managerWebSubstrateStatus {
	return managerWebCurrentSubstrateWithOptions(registry, providerRegistry, providerErr, managerSubstrateOptions{})
}

func managerWebCurrentSubstrateForOverview(registry BotRegistry, providerRegistry ProviderRegistry, providerErr error) managerWebSubstrateStatus {
	return managerWebCurrentSubstrateWithOptions(registry, providerRegistry, providerErr, managerSubstrateOptions{SkipProviderManagedSmoke: true})
}

func managerWebCurrentSubstrateWithOptions(registry BotRegistry, providerRegistry ProviderRegistry, providerErr error, options managerSubstrateOptions) managerWebSubstrateStatus {
	status := managerWebSubstrateStatus{
		Status:              "pass",
		CurrentExecutable:   managerCurrentExecutableIdentity(),
		InstalledExecutable: managerInstalledExecutableIdentity(),
		Build:               managerSubstrateRuntimeBuildFunc(),
		Repository:          managerSubstrateRepositoryFunc(),
	}
	status.addSubstrateReasonIf(status.CurrentExecutable.Error != "", "current_executable:"+status.CurrentExecutable.Error)
	status.addSubstrateReasonIf(status.InstalledExecutable.Error != "", "installed_executable:"+status.InstalledExecutable.Error)
	status.addSubstrateReasonIf(status.CurrentExecutable.SHA256 != "" && status.InstalledExecutable.SHA256 != "" && status.CurrentExecutable.SHA256 != status.InstalledExecutable.SHA256, "installed_executable_hash_mismatch")
	status.addSubstrateReasonIf(!status.Build.BuildInfoOK, "build_info_unavailable:"+firstNonEmpty(status.Build.BuildInfoErr, "missing"))
	status.addSubstrateReasonIf(status.Repository.Head != "" && status.Build.VCSRevision == "", "build_revision_missing")
	status.addSubstrateReasonIf(status.Build.VCSModified, "build_vcs_modified")
	status.addSubstrateReasonIf(status.Build.VCSRevision != "" && status.Repository.Head != "" && !managerRepositoryContainsBuildRevision(status.Repository, status.Build.VCSRevision), "build_revision_mismatch")
	status.addSubstrateReasonIf(status.Repository.Error != "", "repository:"+status.Repository.Error)
	status.addSubstrateReasonIf(status.Repository.Dirty, fmt.Sprintf("repository_dirty:%d", status.Repository.DirtyEntryCount))
	if providerErr != nil {
		status.addSubstrateReasonIf(true, "provider_registry:"+providerErr.Error())
	}

	status.Providers = managerProviderReadinessForRosterWithOptions(registry, providerRegistry, providerErr, options)
	for _, provider := range status.Providers {
		if provider.Status != "ready" && provider.Status != "not_required" {
			status.addSubstrateReasonIf(true, "provider:"+firstNonEmpty(provider.AgentID, provider.ProviderID, "unknown")+":"+provider.Status)
		}
	}
	status.ToolBundles = managerToolBundleReadinessForRoster(registry)
	for _, bundle := range status.ToolBundles {
		if bundle.Status != "ready" && bundle.Status != "not_required" {
			status.addSubstrateReasonIf(true, "tool_bundles:"+firstNonEmpty(bundle.AgentID, bundle.Workdir, "unknown")+":"+bundle.Status)
		}
	}
	status.Browser = managerBrowserReadinessFromToolBundles(status.ToolBundles)
	if status.Browser.Status == "blocked" {
		status.addSubstrateReasonIf(true, "browser:"+status.Browser.Status)
	}
	status.CleanStop = managerCleanStopReadinessForRoster(registry)
	if status.CleanStop.Status == "blocked" {
		status.addSubstrateReasonIf(true, "clean_stop:"+status.CleanStop.Status)
	}
	status.RPCContract = managerRPCContractReadinessForRoster(registry)
	if status.RPCContract.Status == "blocked" {
		// FF-R09-1b: surface the specific blocked probe in the admission reason so a failed
		// roster start names the cause in the orchestrator log instead of a bare status.
		reason := "rpc_contract:" + status.RPCContract.Status
		if detail := strings.TrimSpace(status.RPCContract.Reason); detail != "" {
			if len(detail) > 300 {
				detail = detail[:300] + "..."
			}
			reason += " (" + detail + ")"
		}
		status.addSubstrateReasonIf(true, reason)
	}
	status.Diagnostics = managerSubstrateDiagnostics{
		Status:      status.Status,
		ReasonCount: len(status.Reasons),
		ReasonCodes: append([]string(nil), status.Reasons...),
	}
	return status
}

func ManagedRunPreflight(record ManagedAgentRecord) (managedRunPreflightResult, error) {
	return managedRunPreflightWithOptions(record, managedRunPreflightOptions{})
}

func managedRunPreflightWithOptions(record ManagedAgentRecord, options managedRunPreflightOptions) (managedRunPreflightResult, error) {
	record = normalizeManagedAgentRecord(record)
	registry := LoadBotRegistry()
	registry.Agents = []ManagedAgentRecord{record}
	providerRegistry, providerErr := LoadProviderRegistryWithError()
	var materializeErr error
	if strings.TrimSpace(record.Workdir) != "" {
		_, materializeErr = MaterializeManagedAgentAnatomy(record, managedAgentStartRuntimeConfig(record))
	}
	status := managerWebCurrentSubstrate(registry, providerRegistry, providerErr)
	if materializeErr != nil {
		status.addSubstrateReasonIf(true, "anatomy_materialization:"+materializeErr.Error())
		status.Diagnostics = managerSubstrateDiagnostics{
			Status:      status.Status,
			ReasonCount: len(status.Reasons),
			ReasonCodes: append([]string(nil), status.Reasons...),
		}
	}
	reasons := append([]string(nil), status.Reasons...)
	childExecutablePath := strings.TrimSpace(status.InstalledExecutable.Path)
	childExecutableSHA256 := strings.TrimSpace(status.InstalledExecutable.SHA256)
	if childExecutablePath == "" {
		reasons = append(reasons, "child_executable_missing")
	}
	if childExecutableSHA256 == "" {
		reasons = append(reasons, "child_executable_hash_missing")
	}
	reasons = uniqueTrimmedCSVStrings(reasons)
	admissionReasons, waivedReasons := managedRunApplyFixtureCleanStopWaiver(reasons)
	admissionReasons, resumeWaivedReasons := managedRunApplyResumeContinuationWaiver(admissionReasons, status.CleanStop, options.resumeContinuationWaiver())
	waivedReasons = uniqueTrimmedCSVStrings(append(waivedReasons, resumeWaivedReasons...))
	admissionReasons, repositoryWaivedReasons := managedRunApplyRepositoryDirtyAllowlistWaiver(admissionReasons, status.Repository, status.Build, options.RepositoryDirtyPathAllowlist)
	waivedReasons = uniqueTrimmedCSVStrings(append(waivedReasons, repositoryWaivedReasons...))
	admission := "admitted"
	if len(admissionReasons) > 0 {
		admission = "blocked"
	}
	receipt := managedRunSubstrateAdmissionReceipt{
		Contract:              managedRunSubstrateAdmissionContract,
		Status:                status.Status,
		Admission:             admission,
		Reasons:               admissionReasons,
		WaivedReasons:         waivedReasons,
		AgentID:               strings.TrimSpace(record.AgentID),
		Workdir:               strings.TrimSpace(record.Workdir),
		GeneratedAt:           time.Now().UTC().Format(time.RFC3339Nano),
		ChildExecutablePath:   childExecutablePath,
		ChildExecutableSHA256: childExecutableSHA256,
		Substrate:             status,
	}
	result := managedRunPreflightResult{
		Receipt:               receipt,
		ChildExecutablePath:   childExecutablePath,
		ChildExecutableSHA256: childExecutableSHA256,
	}
	if len(admissionReasons) > 0 {
		return result, &managedRunPreflightBlockedError{Reasons: admissionReasons}
	}
	return result, nil
}

func managedRunApplyRepositoryDirtyAllowlistWaiver(reasons []string, repository managerRepositoryIdentity, build managerRuntimeBuildIdentity, allowlist []string) ([]string, []string) {
	reasons = uniqueTrimmedCSVStrings(reasons)
	if len(reasons) == 0 {
		return reasons, nil
	}
	if !managerRepositoryDirtyMatchesAllowlist(repository, allowlist) {
		return reasons, nil
	}
	allowBuildModified := managedRunBuildVCSModifiedMatchesAllowedRepositoryDirty(repository, build)
	admissionReasons := make([]string, 0, len(reasons))
	waivedReasons := make([]string, 0, 1)
	for _, reason := range reasons {
		reason = strings.TrimSpace(reason)
		if strings.HasPrefix(strings.ToLower(reason), "repository_dirty:") {
			waivedReasons = append(waivedReasons, reason)
			continue
		}
		if allowBuildModified && strings.EqualFold(reason, "build_vcs_modified") {
			waivedReasons = append(waivedReasons, reason)
			continue
		}
		admissionReasons = append(admissionReasons, reason)
	}
	return uniqueTrimmedCSVStrings(admissionReasons), uniqueTrimmedCSVStrings(waivedReasons)
}

func managedRunBuildVCSModifiedMatchesAllowedRepositoryDirty(repository managerRepositoryIdentity, build managerRuntimeBuildIdentity) bool {
	if !build.VCSModified {
		return false
	}
	if strings.TrimSpace(build.VCSRevision) == "" || strings.TrimSpace(repository.Head) == "" {
		return false
	}
	return managerRepositoryContainsBuildRevision(repository, build.VCSRevision)
}

func managedRunApplyResumeDirtyProjectCheckoutWaiver(reasons []string, cleanStop managerCleanStopReadiness, allow bool) ([]string, []string) {
	return managedRunApplyResumeCleanStopContinuationWaiver(reasons, cleanStop, allow, false, false)
}

func managedRunApplyResumeCleanStopContinuationWaiver(reasons []string, cleanStop managerCleanStopReadiness, allowDirtyProjectCheckout, allowLivePatchQueue, allowAgentRequests bool) ([]string, []string) {
	return managedRunApplyResumeContinuationWaiver(reasons, cleanStop, managedRunResumeContinuationWaiver{
		AllowDirtyProjectCheckout: allowDirtyProjectCheckout,
		AllowLivePatchQueue:       allowLivePatchQueue,
		AllowAgentRequests:        allowAgentRequests,
	})
}

func managedRunApplyResumeContinuationWaiver(reasons []string, cleanStop managerCleanStopReadiness, waiver managedRunResumeContinuationWaiver) ([]string, []string) {
	reasons = uniqueTrimmedCSVStrings(reasons)
	if len(reasons) == 0 || !waiver.hasAny() {
		return reasons, nil
	}
	if !managerCleanStopReasonIsOnlyResumeContinuation(cleanStop, waiver) {
		return reasons, nil
	}
	admissionReasons := make([]string, 0, len(reasons))
	waivedReasons := make([]string, 0, 1)
	for _, reason := range reasons {
		reason = strings.TrimSpace(reason)
		if strings.EqualFold(reason, "clean_stop:blocked") {
			waivedReasons = append(waivedReasons, reason)
			continue
		}
		admissionReasons = append(admissionReasons, reason)
	}
	return uniqueTrimmedCSVStrings(admissionReasons), uniqueTrimmedCSVStrings(waivedReasons)
}

func managerCleanStopReasonIsOnlyLocalDirtyProjectCheckout(cleanStop managerCleanStopReadiness) bool {
	return managerCleanStopReasonIsOnlyResumeContinuation(cleanStop, managedRunResumeContinuationWaiver{AllowDirtyProjectCheckout: true})
}

func managerCleanStopReasonIsOnlyResumeContinuation(cleanStop managerCleanStopReadiness, waiver managedRunResumeContinuationWaiver) bool {
	if !strings.EqualFold(strings.TrimSpace(cleanStop.Status), "blocked") {
		return false
	}
	parts := strings.Split(cleanStop.Reason, ";")
	seen := false
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.SplitN(part, ":", 3)
		if len(fields) != 3 || strings.TrimSpace(fields[0]) == "" || strings.TrimSpace(fields[2]) == "" {
			return false
		}
		switch strings.ToLower(strings.TrimSpace(fields[1])) {
		case "local_dirty_project_checkout":
			if !waiver.AllowDirtyProjectCheckout {
				return false
			}
		case "remote_active_project_checkout", "remote_dirty_project_checkout":
			if !waiver.AllowDirtyProjectCheckout {
				return false
			}
		case "remote_patch_queue_live", "remote_patch_queue_claim", "remote_patch_queue_live_claimed_by":
			if !waiver.AllowLivePatchQueue {
				return false
			}
		case "remote_open_agent_request":
			if !waiver.AllowAgentRequests || !managerCleanStopRemoteAgentRequestIsResumeContinuation(fields[2]) {
				return false
			}
		case "remote_project_branch_live":
			if !waiver.AllowLiveProjectBranches || !managerCleanStopRemoteProjectBranchIsResumeContinuation(fields[2]) {
				return false
			}
		case "remote_pending_trigger":
			if !waiver.AllowPendingResumeTriggers || !managerCleanStopRemotePendingTriggerIsResumeContinuation(fields[2]) {
				return false
			}
		default:
			return false
		}
		seen = true
	}
	return seen
}

func managerCleanStopRemotePendingTriggerIsResumeContinuation(detail string) bool {
	return strings.EqualFold(strings.TrimSpace(detail), "request_resume")
}

func managerCleanStopRemoteAgentRequestIsResumeContinuation(detail string) bool {
	detail = strings.TrimSpace(detail)
	idx := strings.LastIndex(detail, ":")
	if idx <= 0 || idx == len(detail)-1 {
		return false
	}
	requestID := strings.TrimSpace(detail[:idx])
	status := strings.ToUpper(strings.TrimSpace(detail[idx+1:]))
	if requestID == "" {
		return false
	}
	return status == "PENDING" || status == "TIMEOUT"
}

func managerCleanStopRemoteProjectBranchIsResumeContinuation(detail string) bool {
	fields := strings.Split(strings.TrimSpace(detail), ":")
	if len(fields) < 2 {
		return false
	}
	branchRef := strings.TrimSpace(fields[0])
	status := strings.ToUpper(strings.TrimSpace(fields[1]))
	if branchRef == "" {
		return false
	}
	switch status {
	case "RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW":
		return true
	default:
		return false
	}
}

func managerRepositoryDirtyMatchesAllowlist(repository managerRepositoryIdentity, allowlist []string) bool {
	if !repository.Dirty || repository.DirtyEntryCount <= 0 {
		return false
	}
	dirtyPaths := managerRepositoryDirtyPreviewPaths(repository)
	if len(dirtyPaths) != repository.DirtyEntryCount || len(dirtyPaths) != len(allowlist) {
		return false
	}
	allowed := map[string]struct{}{}
	for _, path := range allowlist {
		normalized := managerNormalizeRepositoryPath(path)
		if normalized != "" {
			allowed[normalized] = struct{}{}
		}
	}
	if len(allowed) != len(allowlist) || len(allowed) != len(dirtyPaths) {
		return false
	}
	for _, path := range dirtyPaths {
		if _, ok := allowed[path]; !ok {
			return false
		}
	}
	return true
}

func managerRepositoryDirtyPreviewPaths(repository managerRepositoryIdentity) []string {
	if repository.DirtyEntryCount > len(repository.DirtyPreview) {
		return nil
	}
	paths := make([]string, 0, len(repository.DirtyPreview))
	for _, entry := range repository.DirtyPreview {
		path := managerGitPorcelainEntryPath(entry)
		if path == "" {
			return nil
		}
		paths = append(paths, path)
	}
	return paths
}

func managerGitPorcelainEntryPath(entry string) string {
	entry = strings.TrimRight(entry, "\r\n")
	if len(entry) >= 3 && entry[2] == ' ' {
		entry = entry[3:]
	} else if idx := strings.IndexAny(entry, " \t"); idx >= 0 {
		entry = entry[idx+1:]
	}
	entry = strings.TrimSpace(entry)
	if idx := strings.LastIndex(entry, " -> "); idx >= 0 {
		entry = strings.TrimSpace(entry[idx+4:])
	}
	return managerNormalizeRepositoryPath(entry)
}

func managerNormalizeRepositoryPath(path string) string {
	path = strings.TrimSpace(strings.ReplaceAll(path, "\\", "/"))
	for strings.HasPrefix(path, "./") {
		path = strings.TrimPrefix(path, "./")
	}
	return strings.ToLower(path)
}

func managedRunApplyFixtureCleanStopWaiver(reasons []string) ([]string, []string) {
	reasons = uniqueTrimmedCSVStrings(reasons)
	if !managedRunFixtureCleanStopWaiverEnabled() {
		return reasons, nil
	}
	admissionReasons := make([]string, 0, len(reasons))
	waivedReasons := make([]string, 0, 1)
	for _, reason := range reasons {
		if strings.EqualFold(strings.TrimSpace(reason), "clean_stop:blocked") {
			waivedReasons = append(waivedReasons, reason)
			continue
		}
		admissionReasons = append(admissionReasons, reason)
	}
	return uniqueTrimmedCSVStrings(admissionReasons), uniqueTrimmedCSVStrings(waivedReasons)
}

func managedRunFixtureCleanStopWaiverEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(managedRunFixtureAllowCleanStopEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func managedRunSubstrateAdmissionReceiptPath(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	return filepath.Join(workdir, ".runtime-config", managedRunSubstrateAdmissionFilename)
}

func writeManagedRunSubstrateAdmissionReceipt(workdir string, receipt managedRunSubstrateAdmissionReceipt) error {
	path := managedRunSubstrateAdmissionReceiptPath(workdir)
	if path == "" {
		return fmt.Errorf("managed run substrate admission receipt requires workdir")
	}
	raw, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return atomicWriteFile(path, raw, 0o600)
}

func admitManagedRunStart(record ManagedAgentRecord) (managedRunPreflightResult, error) {
	return admitManagedRunStartWithOptions(record, managedRunPreflightOptions{})
}

func admitManagedRunStartWithOptions(record ManagedAgentRecord, options managedRunPreflightOptions) (managedRunPreflightResult, error) {
	preflight, preflightErr := managedRunPreflightWithOptions(record, options)
	if err := writeManagedRunSubstrateAdmissionReceipt(record.Workdir, preflight.Receipt); err != nil {
		return preflight, fmt.Errorf("persist managed run substrate admission receipt: %w", err)
	}
	return preflight, preflightErr
}

func (s *managerWebSubstrateStatus) addSubstrateReasonIf(condition bool, reason string) {
	if !condition {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	s.Reasons = append(s.Reasons, reason)
	s.Status = "fail"
}

func managerCurrentExecutableIdentity() managerExecutableIdentity {
	path, err := managerSubstrateExecutableFunc()
	if err != nil {
		return managerExecutableIdentity{Error: err.Error()}
	}
	return managerExecutableIdentityForPath(path)
}

func managerInstalledExecutableIdentity() managerExecutableIdentity {
	path := strings.TrimSpace(managerSubstrateInstalledExecutableFunc())
	if path == "" {
		return managerExecutableIdentity{Error: "installed executable path is unavailable"}
	}
	return managerExecutableIdentityForPath(path)
}

func managerExecutableIdentityForPath(path string) managerExecutableIdentity {
	identity := managerExecutableIdentity{Path: strings.TrimSpace(path)}
	if identity.Path == "" {
		identity.Error = "empty executable path"
		return identity
	}
	digest, err := managerFileSHA256(identity.Path)
	if err != nil {
		identity.Error = err.Error()
		return identity
	}
	identity.SHA256 = digest
	return identity
}

func defaultInstalledRhizomeBotPath() string {
	if env := strings.TrimSpace(os.Getenv("RHIZOME_BOT_INSTALLED_PATH")); env != "" {
		return env
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return ""
	}
	name := "rhizome-bot"
	if goruntime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(home, ".local", "bin", name)
}

func managerCurrentRuntimeBuildIdentity() managerRuntimeBuildIdentity {
	info := managerRuntimeBuildIdentity{}
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		info.BuildInfoErr = "debug build info unavailable"
		return info
	}
	info.BuildInfoOK = true
	info.GoVersion = buildInfo.GoVersion
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			info.VCSRevision = setting.Value
		case "vcs.time":
			info.VCSTime = setting.Value
		case "vcs.modified":
			info.VCSModified = strings.EqualFold(strings.TrimSpace(setting.Value), "true")
		}
	}
	return info
}

func managerCurrentRepositoryIdentity() managerRepositoryIdentity {
	root := strings.TrimSpace(managerSubstrateRepoRootFunc())
	identity := managerRepositoryIdentity{Root: root}
	if root == "" {
		identity.Error = "git repository root not found"
		return identity
	}
	if head, err := managerSubstrateRepositoryCommandOutput(root, "git", "-C", root, "rev-parse", "HEAD"); err == nil {
		identity.Head = strings.TrimSpace(head)
	} else {
		identity.Error = err.Error()
	}
	if branch, err := managerSubstrateRepositoryCommandOutput(root, "git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD"); err == nil {
		identity.Branch = strings.TrimSpace(branch)
	}
	if raw, err := managerSubstrateRepositoryCommandOutput(root, "git", "-C", root, "status", "--porcelain"); err == nil {
		entries := nonEmptyLines(raw)
		identity.Dirty = len(entries) > 0
		identity.DirtyEntryCount = len(entries)
		if len(entries) > 20 {
			identity.DirtyPreview = append([]string(nil), entries[:20]...)
		} else {
			identity.DirtyPreview = entries
		}
	} else if identity.Error == "" {
		identity.Error = err.Error()
	}
	return identity
}

func managerRepositoryContainsBuildRevision(repository managerRepositoryIdentity, revision string) bool {
	revision = strings.TrimSpace(revision)
	if revision == "" {
		return false
	}
	if strings.TrimSpace(repository.Head) == revision {
		return true
	}
	return managerRepositorySubmoduleContainsRevision(repository.Root, revision)
}

func managerRepositorySubmoduleContainsRevision(root, revision string) bool {
	root = strings.TrimSpace(root)
	revision = strings.TrimSpace(revision)
	if root == "" || revision == "" {
		return false
	}
	if managerRepositoryGitlinkTreeContainsRevision(root, revision) {
		return true
	}
	raw, err := managerSubstrateRepositoryCommandOutput(root, "git", "-C", root, "submodule", "status", "--recursive")
	if err != nil {
		return false
	}
	for _, line := range nonEmptyLines(raw) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		hash := strings.TrimLeft(strings.TrimSpace(fields[0]), "+-U")
		if hash == revision {
			return true
		}
	}
	return false
}

func managerRepositoryGitlinkTreeContainsRevision(root, revision string) bool {
	raw, err := managerSubstrateRepositoryCommandOutput(root, "git", "-C", root, "ls-tree", "-r", "HEAD")
	if err != nil {
		return false
	}
	for _, line := range nonEmptyLines(raw) {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		if fields[1] == "commit" && strings.TrimSpace(fields[2]) == revision {
			return true
		}
	}
	return false
}

func managerSubstrateDiscoverRepoRoot() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if pathExists(filepath.Join(cwd, ".git")) {
			return cwd
		}
		parent := filepath.Dir(cwd)
		if parent == cwd {
			return ""
		}
		cwd = parent
	}
}

func managerSubstrateCommandOutput(_ string, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), managerSubstrateCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	raw, err := cmd.Output()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func managerSubstrateRepositoryCommandOutput(dir string, name string, args ...string) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= managerSubstrateRepositoryCommandMaxAttempts; attempt++ {
		raw, err := managerSubstrateCommandOutputFunc(dir, name, args...)
		if err == nil {
			return raw, nil
		}
		lastErr = err
		if !managerSubstrateCommandErrorIsTransient(err) || attempt == managerSubstrateRepositoryCommandMaxAttempts {
			return "", err
		}
		if managerSubstrateRepositoryCommandRetryDelay > 0 {
			time.Sleep(managerSubstrateRepositoryCommandRetryDelay)
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", errors.New("repository command failed without error")
}

func managerSubstrateCommandErrorIsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "context deadline")
}

func managerProviderReadinessForRoster(registry BotRegistry, providerRegistry ProviderRegistry, providerErr error) []managerProviderRouteReadiness {
	return managerProviderReadinessForRosterWithOptions(registry, providerRegistry, providerErr, managerSubstrateOptions{})
}

func managerProviderReadinessForRosterWithOptions(registry BotRegistry, providerRegistry ProviderRegistry, providerErr error, options managerSubstrateOptions) []managerProviderRouteReadiness {
	rows := make([]managerProviderRouteReadiness, 0, len(registry.Agents))
	smokeCache := make(map[string]managerProviderSmokeResult)
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		cfg := managedAgentStartRuntimeConfig(record)
		providerID := strings.TrimSpace(cfg.ProviderID)
		row := managerProviderRouteReadiness{
			AgentID:    record.AgentID,
			ProviderID: providerID,
			Driver:     cfg.LLMBackend,
			Model:      cfg.Model,
		}
		if providerID == "" || strings.EqualFold(providerID, "fake") || strings.EqualFold(cfg.LLMBackend, "fake") {
			row.Status = "not_required"
			row.Reason = "deterministic/fake provider route"
			rows = append(rows, row)
			continue
		}
		if providerErr != nil {
			row.Status = "unknown"
			row.Reason = providerErr.Error()
			rows = append(rows, row)
			continue
		}
		provider, ok := findProviderRecordInRegistry(providerRegistry, providerID)
		if !ok {
			row.Status = "blocked"
			row.Reason = "provider not found"
			rows = append(rows, row)
			continue
		}
		row.Driver = firstNonEmpty(provider.Driver, row.Driver)
		row.Channel = provider.ChannelType
		row.Model = firstNonEmpty(cfg.Model, provider.DefaultModel)
		row.Status, row.Reason, row.Executable = managerProviderRouteStatus(provider, cfg)
		if row.Status == "ready" {
			if options.SkipProviderManagedSmoke {
				row.SmokeStatus = "not_checked"
				row.SmokeReason = "managed-env provider smoke skipped for dashboard overview; strict managed preflight/start still runs fail-close provider smoke"
			} else {
				row.SmokeStatus, row.SmokeReason = managerProviderRouteSmokeStatus(record, provider, cfg, smokeCache)
			}
			if row.SmokeStatus == "blocked" {
				row.Status = "blocked"
				row.Reason = "provider managed-env smoke blocked: " + row.SmokeReason
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func managerProviderRouteStatus(provider ProviderRecord, cfg RuntimeConfig) (string, string, string) {
	provider = normalizeProviderRecord(provider)
	if !provider.Enabled {
		return "blocked", "provider disabled", ""
	}
	switch provider.ChannelType {
	case providerChannelBridge:
		switch provider.Driver {
		case llmBackendCodex:
			spec, err := providerCodexLaunchSpec(provider, findCodexExecutable())
			if err != nil {
				return "blocked", err.Error(), ""
			}
			return managerProviderExecutableStatus(spec.Executable)
		case llmBackendQwen:
			spec, err := providerQwenLaunchSpec(provider, findQwenExecutable())
			if err != nil {
				return "blocked", err.Error(), ""
			}
			return managerProviderExecutableStatus(spec.Executable)
		default:
			return "blocked", "unsupported bridge driver " + provider.Driver, ""
		}
	case providerChannelAPI:
		if configuredAPIKeyForProvider("", provider) == "" {
			return "blocked", "api key unavailable for provider route", ""
		}
		baseURL := strings.TrimSpace(providerOpenAIBaseURL(provider))
		if provider.Driver == providerDriverOpenAICompatible && baseURL == "" {
			return "blocked", "api base url unavailable for openai-compatible provider route", ""
		}
		if baseURL == "" && provider.Driver != providerDriverOpenRouter {
			return "blocked", "api base url unavailable for provider route", ""
		}
		return "ready", "api provider route has credential and base url metadata", ""
	default:
		if strings.TrimSpace(cfg.ProviderID) != "" {
			return "blocked", "unsupported provider channel " + provider.ChannelType, ""
		}
		return "not_required", "no provider route configured", ""
	}
}

func managerProviderExecutableStatus(path string) (string, string, string) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "blocked", "provider executable not found", ""
	}
	if executableFileExists(path) {
		return "ready", "provider executable exists", path
	}
	if resolved, err := exec.LookPath(path); err == nil && executableFileExists(resolved) {
		return "ready", "provider executable resolved on PATH", resolved
	}
	return "blocked", "provider executable does not exist", path
}

type managerProviderSmokeResult struct {
	Status string
	Reason string
}

func managerProviderRouteSmokeStatus(record ManagedAgentRecord, provider ProviderRecord, cfg RuntimeConfig, smokeCache map[string]managerProviderSmokeResult) (string, string) {
	provider = normalizeProviderRecord(provider)
	if strings.TrimSpace(provider.ProviderID) == "" {
		return "not_required", "no provider route configured"
	}
	env, err := buildManagedAgentProcessEnv(record)
	if err != nil {
		return "blocked", err.Error()
	}
	envMap := managerEnvListMap(env)
	switch provider.ChannelType {
	case providerChannelBridge:
		switch provider.Driver {
		case llmBackendCodex:
			spec, err := providerCodexLaunchSpec(provider, managerProviderBridgeFallbackExecutable(envMap, "codex", "CODEX_CLI_PATH", "CUSTOM_CLI_PATH"))
			if err != nil {
				return "blocked", err.Error()
			}
			status, reason, executable := managerProviderExecutableStatusInEnv(spec.Executable, envMap)
			if status != "ready" {
				return "blocked", reason
			}
			if err := managerProviderBridgeExecutableSmoke(record, executable, spec.Args, env); err != nil {
				return "blocked", err.Error()
			}
			model := strings.TrimSpace(firstNonEmpty(cfg.Model, provider.DefaultModel))
			if err := managerProviderCodexModelSmokeCached(record, executable, spec.Args, env, model, smokeCache); err != nil {
				return "blocked", err.Error()
			}
			return "passed", "codex bridge executable and model smoke passed in managed child env: " + executable + " model=" + model
		case llmBackendQwen:
			spec, err := providerQwenLaunchSpec(provider, managerProviderBridgeFallbackExecutable(envMap, "qwen", "QWEN_CLI_PATH"))
			if err != nil {
				return "blocked", err.Error()
			}
			status, reason, executable := managerProviderExecutableStatusInEnv(spec.Executable, envMap)
			if status != "ready" {
				return "blocked", reason
			}
			if err := managerProviderBridgeExecutableSmoke(record, executable, spec.Args, env); err != nil {
				return "blocked", err.Error()
			}
			return "passed", "qwen bridge executable runs --version in managed child env: " + executable
		default:
			return "blocked", "unsupported bridge driver " + provider.Driver
		}
	case providerChannelAPI:
		if managerProviderAPIMetadataSmokeWaiverEnabled() {
			return "waived", "api provider execution smoke is operator-waived; credential/base-url metadata is recorded in the substrate admission receipt"
		}
		for _, key := range providerAPIKeyEnvNames(provider) {
			if strings.TrimSpace(envMap[key]) != "" {
				return "blocked", "api provider credential is present in managed child env, but no executable API smoke proof is available"
			}
		}
		return "blocked", "api key unavailable in managed child env"
	default:
		return "blocked", "unsupported provider channel " + provider.ChannelType
	}
}

func managerProviderAPIMetadataSmokeWaiverEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_MANAGED_PROVIDER_API_METADATA_SMOKE_WAIVER")))
	return value == "1" || value == "true" || value == "yes"
}

func managerProviderCodexModelSmokeCached(record ManagedAgentRecord, executable string, args []string, env []string, model string, smokeCache map[string]managerProviderSmokeResult) error {
	model = strings.TrimSpace(model)
	if model == "" {
		return fmt.Errorf("codex bridge model smoke requires a concrete model")
	}
	cacheKey := "codex\x00" + strings.TrimSpace(executable) + "\x00" + strings.Join(args, "\x00") + "\x00" + model
	if smokeCache != nil {
		if cached, ok := smokeCache[cacheKey]; ok {
			if cached.Status == "passed" {
				return nil
			}
			return fmt.Errorf("%s", cached.Reason)
		}
	}
	err := managerProviderCodexModelSmokeWithRetry(record, executable, args, env, model)
	if smokeCache != nil {
		if err != nil {
			smokeCache[cacheKey] = managerProviderSmokeResult{Status: "blocked", Reason: err.Error()}
		} else {
			smokeCache[cacheKey] = managerProviderSmokeResult{Status: "passed", Reason: "passed"}
		}
	}
	return err
}

func managerProviderCodexModelSmokeWithRetry(record ManagedAgentRecord, executable string, args []string, env []string, model string) error {
	var lastErr error
	for attempt := 1; attempt <= managerProviderModelSmokeMaxAttempts; attempt++ {
		err := managerProviderCodexModelSmoke(record, executable, args, env, model)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt >= managerProviderModelSmokeMaxAttempts || !managerProviderCodexModelSmokeRetryable(err) {
			return err
		}
		if managerProviderModelSmokeRetryDelay > 0 {
			time.Sleep(managerProviderModelSmokeRetryDelay)
		}
	}
	if lastErr != nil {
		return fmt.Errorf("%w (after %d transient smoke attempts)", lastErr, managerProviderModelSmokeMaxAttempts)
	}
	return nil
}

func managerProviderCodexModelSmokeRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"unsupported model",
		"model is not supported",
		"not a supported model",
		"unknown model",
		"model_not_found",
		"does not have access to model",
		"do not have access to model",
		"not available for your account",
		"requires a newer version of codex",
		"requires newer version of codex",
		"requires newer codex",
		"invalid_request_error",
	} {
		if strings.Contains(msg, needle) {
			return false
		}
	}
	return true
}

func managerProviderCodexModelSmoke(record ManagedAgentRecord, executable string, args []string, env []string, model string) error {
	executable = strings.TrimSpace(executable)
	model = strings.TrimSpace(model)
	if executable == "" {
		return fmt.Errorf("codex bridge model smoke requires resolved executable")
	}
	if model == "" {
		return fmt.Errorf("codex bridge model smoke requires model")
	}
	ctx, cancel := context.WithTimeout(context.Background(), managerProviderModelSmokeTimeout)
	defer cancel()
	smokeArgs := append([]string(nil), args...)
	smokeArgs = append(smokeArgs,
		"exec",
		"--ephemeral",
		"--skip-git-repo-check",
		"--sandbox", "read-only",
		"-m", model,
		"Return exactly OK. Do not inspect files or call tools.",
	)
	cmd := managerProviderBridgeSmokeCommand(ctx, executable, smokeArgs)
	if workdir := strings.TrimSpace(record.Workdir); workdir != "" {
		cmd.Dir = workdir
	}
	cmd.Env = append([]string(nil), env...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return fmt.Errorf("codex bridge model smoke timed out for model %s", model)
	}
	if err != nil {
		return fmt.Errorf("codex bridge model smoke failed for model %s: %w: %s", model, err, truncate(strings.TrimSpace(string(output)), 500))
	}
	return nil
}

func managerProviderBridgeExecutableSmoke(record ManagedAgentRecord, executable string, args []string, env []string) error {
	executable = strings.TrimSpace(executable)
	if executable == "" {
		return fmt.Errorf("provider executable smoke requires resolved executable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), managerProviderSmokeTimeout)
	defer cancel()
	smokeArgs := append([]string(nil), args...)
	smokeArgs = append(smokeArgs, "--version")
	cmd := managerProviderBridgeSmokeCommand(ctx, executable, smokeArgs)
	if workdir := strings.TrimSpace(record.Workdir); workdir != "" {
		cmd.Dir = workdir
	}
	cmd.Env = append([]string(nil), env...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return fmt.Errorf("provider executable --version smoke timed out")
	}
	if err != nil {
		return fmt.Errorf("provider executable --version smoke failed: %w: %s", err, truncate(strings.TrimSpace(string(output)), 300))
	}
	return nil
}

func managerProviderBridgeSmokeCommand(ctx context.Context, executable string, args []string) *exec.Cmd {
	executable = strings.TrimSpace(executable)
	if goruntime.GOOS == "windows" {
		switch strings.ToLower(filepath.Ext(executable)) {
		case ".cmd", ".bat":
			shell := firstNonEmpty(os.Getenv("COMSPEC"), os.Getenv("ComSpec"), "cmd.exe")
			shellArgs := append([]string{"/C", executable}, args...)
			return exec.CommandContext(ctx, shell, shellArgs...)
		}
	}
	return exec.CommandContext(ctx, executable, args...)
}

func managerProviderBridgeFallbackExecutable(env map[string]string, name string, keys ...string) string {
	values := make([]string, 0, len(keys)+2)
	for _, key := range keys {
		values = append(values, env[key])
	}
	if resolved := managerLookPathInEnv(name, env); resolved != "" {
		values = append(values, resolved)
	}
	values = append(values, name)
	return firstNonEmpty(values...)
}

func managerEnvListMap(env []string) map[string]string {
	out := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		out[key] = value
		if goruntime.GOOS == "windows" {
			out[strings.ToUpper(key)] = value
		}
	}
	return out
}

func managerProviderExecutableStatusInEnv(path string, env map[string]string) (string, string, string) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "blocked", "provider executable not found in managed child env", ""
	}
	if executableFileExists(path) {
		return "ready", "provider executable exists", path
	}
	if resolved := managerLookPathInEnv(path, env); resolved != "" && executableFileExists(resolved) {
		return "ready", "provider executable resolved on managed child PATH", resolved
	}
	return "blocked", "provider executable does not exist in managed child env", path
}

func managerLookPathInEnv(name string, env map[string]string) string {
	name = strings.TrimSpace(name)
	if name == "" || filepath.Base(name) != name {
		return ""
	}
	pathValue := firstNonEmpty(env["PATH"], env["Path"], env["path"])
	if pathValue == "" {
		return ""
	}
	extensions := []string{""}
	if goruntime.GOOS == "windows" && filepath.Ext(name) == "" {
		pathext := firstNonEmpty(env["PATHEXT"], env["PathExt"], ".COM;.EXE;.BAT;.CMD")
		extensions = nil
		for _, ext := range strings.Split(pathext, ";") {
			ext = strings.TrimSpace(ext)
			if ext != "" {
				extensions = append(extensions, ext)
			}
		}
	}
	for _, dir := range filepath.SplitList(pathValue) {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		for _, ext := range extensions {
			candidate := filepath.Join(dir, name+ext)
			if executableFileExists(candidate) {
				return candidate
			}
		}
	}
	return ""
}

func managerToolBundleReadinessForRoster(registry BotRegistry) []managerAgentToolBundleReadiness {
	rows := make([]managerAgentToolBundleReadiness, 0, len(registry.Agents))
	defaults := registry.Defaults
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		row := managerAgentToolBundleReadiness{
			AgentID: record.AgentID,
			Workdir: record.Workdir,
		}
		anatomy, anatomyErr := managerSubstrateAnatomyForRecord(record, defaults)
		row.RequiredBundles = managedAgentToolBundlesForRecord(record, defaults, anatomy)
		if anatomyErr != nil {
			row.Status = "blocked"
			row.Reason = "anatomy_unreadable:" + anatomyErr.Error()
			rows = append(rows, row)
			continue
		}
		report := RegisterInstalledToolBundles(NewToolRegistry(), record.Workdir)
		row.Candidates = append([]ToolBundleReadinessItem(nil), report.Candidates...)
		row.Healthchecks = append([]ToolBundleDiscoveryDiagnostic(nil), report.Healthchecks...)
		row.Suites = toolBundleSuiteReadiness(anatomy, NewToolRegistry(), report.Candidates)
		row.Status, row.Reason = managerRequiredToolBundleStatus(row.RequiredBundles, report.Candidates)
		rows = append(rows, row)
	}
	return rows
}

func managerSubstrateAnatomyForRecord(record ManagedAgentRecord, defaults BotManagerDefaults) (AgentAnatomyConfig, error) {
	cfg := managedAgentEffectiveRuntimeConfig(record, managedAgentStartRuntimeConfig(record))
	profile := runtimeProfileForAnatomy(cfg)
	anatomyPath := firstNonEmpty(record.AnatomyPath, defaults.AnatomyPath)
	anatomyPreset := firstNonEmpty(record.AnatomyPreset, defaults.AnatomyPreset)
	switch {
	case strings.TrimSpace(anatomyPath) != "":
		return ReadAgentAnatomyConfigFile(anatomyPath, profile)
	case strings.TrimSpace(anatomyPreset) != "":
		return DefaultAgentAnatomyConfigForPreset(profile, anatomyPreset), nil
	case pathExists(agentAnatomyPath(record.Workdir)):
		return ReadAgentAnatomyConfig(record.Workdir, profile)
	default:
		return DefaultAgentAnatomyConfig(profile), nil
	}
}

func managerRequiredToolBundleStatus(required []string, candidates []ToolBundleReadinessItem) (string, string) {
	required = normalizeManagedToolBundleList(required)
	if len(required) == 0 {
		return "not_required", "no managed tool bundles required by roster/anatomy"
	}
	byName := map[string]ToolBundleReadinessItem{}
	for _, candidate := range candidates {
		byName[strings.TrimSpace(candidate.Name)] = candidate
	}
	blocked := []string{}
	for _, name := range required {
		candidate, ok := byName[name]
		if !ok {
			blocked = append(blocked, name+":missing")
			continue
		}
		if strings.TrimSpace(candidate.Status) != "ready" && strings.TrimSpace(candidate.Status) != "ready_with_warnings" {
			blocked = append(blocked, name+":"+firstNonEmpty(candidate.Status, "unknown"))
		}
	}
	if len(blocked) > 0 {
		sort.Strings(blocked)
		return "blocked", strings.Join(blocked, ", ")
	}
	return "ready", "required managed tool bundles are installed"
}

func managerBrowserReadinessFromToolBundles(rows []managerAgentToolBundleReadiness) managerBrowserReadiness {
	agents := []string{}
	blocked := []string{}
	for _, row := range rows {
		needsBrowser := false
		needsVisualProbeSmoke := false
		for _, bundle := range row.RequiredBundles {
			switch strings.TrimSpace(bundle) {
			case "browser_session":
				needsBrowser = true
			case "browser_visual_probe":
				needsBrowser = true
				needsVisualProbeSmoke = true
			}
		}
		if !needsBrowser {
			continue
		}
		agentID := firstNonEmpty(row.AgentID, row.Workdir, "unknown")
		agents = append(agents, agentID)
		if row.Status != "ready" {
			blocked = append(blocked, agentID+":"+firstNonEmpty(row.Reason, row.Status))
			continue
		}
		if needsVisualProbeSmoke && !managerToolBundleHealthcheckPassed(row.Healthchecks, "browser_visual_probe") {
			blocked = append(blocked, agentID+":browser_visual_probe smoke missing")
		}
	}
	agents = uniqueTrimmedCSVStrings(agents)
	if len(agents) == 0 {
		return managerBrowserReadiness{Status: "not_required", Reason: "no roster agent requires browser tool bundles"}
	}
	if len(blocked) > 0 {
		sort.Strings(blocked)
		return managerBrowserReadiness{Status: "blocked", Reason: strings.Join(blocked, "; "), Agents: agents}
	}
	return managerBrowserReadiness{Status: "ready", Reason: "required browser tool bundles passed executable smoke", Agents: agents}
}

func managerToolBundleHealthcheckPassed(healthchecks []ToolBundleDiscoveryDiagnostic, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, diagnostic := range healthchecks {
		if strings.TrimSpace(diagnostic.Name) == name && strings.TrimSpace(diagnostic.Code) == "healthcheck_passed" {
			return true
		}
	}
	return false
}

func managerCleanStopReadinessForRoster(registry BotRegistry) managerCleanStopReadiness {
	blocked := []string{}
	agents := []string{}
	type remoteResult struct {
		blocked []string
		agents  []string
	}
	remoteResults := make(chan remoteResult, len(registry.Agents))
	remoteCount := 0
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		agentID := firstNonEmpty(record.AgentID, record.Workdir, "unknown")
		if managedAgentGracefulStopRequested(record.Workdir) {
			agents = append(agents, agentID)
			blocked = append(blocked, agentID+":stop_request_residual")
		}
		root, ok, err := managedAgentBrowserSessionRoot(record.Workdir)
		if err != nil {
			agents = append(agents, agentID)
			blocked = append(blocked, agentID+":browser_session_root_error:"+err.Error())
			continue
		}
		if ok && pathExists(root) {
			agents = append(agents, agentID)
			blocked = append(blocked, agentID+":browser_session_residual")
		}
		localCheckoutBlocked := managerLocalProjectCheckoutResidualsForAgent(record)
		if len(localCheckoutBlocked) > 0 {
			blocked = append(blocked, localCheckoutBlocked...)
			agents = append(agents, agentID)
		}
		remoteCount++
		go func(record ManagedAgentRecord) {
			remoteBlocked, remoteAgents := managerRemoteCleanStopBlockersFunc(record)
			remoteResults <- remoteResult{blocked: remoteBlocked, agents: remoteAgents}
		}(record)
	}
	for i := 0; i < remoteCount; i++ {
		result := <-remoteResults
		if len(result.blocked) == 0 {
			continue
		}
		blocked = append(blocked, result.blocked...)
		agents = append(agents, result.agents...)
	}
	return managerCleanStopReadinessFromBlockers(blocked, agents)
}

func managerCleanStopReadinessFromBlockers(blocked []string, agents []string) managerCleanStopReadiness {
	if len(blocked) == 0 {
		return managerCleanStopReadiness{Status: "ready", Reason: "no local clean-stop residuals detected"}
	}
	hard := []string{}
	soft := []string{}
	for _, blocker := range blocked {
		blocker = strings.TrimSpace(blocker)
		if blocker == "" {
			continue
		}
		if managerCleanStopRemoteCheckFailureIsTransient(blocker) {
			soft = append(soft, blocker)
			continue
		}
		hard = append(hard, blocker)
	}
	if len(hard) > 0 {
		sort.Strings(hard)
		return managerCleanStopReadiness{
			Status: "blocked",
			Reason: strings.Join(hard, "; "),
			Agents: uniqueTrimmedCSVStrings(agents),
		}
	}
	sort.Strings(soft)
	return managerCleanStopReadiness{
		Status: "degraded",
		Reason: strings.Join(soft, "; "),
		Agents: uniqueTrimmedCSVStrings(agents),
	}
}

func managerCleanStopRemoteCheckFailureIsTransient(blocker string) bool {
	parts := strings.SplitN(strings.TrimSpace(blocker), ":", 3)
	if len(parts) < 3 {
		return false
	}
	reason := strings.ToLower(strings.TrimSpace(parts[1]))
	if !strings.Contains(reason, "check_failed") {
		return false
	}
	detail := strings.TrimSpace(parts[2])
	if detail == "" {
		return false
	}
	return managerRPCContractTransientError(errors.New(detail))
}

func managerRemoteCleanStopBlockersForAgent(record ManagedAgentRecord) ([]string, []string) {
	record = normalizeManagedAgentRecord(record)
	agentID := strings.TrimSpace(record.AgentID)
	if agentID == "" {
		return nil, nil
	}
	cfg := managedAgentStartRuntimeConfig(record)
	workspaceID := strings.TrimSpace(cfg.WorkspaceID)
	endpoint := strings.TrimSpace(cfg.RhizomeRPC)
	if endpoint == "" && strings.TrimSpace(cfg.RhizomeHost) != "" {
		endpoint = defaultRPCEndpoint(cfg.RhizomeHost)
	}
	if workspaceID == "" {
		return []string{agentID + ":remote_clean_stop_workspace_unavailable"}, []string{agentID}
	}
	if strings.TrimSpace(cfg.RhizomeToken) == "" {
		if managerRPCContractAllowLocalOnlyOverride() {
			return nil, nil
		}
		return []string{agentID + ":remote_clean_stop_token_unavailable"}, []string{agentID}
	}
	if endpoint == "" {
		return []string{agentID + ":remote_clean_stop_rpc_endpoint_unavailable"}, []string{agentID}
	}
	if managerRPCContractLocalEndpoint(endpoint) && !managerRPCContractSmokeLocalEndpoints() && !managerRPCContractAllowLocalOnlyOverride() {
		return []string{agentID + ":remote_clean_stop_local_rpc_smoke_disabled"}, []string{agentID}
	}

	ctx, cancel := context.WithTimeout(context.Background(), managerRPCContractSmokeTimeout)
	defer cancel()
	client := NewRhizomeClient(endpoint, cfg.RhizomeToken)
	blocked := []string{}
	projectIDs := map[string]struct{}{}
	taskProjectRefs := map[string][]managerRemoteTaskProjectRef{}
	var sessions []AgentSessionStateRecord
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.sessions", func(ctx context.Context) error {
		var err error
		sessions, err = client.ListSessions(ctx, workspaceID, true, 500)
		return err
	}); err != nil {
		blocked = append(blocked, agentID+":remote_active_session_check_failed:"+err.Error())
	} else {
		for _, session := range sessions {
			if strings.TrimSpace(session.AgentID) != agentID {
				continue
			}
			blocked = append(blocked, agentID+":remote_active_session:"+strings.TrimSpace(session.SessionID))
		}
	}
	var tasks []WorkspaceTaskRecord
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.tasks", func(ctx context.Context) error {
		var err error
		tasks, err = client.ListTasks(ctx, workspaceID)
		return err
	}); err != nil {
		blocked = append(blocked, agentID+":remote_claim_check_failed:"+err.Error())
	} else {
		for _, task := range tasks {
			if projectID := strings.TrimSpace(task.ProjectID); projectID != "" {
				ref := managerRemoteTaskProjectRefFromTask(task, projectID)
				taskProjectRefs[projectID] = append(taskProjectRefs[projectID], ref)
			}
			if strings.TrimSpace(pointerValue(task.ClaimAgentID)) != agentID {
				continue
			}
			if taskClaimStatusIsActiveOwnership(taskClaimStatus(task)) {
				blocked = append(blocked, agentID+":remote_active_claim:"+strings.TrimSpace(task.TaskID))
			}
		}
	}
	var projects []ProjectRecord
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.projects", func(ctx context.Context) error {
		var err error
		projects, err = client.ListProjects(ctx, workspaceID)
		return err
	}); err != nil {
		blocked = append(blocked, agentID+":remote_project_list_check_failed:"+err.Error())
		for projectID := range taskProjectRefs {
			projectIDs[projectID] = struct{}{}
		}
	} else {
		knownProjectIDs := map[string]struct{}{}
		for _, project := range projects {
			projectID := strings.TrimSpace(project.ProjectID)
			if projectID == "" {
				continue
			}
			knownProjectIDs[projectID] = struct{}{}
			if !managerProjectStatusTerminal(project.Status) {
				projectIDs[projectID] = struct{}{}
			}
		}
		for projectID, refs := range taskProjectRefs {
			if _, ok := knownProjectIDs[projectID]; ok {
				projectIDs[projectID] = struct{}{}
				continue
			}
			for _, ref := range refs {
				blocked = append(blocked, agentID+":"+managerRemoteTaskUnknownProjectBlocker(ref))
			}
		}
	}
	if len(projectIDs) > 0 {
		orderedProjectIDs := sortedManagerProjectIDs(projectIDs)
		blocked = append(blocked, managerRemoteProjectCoordinationResiduals(ctx, client, workspaceID, agentID, orderedProjectIDs)...)
		blocked = append(blocked, managerRemotePatchQueueIntegrationReconcileBlockers(ctx, client, workspaceID, agentID, orderedProjectIDs)...)
	}
	var requests []AgentRequestRecord
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.open_requests", func(ctx context.Context) error {
		var err error
		requests, err = client.ListOpenRequests(ctx, workspaceID, agentID, 100)
		return err
	}); err != nil {
		blocked = append(blocked, agentID+":remote_agent_request_open_check_failed:"+err.Error())
	} else {
		for _, request := range requests {
			requestID := strings.TrimSpace(request.RequestID)
			if requestID == "" {
				requestID = strings.TrimSpace(request.Method)
			}
			status := strings.TrimSpace(request.Status)
			if !managerCleanStopAgentRequestStatusBlocks(status) {
				continue
			}
			blocked = append(blocked, agentID+":remote_open_agent_request:"+requestID+":"+status)
		}
	}
	var scratch string
	var scratchOK bool
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.pending_trigger", func(ctx context.Context) error {
		var err error
		scratch, scratchOK, err = client.StateGet(ctx, workspaceID, agentID, runtimeScratchStateKey)
		return err
	}); err != nil {
		blocked = append(blocked, agentID+":remote_pending_trigger_check_failed:"+err.Error())
	} else if scratchOK {
		var state RuntimeScratchState
		if err := json.Unmarshal([]byte(strings.TrimSpace(scratch)), &state); err != nil {
			blocked = append(blocked, agentID+":remote_pending_trigger_decode_failed:"+err.Error())
		} else if strings.TrimSpace(state.PendingTrigger) != "" {
			blocked = append(blocked, agentID+":remote_pending_trigger:"+strings.TrimSpace(state.PendingTrigger))
		} else if binding := managerScratchActiveBindingResidual(state); binding != "" {
			blocked = append(blocked, agentID+":remote_scratch_active_binding:"+binding)
		}
	}
	local := LoadLocalRuntimeProfile(record.Workdir)
	if managedAgentBudgetCleanupConfigured(local) {
		var reservations []BudgetReservationRecord
		if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.budget_reservations", func(ctx context.Context) error {
			var err error
			reservations, err = listManagedAgentOpenBudgetReservationsForStop(ctx, client, local, workspaceID, agentID)
			return err
		}); err != nil {
			blocked = append(blocked, agentID+":remote_budget_reservation_check_failed:"+err.Error())
		} else {
			for _, reservation := range reservations {
				blocked = append(blocked, agentID+":remote_open_budget_reservation:"+strings.TrimSpace(reservation.ReservationID))
			}
		}
	}
	if len(blocked) == 0 {
		return nil, nil
	}
	sort.Strings(blocked)
	return blocked, []string{agentID}
}

func managerCleanStopAgentRequestStatusBlocks(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "PENDING", "PROCESSING":
		return true
	case "TIMEOUT", "COMPLETED", "FAILED", "CANCELLED", "CANCELED", "REJECTED":
		return false
	default:
		return true
	}
}

type managerRemoteTaskProjectRef struct {
	TaskID              string
	ProjectID           string
	Status              string
	TaskKind            string
	ProjectLane         string
	Title               string
	Description         string
	Tags                []string
	RequiresProjectGate bool
	ClaimAgentID        string
	ClaimStatus         string
}

func managerRemoteTaskProjectRefFromTask(task WorkspaceTaskRecord, projectID string) managerRemoteTaskProjectRef {
	requiresGate := false
	if task.RequiresProjectGate != nil {
		requiresGate = *task.RequiresProjectGate
	}
	return managerRemoteTaskProjectRef{
		TaskID:              strings.TrimSpace(task.TaskID),
		ProjectID:           strings.TrimSpace(projectID),
		Status:              strings.TrimSpace(task.Status),
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		Title:               strings.TrimSpace(task.Title),
		Description:         strings.TrimSpace(task.Description),
		Tags:                uniqueTrimmedCSVStrings(task.Tags),
		RequiresProjectGate: requiresGate,
		ClaimAgentID:        strings.TrimSpace(pointerValue(task.ClaimAgentID)),
		ClaimStatus:         strings.TrimSpace(taskClaimStatus(task)),
	}
}

func managerRemoteTaskUnknownProjectBlocker(ref managerRemoteTaskProjectRef) string {
	if managerRemoteTaskLooksBootstrapFutureProject(ref) {
		return "remote_bootstrap_task_future_project_id:" + strings.TrimSpace(ref.TaskID) + ":" + strings.TrimSpace(ref.ProjectID)
	}
	return "remote_task_unknown_project_id:" + strings.TrimSpace(ref.TaskID) + ":" + strings.TrimSpace(ref.ProjectID)
}

func managerRemoteTaskLooksBootstrapFutureProject(ref managerRemoteTaskProjectRef) bool {
	if !strings.EqualFold(strings.TrimSpace(ref.Status), "PENDING") {
		return false
	}
	if taskClaimStatusIsActiveOwnership(ref.ClaimStatus) || strings.TrimSpace(ref.ClaimAgentID) != "" {
		return false
	}
	if ref.RequiresProjectGate {
		return false
	}
	kind := strings.ToUpper(strings.TrimSpace(ref.TaskKind))
	if kind != "" && kind != "COORDINATION" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(ref.ProjectLane))
	if lane != "" && lane != "strategy" && lane != "coordination" && lane != "planning" {
		return false
	}
	text := strings.ToLower(strings.Join(append([]string{
		ref.TaskID,
		ref.Title,
		ref.Description,
		ref.ProjectID,
	}, ref.Tags...), "\n"))
	return strings.Contains(text, "root") ||
		strings.Contains(text, "bootstrap") ||
		strings.Contains(text, "operator") ||
		strings.Contains(text, "signal")
}

func managerScratchActiveBindingResidual(state RuntimeScratchState) string {
	parts := []string{}
	if value := strings.TrimSpace(state.ActiveTaskID); value != "" {
		parts = append(parts, "task="+value)
	}
	if value := strings.TrimSpace(state.ActiveSessionID); value != "" {
		parts = append(parts, "session="+value)
	}
	if value := strings.TrimSpace(state.ActiveRunID); value != "" {
		parts = append(parts, "run="+value)
	}
	return strings.Join(parts, ",")
}

func sortedManagerProjectIDs(projectIDs map[string]struct{}) []string {
	ordered := make([]string, 0, len(projectIDs))
	for projectID := range projectIDs {
		if projectID = strings.TrimSpace(projectID); projectID != "" {
			ordered = append(ordered, projectID)
		}
	}
	sort.Strings(ordered)
	return ordered
}

func managerProjectStatusTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ARCHIVED", "CLOSED", "COMPLETED", "CANCELED", "CANCELLED":
		return true
	default:
		return false
	}
}

func managerRemoteProjectCoordinationResiduals(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, projectIDs []string) []string {
	if len(projectIDs) == 0 {
		return nil
	}
	blocked := []string{}
	for _, projectID := range projectIDs {
		var coordination ProjectCoordinationRecord
		if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.project_coordination", func(ctx context.Context) error {
			var err error
			coordination, err = client.GetProjectCoordination(ctx, workspaceID, projectID)
			return err
		}); err != nil {
			blocked = append(blocked, agentID+":remote_project_coordination_check_failed:"+projectID+":"+err.Error())
			continue
		}
		for _, item := range coordination.PatchQueueItems {
			if projectPatchQueueStateIsTerminal(item.State) {
				continue
			}
			itemRef := strings.TrimSpace(item.QueueID) + "/" + strings.TrimSpace(item.ItemID)
			claimedBy := strings.TrimSpace(item.ClaimedBy)
			switch {
			case claimedBy == agentID && strings.TrimSpace(item.ClaimToken) != "":
				blocked = append(blocked, agentID+":remote_patch_queue_claim:"+itemRef+":"+strings.TrimSpace(item.State))
			case claimedBy != "":
				blocked = append(blocked, agentID+":remote_patch_queue_live_claimed_by:"+itemRef+":"+strings.TrimSpace(item.State)+":"+claimedBy)
			default:
				blocked = append(blocked, agentID+":remote_patch_queue_live:"+itemRef+":"+strings.TrimSpace(item.State))
			}
		}
		for _, checkout := range coordination.Checkouts {
			if managerProjectCheckoutStatusTerminal(checkout.Status) {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(checkout.DirtyState), "dirty") {
				blocked = append(blocked, agentID+":remote_dirty_project_checkout:"+firstNonEmpty(strings.TrimSpace(checkout.CheckoutID), strings.TrimSpace(checkout.LocalPath)))
			}
			if strings.TrimSpace(checkout.ActiveTaskID) != "" || strings.TrimSpace(checkout.ActiveClaimID) != "" {
				blocked = append(blocked, agentID+":remote_active_project_checkout:"+firstNonEmpty(strings.TrimSpace(checkout.CheckoutID), strings.TrimSpace(checkout.LocalPath)))
			}
		}
		for _, branch := range coordination.Branches {
			if projectCoordinationBranchStatusTerminal(branch.Status) {
				continue
			}
			if !managerProjectBranchBlocksCleanStop(branch) {
				continue
			}
			branchRef := firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName))
			if branchRef == "" {
				continue
			}
			parts := []string{branchRef, firstNonEmpty(strings.TrimSpace(branch.Status), "UNKNOWN")}
			if owner := strings.TrimSpace(branch.AgentID); owner != "" {
				parts = append(parts, owner)
			}
			if taskID := strings.TrimSpace(branch.ActiveTaskID); taskID != "" {
				parts = append(parts, "task="+taskID)
			}
			if claimID := strings.TrimSpace(branch.ActiveClaimID); claimID != "" {
				parts = append(parts, "claim="+claimID)
			}
			blocked = append(blocked, agentID+":remote_project_branch_live:"+strings.Join(parts, ":"))
		}
	}
	return blocked
}

func managerProjectBranchBlocksCleanStop(branch ProjectBranchRecord) bool {
	if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != "" {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "RESERVED", "ACTIVE", "BLOCKED":
		return true
	default:
		return false
	}
}

func managerRemotePatchQueueIntegrationReconcileBlockers(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, projectIDs []string) []string {
	if client == nil || len(projectIDs) == 0 {
		return nil
	}
	projectSet := map[string]struct{}{}
	for _, projectID := range projectIDs {
		if projectID = strings.TrimSpace(projectID); projectID != "" {
			projectSet[projectID] = struct{}{}
		}
	}
	if len(projectSet) == 0 {
		return nil
	}
	acceptedItems := map[string]ProjectPatchQueueItemRecord{}
	for _, projectID := range projectIDs {
		var coordination ProjectCoordinationRecord
		if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.integration_coordination", func(ctx context.Context) error {
			var err error
			coordination, err = client.GetProjectCoordination(ctx, workspaceID, projectID)
			return err
		}); err != nil {
			return []string{agentID + ":remote_integration_reconcile_coordination_check_failed:" + strings.TrimSpace(projectID) + ":" + err.Error()}
		}
		for _, item := range coordination.PatchQueueItems {
			if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
				continue
			}
			entityID := managerPatchQueueIntegrationEntityID(item.QueueID, item.ItemID)
			if entityID == "" {
				continue
			}
			acceptedItems[entityID] = item
		}
	}
	if len(acceptedItems) == 0 {
		return nil
	}
	var admissions []RuntimeEventRecord
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.integration_admissions", func(ctx context.Context) error {
		var err error
		admissions, err = client.ListRuntimeEvents(ctx, RuntimeEventListInput{
			WorkspaceID: workspaceID,
			EventType:   managerPatchQueueIntegrationAdmittedEventType,
			EntityType:  managerPatchQueueIntegrationEntityType,
			Limit:       500,
		})
		return err
	}); err != nil {
		return []string{agentID + ":remote_integration_reconcile_admission_check_failed:" + err.Error()}
	}
	var integrated []RuntimeEventRecord
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.integration_integrated", func(ctx context.Context) error {
		var err error
		integrated, err = client.ListRuntimeEvents(ctx, RuntimeEventListInput{
			WorkspaceID: workspaceID,
			EventType:   managerPatchQueueIntegratedEventType,
			EntityType:  managerPatchQueueIntegrationEntityType,
			Limit:       500,
		})
		return err
	}); err != nil {
		return []string{agentID + ":remote_integration_reconcile_integrated_check_failed:" + err.Error()}
	}
	var repairs []RuntimeEventRecord
	if err := managerSubstrateRPCWithRetry(ctx, "clean_stop.integration_repairs", func(ctx context.Context) error {
		var err error
		repairs, err = client.ListRuntimeEvents(ctx, RuntimeEventListInput{
			WorkspaceID: workspaceID,
			EventType:   managerPatchQueueIntegrationRepairEventType,
			EntityType:  managerPatchQueueIntegrationEntityType,
			Limit:       500,
		})
		return err
	}); err != nil {
		return []string{agentID + ":remote_integration_reconcile_repair_check_failed:" + err.Error()}
	}
	latestAdmission := managerLatestRuntimeEventByEntity(admissions)
	latestOutcome := managerLatestRuntimeEventByEntity(append(integrated, repairs...))
	blocked := []string{}
	for entityID, admission := range latestAdmission {
		item, ok := acceptedItems[entityID]
		if !ok {
			continue
		}
		if _, ok := projectSet[strings.TrimSpace(item.ProjectID)]; !ok {
			continue
		}
		if outcome, ok := latestOutcome[entityID]; ok && !managerRuntimeEventAfter(admission, outcome) {
			continue
		}
		targetBranch := firstNonEmpty(managerRuntimeEventPayloadString(admission, "target_branch"), "unknown_target")
		blocked = append(blocked, agentID+":remote_patch_queue_integration_unreconciled:"+strings.TrimSpace(item.ProjectID)+":"+entityID+":"+targetBranch)
	}
	if len(blocked) == 0 {
		return nil
	}
	sort.Strings(blocked)
	return blocked
}

func managerPatchQueueIntegrationEntityID(queueID, itemID string) string {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	if queueID == "" || itemID == "" {
		return ""
	}
	return queueID + "/" + itemID
}

func managerLatestRuntimeEventByEntity(events []RuntimeEventRecord) map[string]RuntimeEventRecord {
	latest := map[string]RuntimeEventRecord{}
	for _, event := range events {
		entityID := strings.TrimSpace(event.EntityID)
		if entityID == "" {
			continue
		}
		current, ok := latest[entityID]
		if !ok || managerRuntimeEventAfter(event, current) {
			latest[entityID] = event
		}
	}
	return latest
}

func managerRuntimeEventAfter(left, right RuntimeEventRecord) bool {
	if strings.TrimSpace(right.EventID) == "" {
		return true
	}
	if left.IngestSeq != 0 || right.IngestSeq != 0 {
		if left.IngestSeq != right.IngestSeq {
			return left.IngestSeq > right.IngestSeq
		}
	}
	leftAt, leftErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(left.CreatedAt))
	rightAt, rightErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(right.CreatedAt))
	if leftErr == nil && rightErr == nil && !leftAt.Equal(rightAt) {
		return leftAt.After(rightAt)
	}
	return strings.TrimSpace(left.EventID) > strings.TrimSpace(right.EventID)
}

func managerRuntimeEventPayloadString(event RuntimeEventRecord, key string) string {
	key = strings.TrimSpace(key)
	if key == "" || strings.TrimSpace(event.PayloadJSON) == "" {
		return ""
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return ""
	}
	value, ok := payload[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func managerProjectCheckoutStatusTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ABANDONED", "ARCHIVED", "STALE":
		return true
	default:
		return false
	}
}

func managerLocalProjectCheckoutResidualsForAgent(record ManagedAgentRecord) []string {
	record = normalizeManagedAgentRecord(record)
	agentID := strings.TrimSpace(record.AgentID)
	workdir := strings.TrimSpace(record.Workdir)
	if agentID == "" || workdir == "" {
		return nil
	}
	root := filepath.Join(workdir, "project-checkouts")
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return []string{agentID + ":local_project_checkout_scan_failed:" + err.Error()}
	}
	blocked := []string{}
	seenRoots := map[string]struct{}{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		checkoutPath := filepath.Join(root, entry.Name())
		repoRoot, err := managerSubstrateRepositoryCommandOutput(checkoutPath, "git", "-C", checkoutPath, "rev-parse", "--show-toplevel")
		if err == nil {
			repoRoot = strings.TrimSpace(repoRoot)
			if repoRoot != "" {
				if _, ok := seenRoots[repoRoot]; ok {
					continue
				}
				seenRoots[repoRoot] = struct{}{}
			}
		}
		status, err := managerSubstrateRepositoryCommandOutput(checkoutPath, "git", "-C", checkoutPath, "status", "--porcelain")
		if err != nil {
			if _, statErr := os.Stat(filepath.Join(checkoutPath, ".git")); statErr != nil {
				continue
			}
			blocked = append(blocked, agentID+":local_project_checkout_inspect_failed:"+entry.Name()+":"+err.Error())
			continue
		}
		if strings.TrimSpace(status) != "" {
			blocked = append(blocked, agentID+":local_dirty_project_checkout:"+entry.Name())
		}
	}
	return blocked
}

const managerRPCContractSmokeMaxAttempts = 5

var (
	managerRPCContractSmokeTimeout    = 60 * time.Second
	managerRPCContractCallTimeout     = 10 * time.Second
	managerRPCContractSmokeRetryDelay = time.Second
)

var managerRPCContractMethodMatrix = []string{
	"rpc.methods.list",
	"rpc.describe",
	"runtime.build.info",
	"workspace.sessions.list",
	"workspace.tasks.list",
	"agent.work.next",
	"agent.task.hydrate",
	"agent.task.claim",
	"agent.task.release",
	"agent.task.complete",
	"agent.task.block",
	"agent.task_frontier.decision",
	"agent.session.start",
	"agent.session.status",
	"agent.session.blocked",
	"agent.session.decision_needed",
	"agent.session.keepalive",
	"agent.session.end",
	"agent.state.get",
	"agent.state.set",
	"agent.request",
	"agent.respond",
	"agent.request.result",
	"agent.request.list",
	"agent.request.open.list",
	"workspace.execution.run.write",
	"workspace.execution.agent_runs.cancel",
	"tool.call",
	"task.submit",
	"task.project_fields.put",
	"task.close",
	"project.list",
	"project.profile.update",
	"project.phase.transition",
	"project.role.assign",
	"project.repository.upsert",
	"project.repositories.list",
	"project.coordination.get",
	"project.governance.predicates.check",
	"project.governance.challenge.raise",
	"project.governance.challenge.defend",
	"project.governance.vote.cast",
	"project.governance.challenge.tally",
	"project.governance.challenge.get",
	"project.governance.challenge.list",
	"project.governance.votes.list",
	"project.checkout.register",
	"project.branch.register",
	"project.branches.list",
	"project.patch_queue.list",
	"project.patch_queue.submit",
	"project.patch_queue.supersede",
	"project.patch_queue.claim",
	"project.patch_queue.operation_bind",
	"project.patch_queue.cas_record",
	"project.patch_queue.materialization_record",
	"project.patch_queue.rollback_record",
	"project.patch_queue.reviewer_advisory_record",
	"project.patch_queue.operator_enablement_record",
	"project.patch_queue.integration_record",
	"project.patch_queue.integration_repair",
	"project.patch_queue.release",
	"project.patch_queue.decision",
	"project.patch_queue.review_task.reconcile",
	"project.patch_queue.decision_continuation.consume",
	"budget.reservations.list",
}

type managerRPCParamRequirement struct {
	Required bool
	Enum     []string
}

type managerRPCMethodSchemaRequirement struct {
	Params map[string]managerRPCParamRequirement
}

var managerRPCContractSchemaRequirements = map[string]managerRPCMethodSchemaRequirement{
	"runtime.build.info": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
	}},
	"agent.respond": {Params: map[string]managerRPCParamRequirement{
		"request_id": {Required: true},
		"response":   {Required: true},
	}},
	"agent.request.open.list": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
		"agent_id":     {Required: true},
		"limit":        {},
	}},
	"agent.task_frontier.decision": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":           {Required: true},
		"agent_id":               {Required: true},
		"frontier_generation_id": {Required: true},
		"decision_state":         {Required: true, Enum: []string{"selected", "declined", "model_failed", "hydration_failed", "claim_failed", "admission_failed"}},
		"selected_task_id":       {},
		"summary":                {},
	}},
	"project.checkout.register": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":    {Required: true},
		"project_id":      {Required: true},
		"actor_id":        {Required: true},
		"repo_id":         {Required: true},
		"machine_id":      {Required: true},
		"local_path":      {Required: true},
		"active_task_id":  {},
		"active_claim_id": {},
		"dirty_state":     {Enum: []string{"clean", "dirty", "unknown"}},
		"status":          {Enum: []string{"ACTIVE", "STALE", "BLOCKED", "ABANDONED", "ARCHIVED"}},
	}},
	"project.branch.register": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":    {Required: true},
		"project_id":      {Required: true},
		"actor_id":        {Required: true},
		"repo_id":         {Required: true},
		"branch_name":     {Required: true},
		"active_task_id":  {},
		"active_claim_id": {},
		"status":          {Enum: []string{"RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW", "MERGED", "ABANDONED", "ARCHIVED"}},
	}},
	"project.patch_queue.submit": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":               {Required: true},
		"project_id":                 {Required: true},
		"actor_id":                   {Required: true},
		"repo_id":                    {Required: true},
		"branch_id":                  {Required: true},
		"repo_authority_mode":        {Enum: []string{"repoauthority_controlled_queue"}},
		"task_id":                    {Required: true},
		"session_id":                 {Required: true},
		"run_id":                     {Required: true},
		"agent_id":                   {Required: true},
		"capability_snapshot_id":     {Required: true},
		"capability_snapshot_schema": {Required: true},
		"repo_root":                  {Required: true},
		"base_tree_hash":             {Required: true},
		"repo_lease_id":              {Required: true},
		"lease_term":                 {Required: true},
	}},
	"workspace.execution.agent_runs.cancel": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
		"agent_id":     {Required: true},
		"summary":      {},
		"outcome":      {},
	}},
	"project.governance.predicates.check": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":        {Required: true},
		"project_id":          {Required: true},
		"challenged_agent_id": {Required: true},
		"stall_predicates":    {Enum: []string{"fanout_absent", "idle_roster"}},
	}},
	"project.governance.challenge.raise": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":                 {Required: true},
		"project_id":                   {Required: true},
		"actor_id":                     {Required: true},
		"challenged_agent_id":          {Required: true},
		"challenger_agent_id":          {Required: true},
		"nominated_successor_agent_id": {},
		"stall_predicates":             {Enum: []string{"fanout_absent", "idle_roster"}},
		"evidence_refs":                {},
		"argument_doc_key":             {},
		"tension_id":                   {},
		"defense_window_seconds":       {},
		"voting_window_seconds":        {},
		"max_rounds":                   {},
	}},
	"project.governance.challenge.defend": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":          {Required: true},
		"actor_id":              {Required: true},
		"challenge_id":          {Required: true},
		"round":                 {},
		"stance":                {Required: true, Enum: []string{"DEFEND", "CONCEDE"}},
		"defense_doc_key":       {},
		"voting_window_seconds": {},
	}},
	"project.governance.vote.cast": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":      {Required: true},
		"actor_id":          {Required: true},
		"challenge_id":      {Required: true},
		"round":             {},
		"voter_agent_id":    {Required: true},
		"ballot":            {Required: true, Enum: []string{"UPHOLD_LEAD", "REASSIGN", "ABSTAIN"}},
		"rationale_doc_key": {},
	}},
	"project.governance.challenge.tally": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":       {Required: true},
		"actor_id":           {Required: true},
		"challenge_id":       {Required: true},
		"reassign_enabled":   {},
		"lead_lease_seconds": {},
	}},
	"project.governance.challenge.get": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":  {Required: true},
		"challenge_id":  {Required: true},
		"include_votes": {},
	}},
	"project.governance.challenge.list": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
		"project_id":   {},
		"state":        {Enum: []string{"RAISED", "DEFENSE_OPEN", "VOTING", "NEGOTIATION", "RESOLVED_UPHELD", "RESOLVED_REASSIGNED", "RESOLVED_DEFAULT", "AUTO_WITHDRAWN"}},
		"limit":        {},
	}},
	"project.governance.votes.list": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
		"challenge_id": {Required: true},
		"round":        {},
	}},
	"project.patch_queue.claim": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
		"project_id":   {Required: true},
		"actor_id":     {Required: true},
		"queue_id":     {Required: true},
		"item_id":      {Required: true},
		"claim_token":  {},
	}},
	"project.patch_queue.release": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
		"project_id":   {Required: true},
		"actor_id":     {Required: true},
		"queue_id":     {Required: true},
		"item_id":      {Required: true},
		"claim_token":  {Required: true},
	}},
	"project.patch_queue.decision": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":     {Required: true},
		"project_id":       {Required: true},
		"actor_id":         {Required: true},
		"queue_id":         {Required: true},
		"item_id":          {Required: true},
		"decision":         {Required: true, Enum: []string{"ACCEPTED", "REJECTED", "BLOCKED", "CANCELED"}},
		"decision_summary": {Required: true},
		"claim_token":      {Required: true},
	}},
	"project.patch_queue.rollback_record": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":      {Required: true},
		"project_id":        {Required: true},
		"actor_id":          {Required: true},
		"queue_id":          {Required: true},
		"item_id":           {Required: true},
		"rollback_evidence": {Required: true},
		"claim_token":       {Required: true},
	}},
	"project.patch_queue.integration_record": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":             {Required: true},
		"project_id":               {Required: true},
		"actor_id":                 {Required: true},
		"queue_id":                 {Required: true},
		"item_id":                  {Required: true},
		"outcome":                  {Required: true, Enum: []string{"admitted", "integrated", "repair"}},
		"integration_mode":         {Enum: []string{"materialization", "direct_merge"}},
		"authority_mode":           {Enum: []string{"project_role", "trust_first_advisory"}},
		"target_branch":            {Required: true},
		"target_head_after":        {},
		"remote_target_head_after": {},
		"repair_reason":            {},
	}},
	"project.patch_queue.integration_repair": {Params: map[string]managerRPCParamRequirement{
		"workspace_id":             {Required: true},
		"project_id":               {Required: true},
		"actor_id":                 {Required: true},
		"queue_id":                 {Required: true},
		"item_id":                  {Required: true},
		"outcome":                  {Enum: []string{"repair"}},
		"integration_mode":         {Enum: []string{"materialization", "direct_merge"}},
		"authority_mode":           {Enum: []string{"project_role", "trust_first_advisory"}},
		"target_head_after":        {},
		"remote_target_head_after": {},
		"repair_reason":            {Required: true},
	}},
	"project.patch_queue.decision_continuation.consume": {Params: map[string]managerRPCParamRequirement{
		"workspace_id": {Required: true},
		"project_id":   {Required: true},
		"actor_id":     {Required: true},
		"outbox_id":    {},
		"queue_id":     {},
		"item_id":      {},
	}},
	"tool.call": {Params: map[string]managerRPCParamRequirement{
		"tool_id":              {Required: true},
		"workspace_id":         {Required: true},
		"arguments":            {Required: true},
		"task_id":              {},
		"session_id":           {},
		"run_id":               {},
		"requested_capability": {},
	}},
}

type managerRPCMethodsListResult struct {
	Count   int `json:"count"`
	Methods []struct {
		Method string `json:"method"`
	} `json:"methods"`
}

type managerRPCDescribeResult struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

func managerRPCContractRequiredMethods() []string {
	return append([]string(nil), managerRPCContractMethodMatrix...)
}

func managerRPCContractSmokeLocalEndpoints() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL")))
	return value == "1" || value == "true" || value == "yes"
}

func managerRPCContractAllowLocalOnlyOverride() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_MANAGED_RPC_CONTRACT_ALLOW_LOCAL_ONLY")))
	return value == "1" || value == "true" || value == "yes"
}

func managerRPCContractLocalEndpoint(endpoint string) bool {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return false
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func managerRPCContractReadinessForRoster(registry BotRegistry) managerRPCContractReadiness {
	required := managerRPCContractRequiredMethods()
	rows := []managerRPCContractAgentReadiness{}
	blocked := []string{}
	readyCount := 0
	skippedCount := 0
	schemaCache := map[string]error{}
	for _, record := range registry.Agents {
		record = normalizeManagedAgentRecord(record)
		agentID := firstNonEmpty(record.AgentID, record.Workdir, "unknown")
		cfg := managedAgentStartRuntimeConfig(record)
		endpoint := strings.TrimSpace(cfg.RhizomeRPC)
		if endpoint == "" && strings.TrimSpace(cfg.RhizomeHost) != "" {
			endpoint = defaultRPCEndpoint(cfg.RhizomeHost)
		}
		row := managerRPCContractAgentReadiness{
			AgentID:  agentID,
			Endpoint: endpoint,
		}
		if strings.TrimSpace(cfg.RhizomeToken) == "" {
			if managerRPCContractAllowLocalOnlyOverride() {
				row.Status = "not_required"
				row.Reason = "rhizome token unavailable; explicit local-only RPC contract override recorded"
				rows = append(rows, row)
				skippedCount++
				continue
			}
			row.Status = "blocked"
			row.Reason = "rhizome token unavailable; remote RPC compatibility proof is required"
			rows = append(rows, row)
			blocked = append(blocked, agentID+":rhizome token unavailable")
			continue
		}
		if endpoint == "" {
			row.Status = "blocked"
			row.Reason = "rpc endpoint unavailable"
			row.MissingMethods = append([]string(nil), required...)
			rows = append(rows, row)
			blocked = append(blocked, agentID+":rpc endpoint unavailable")
			continue
		}
		if managerRPCContractLocalEndpoint(endpoint) && !managerRPCContractSmokeLocalEndpoints() {
			if managerRPCContractAllowLocalOnlyOverride() {
				row.Status = "not_required"
				row.Reason = "local RPC endpoint; explicit local-only RPC contract override recorded"
				rows = append(rows, row)
				skippedCount++
				continue
			}
			row.Status = "blocked"
			row.Reason = "local RPC endpoint requires RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL=1 or explicit local-only override"
			rows = append(rows, row)
			blocked = append(blocked, agentID+":local RPC endpoint smoke disabled")
			continue
		}
		methodCount, missing, err := managerRPCContractSmoke(endpoint, cfg.RhizomeToken, cfg.WorkspaceID, required, schemaCache)
		row.MethodCount = methodCount
		row.MissingMethods = append([]string(nil), missing...)
		if err != nil {
			row.Status = "blocked"
			row.Reason = err.Error()
			blocked = append(blocked, agentID+":"+err.Error())
		} else if len(missing) > 0 {
			row.Status = "blocked"
			row.Reason = "remote RPC method matrix is missing required methods"
			blocked = append(blocked, agentID+":missing "+strings.Join(missing, ","))
		} else {
			row.Status = "ready"
			row.Reason = "remote RPC method/schema compatibility smoke passed"
			readyCount++
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return managerRPCContractReadiness{
			Status:          "not_required",
			Reason:          "no managed agents configured",
			RequiredMethods: required,
		}
	}
	if len(blocked) > 0 {
		sort.Strings(blocked)
		return managerRPCContractReadiness{
			Status:          "blocked",
			Reason:          strings.Join(blocked, "; "),
			RequiredMethods: required,
			MissingMethods:  managerRPCContractMissingUnion(rows),
			Agents:          rows,
		}
	}
	if readyCount == 0 {
		return managerRPCContractReadiness{
			Status:          "not_required",
			Reason:          fmt.Sprintf("remote RPC compatibility smoke explicitly skipped for %d local-only agent(s)", skippedCount),
			RequiredMethods: required,
			Agents:          rows,
		}
	}
	return managerRPCContractReadiness{
		Status:          "ready",
		Reason:          "remote RPC method/schema compatibility smoke passed",
		RequiredMethods: required,
		Agents:          rows,
	}
}

func managerRPCContractSmoke(endpoint, token, workspaceID string, required []string, schemaCache map[string]error) (int, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), managerRPCContractSmokeTimeout)
	defer cancel()
	client := NewRhizomeClient(endpoint, token)
	var listed managerRPCMethodsListResult
	if err := managerRPCContractCall(ctx, client, "rpc.methods.list", map[string]any{}, &listed); err != nil {
		return 0, nil, fmt.Errorf("rpc.methods.list compatibility smoke failed: %w", err)
	}
	seen := map[string]bool{}
	for _, method := range listed.Methods {
		name := strings.TrimSpace(method.Method)
		if name != "" {
			seen[name] = true
		}
	}
	missing := []string{}
	for _, method := range required {
		method = strings.TrimSpace(method)
		if method == "" {
			continue
		}
		if !seen[method] {
			missing = append(missing, method)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return listed.Count, missing, nil
	}
	cacheKey := strings.TrimSpace(endpoint)
	if schemaCache != nil {
		if err, ok := schemaCache[cacheKey]; ok {
			if err != nil {
				return listed.Count, nil, err
			}
			return listed.Count, nil, nil
		}
	}
	smokeErr := managerRPCContractSmokeSchemas(ctx, client, required)
	if smokeErr == nil {
		smokeErr = managerRPCContractBuildParity(ctx, client, workspaceID)
	}
	if schemaCache != nil && !managerRPCContractTransientError(smokeErr) {
		schemaCache[cacheKey] = smokeErr
	}
	if smokeErr != nil {
		return listed.Count, nil, smokeErr
	}
	return listed.Count, nil, nil
}

type managerRuntimeBuildInfoResult struct {
	BinarySHA256 string `json:"binary_sha256"`
	GoVersion    string `json:"go_version"`
	VCSRevision  string `json:"vcs_revision"`
	VCSTime      string `json:"vcs_time"`
	VCSModified  bool   `json:"vcs_modified"`
}

// managerRPCContractBuildParity proves the remote server binary is built from the
// same VCS revision as the local server checkout (CT-01). A green RPC method/schema
// matrix is not enough: an old or dirty remote build can satisfy the contract
// surface while diverging in behavior. The local bot may be built from the
// agent submodule, so remote parity is checked against the root checkout
// that produces the server binary, with a single-repo fallback for tests and
// local development.
func managerRPCContractBuildParity(ctx context.Context, client *RhizomeClient, workspaceID string) error {
	var remote managerRuntimeBuildInfoResult
	if err := managerRPCContractCall(ctx, client, "runtime.build.info", map[string]any{"workspace_id": workspaceID}, &remote); err != nil {
		return fmt.Errorf("runtime.build.info build-parity smoke failed: %w", err)
	}
	remoteRev := strings.TrimSpace(remote.VCSRevision)
	if remoteRev == "" {
		return fmt.Errorf("runtime.build.info returned no vcs_revision; cannot prove remote/local build parity")
	}
	if remote.VCSModified {
		return fmt.Errorf("remote runtime build is vcs-modified (revision %s); a dirty build is deployed", remoteRev)
	}
	localServerRev := strings.TrimSpace(managerSubstrateRepositoryFunc().Head)
	if localServerRev == "" {
		localServerRev = strings.TrimSpace(managerSubstrateRuntimeBuildFunc().VCSRevision)
	}
	if localServerRev != "" && remoteRev != localServerRev {
		// FF-R09-1: the campaign process REQUIRES per-round run-ledger commits (postmortems,
		// plans, README rows), so the local checkout legitimately moves ahead of the deployed
		// revision by doc-only commits mid-campaign. Strict SHA equality here killed the R09
		// auto-advance (remote 97f6059a vs local 012a200e - delta was one ledger commit).
		// Tolerate the mismatch ONLY when the remote revision is a known ancestor and the
		// entire diff touches run-ledger/doc paths; any build-relevant path stays fail-closed.
		runLedgerOnly, detail := managerBuildParityDriftIsRunLedgerOnly(remoteRev, localServerRev)
		if !runLedgerOnly {
			return fmt.Errorf("remote runtime build revision %s does not match local server checkout revision %s (build drift%s)", remoteRev, localServerRev, detail)
		}
	}
	return nil
}

// managerBuildParityDiffFunc is a test seam over the git ancestry+diff probe used to classify
// remote/local revision drift.
var managerBuildParityDiffFunc = managerBuildParityGitDiffNames

func managerBuildParityGitDiffNames(remoteRev, localRev string) ([]string, error) {
	root := strings.TrimSpace(managerSubstrateRepoRootFunc())
	if root == "" {
		return nil, errors.New("git repository root not found")
	}
	// The remote revision must be a known ancestor of the local checkout; unknown or diverged
	// revisions remain fail-closed build drift.
	if _, err := managerSubstrateRepositoryCommandOutput(root, "git", "-C", root, "merge-base", "--is-ancestor", remoteRev, localRev); err != nil {
		return nil, fmt.Errorf("remote revision %s is not a known ancestor of local %s: %v", remoteRev, localRev, err)
	}
	raw, err := managerSubstrateRepositoryCommandOutput(root, "git", "-C", root, "diff", "--name-only", remoteRev, localRev)
	if err != nil {
		return nil, err
	}
	return nonEmptyLines(raw), nil
}

func managerBuildParityDriftIsRunLedgerOnly(remoteRev, localRev string) (bool, string) {
	paths, err := managerBuildParityDiffFunc(remoteRev, localRev)
	if err != nil {
		return false, "; drift classification failed: " + err.Error()
	}
	if len(paths) == 0 {
		return true, ""
	}
	for _, path := range paths {
		if !managerBuildParityPathIsRunLedger(path) {
			return false, "; first build-relevant path: " + strings.TrimSpace(path)
		}
	}
	return true, ""
}

// managerBuildParityPathIsRunLedger reports whether a changed path cannot affect the deployed
// server build: run ledgers, plans, docs, and markdown. Everything else (Go code, the agent
// submodule pointer, go.mod/go.sum, scripts, configs outside runs/) is build-relevant.
func managerBuildParityPathIsRunLedger(path string) bool {
	p := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(path, "\\", "/")))
	if p == "" {
		return true
	}
	for _, prefix := range []string{"runs/", "plans/", "docs/", "knowledge-vault/"} {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	if strings.HasSuffix(p, ".md") {
		return true
	}
	return p == ".gitignore"
}

func managerRPCContractSmokeSchemas(ctx context.Context, client *RhizomeClient, required []string) error {
	for _, method := range required {
		method = strings.TrimSpace(method)
		if method == "" {
			continue
		}
		var described managerRPCDescribeResult
		if err := managerRPCContractCall(ctx, client, "rpc.describe", map[string]any{"method": method}, &described); err != nil {
			return fmt.Errorf("rpc.describe %s compatibility smoke failed: %w", method, err)
		}
		if strings.TrimSpace(described.Method) != method {
			return fmt.Errorf("rpc.describe %s returned method %q", method, strings.TrimSpace(described.Method))
		}
		if described.Params == nil {
			return fmt.Errorf("rpc.describe %s returned no params schema", method)
		}
		if err := managerRPCContractValidateMethodSchema(method, described.Params); err != nil {
			return err
		}
	}
	return nil
}

func managerRPCContractCall(ctx context.Context, client *RhizomeClient, method string, params any, out any) error {
	return managerSubstrateRPCWithRetry(ctx, method, func(ctx context.Context) error {
		callCtx := ctx
		cancel := func() {}
		if timeout := managerRPCContractCallTimeout; timeout > 0 {
			if deadline, ok := ctx.Deadline(); ok {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
					return context.DeadlineExceeded
				}
				if remaining < timeout {
					timeout = remaining
				}
			}
			callCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()
		return client.call(callCtx, method, params, out)
	})
}

func managerSubstrateRPCWithRetry(ctx context.Context, _ string, call func(context.Context) error) error {
	var last error
	for attempt := 0; attempt < managerRPCContractSmokeMaxAttempts; attempt++ {
		err := call(ctx)
		if err == nil {
			return nil
		}
		last = err
		if !managerRPCContractTransientError(err) || attempt == managerRPCContractSmokeMaxAttempts-1 {
			return err
		}
		delay := managerRPCContractRetryDelay(attempt)
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

func managerRPCContractRetryDelay(attempt int) time.Duration {
	delay := managerRPCContractSmokeRetryDelay
	if delay <= 0 {
		return 0
	}
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	if delay > 8*time.Second {
		return 8 * time.Second
	}
	return delay
}

func managerRPCContractTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "http 429"),
		strings.Contains(message, "rate limit exceeded"),
		strings.Contains(message, "http 500"),
		strings.Contains(message, "http 502"),
		strings.Contains(message, "http 503"),
		strings.Contains(message, "http 504"),
		strings.Contains(message, " transport:"),
		strings.Contains(message, "connection reset"),
		strings.Contains(message, "connection refused"),
		strings.Contains(message, "connection timed out"),
		strings.Contains(message, "operation has timed out"),
		strings.Contains(message, "operation was canceled"),
		strings.Contains(message, "context deadline"),
		strings.Contains(message, "context canceled"),
		strings.Contains(message, "deadline exceeded"),
		strings.Contains(message, "timed out"),
		strings.Contains(message, "timeout"),
		strings.Contains(message, "temporary failure"),
		strings.Contains(message, "temporarily unavailable"),
		strings.Contains(message, "broken pipe"):
		return true
	default:
		return false
	}
}

func managerRPCContractValidateMethodSchema(method string, params map[string]any) error {
	requirement, ok := managerRPCContractSchemaRequirements[method]
	if !ok {
		return nil
	}
	for name, paramRequirement := range requirement.Params {
		raw, ok := params[name]
		if !ok {
			return fmt.Errorf("rpc.describe %s missing required schema param %s", method, name)
		}
		paramSchema, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("rpc.describe %s param %s schema has unexpected shape", method, name)
		}
		if paramRequirement.Required {
			required, _ := paramSchema["required"].(bool)
			if !required {
				return fmt.Errorf("rpc.describe %s param %s is not marked required", method, name)
			}
		}
		if len(paramRequirement.Enum) > 0 {
			if !managerRPCSchemaEnumContainsAll(paramSchema["enum"], paramRequirement.Enum) {
				return fmt.Errorf("rpc.describe %s param %s enum missing %s", method, name, strings.Join(paramRequirement.Enum, ","))
			}
		}
	}
	// CT-02 fail-closed rule: for a curated method, the server must not have grown a
	// new required param the bot does not know to send. Iterate the params the server
	// *declares* and block if any required:true param is absent from the curated set —
	// otherwise the call would be rejected mid-run (lost lineage / stuck claim).
	grown := []string{}
	for name, raw := range params {
		paramSchema, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		required, _ := paramSchema["required"].(bool)
		if !required {
			continue
		}
		if _, known := requirement.Params[strings.TrimSpace(name)]; !known {
			grown = append(grown, strings.TrimSpace(name))
		}
	}
	if len(grown) > 0 {
		sort.Strings(grown)
		return fmt.Errorf("rpc.describe %s declares unrecognized required param(s) %s; server contract grew beyond the bot's known schema", method, strings.Join(grown, ","))
	}
	return nil
}

func managerRPCSchemaEnumContainsAll(raw any, required []string) bool {
	values := map[string]struct{}{}
	switch enum := raw.(type) {
	case []any:
		for _, item := range enum {
			value := strings.TrimSpace(fmt.Sprint(item))
			if value != "" {
				values[value] = struct{}{}
			}
		}
	case []string:
		for _, item := range enum {
			value := strings.TrimSpace(item)
			if value != "" {
				values[value] = struct{}{}
			}
		}
	default:
		return false
	}
	for _, want := range required {
		if _, ok := values[strings.TrimSpace(want)]; !ok {
			return false
		}
	}
	return true
}

func managerRPCContractMissingUnion(rows []managerRPCContractAgentReadiness) []string {
	set := map[string]struct{}{}
	for _, row := range rows {
		for _, method := range row.MissingMethods {
			method = strings.TrimSpace(method)
			if method != "" {
				set[method] = struct{}{}
			}
		}
	}
	missing := make([]string, 0, len(set))
	for method := range set {
		missing = append(missing, method)
	}
	sort.Strings(missing)
	return missing
}

func managerFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func managerStableJSONDigest(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func managedAgentArgsDigest(args []string) string {
	return managerStableJSONDigest(append([]string(nil), args...))
}

func managedAgentRuntimeConfigDigest(cfg RuntimeConfig) string {
	cfg = managedAgentRuntimeConfigDigestIdentity(cfg)
	return managerStableJSONDigest(cfg)
}

func managedAgentRuntimeConfigDigestIdentity(cfg RuntimeConfig) RuntimeConfig {
	cfg = managedAgentEffectiveRuntimeConfig(ManagedAgentRecord{AgentID: cfg.AgentID, Workdir: cfg.Workdir, WorkspaceID: cfg.WorkspaceID}, cfg)
	cfg.ApplyDefaults()
	cfg.OpenAIKey = ""
	cfg.RhizomeToken = ""
	cfg.RhizomeRPC = ""
	cfg.RhizomeHost = ""
	cfg.WorkspaceName = ""
	cfg.WorkspacePassword = ""
	return cfg
}

func nonEmptyLines(raw string) []string {
	lines := []string{}
	for _, line := range strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
