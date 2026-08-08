package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/server"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/tools"
)

const (
	defaultServeListenAddr          = "127.0.0.1:8420"
	loopNameDaemon                  = "daemon"
	loopNameFirehose                = "firehose"
	loopNameMemoryProjection        = "memory_projection"
	loopNameOperatorQueueProjection = "operator_queue_projection"
	loopNameRepoMutationActuator    = "repo_mutation_actuator"
	loopNameTimeoutReaper           = "timeout_reaper"
	loopNameGovernanceDeadline      = "governance_deadline"

	serveProjectionSweepMaxWorkspaces               = 16
	serveProjectionSweepBatchSize                   = 64
	serveProjectionProcessingStaleAge               = 5 * time.Minute
	serveProjectionSweepInterval                    = 2 * time.Second
	serveMemoryProjectionReadinessStaleAfter        = 30 * time.Second
	serveOperatorQueueProjectionSweepInterval       = 5 * time.Second
	serveOperatorQueueProjectionSweepLimit          = 128
	serveOperatorQueueProjectionReadinessStaleAfter = 60 * time.Second
	serveGovernanceDeadlineSweepInterval            = 5 * time.Second
	serveGovernanceDeadlineSweepLimit               = 32
	serveBudgetLedgerStaleAfter                     = 15 * time.Minute
)

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	addr := fs.String("addr", "", "Listen address (default from RHIZOME_LISTEN_ADDR or 127.0.0.1:8420)")
	allowRemote := fs.Bool("allow-remote", false, "Allow an explicit wildcard or non-loopback listen address")
	withDaemon := fs.Bool("with-daemon", false, "Also run daemon polling loop in the same process")
	pollMS := fs.Int("poll-ms", 1000, "Daemon polling interval in milliseconds (only with --with-daemon)")
	maxNodes := fs.Int("max-nodes", 10, "Maximum nodes per tick (only with --with-daemon)")
	nodeTimeoutSec := fs.Int("node-timeout-sec", 120, "Executor node timeout (only with --with-daemon)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	listenAddr := resolveServeListenAddr(*addr, os.Getenv("RHIZOME_LISTEN_ADDR"))
	remote, err := validateServeListenAddr(listenAddr, *allowRemote)
	if err != nil {
		return err
	}
	if remote {
		log.Printf("WARNING: --allow-remote exposes Rhizome on %s without built-in TLS; use only on a trusted network or behind a TLS reverse proxy", listenAddr)
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("bind http listener %s: %w", listenAddr, err)
	}
	defer func() { _ = listener.Close() }()

	if *withDaemon {
		if *pollMS <= 0 {
			return errors.New("--poll-ms must be positive")
		}
		if *maxNodes <= 0 {
			return errors.New("--max-nodes must be positive")
		}
		if *nodeTimeoutSec <= 0 {
			return errors.New("--node-timeout-sec must be positive")
		}
	}

	cfg := app.LoadConfig()
	store, err := sqlite.NewStore(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open sqlite store: %w", err)
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if _, err := store.EnsureLocalAuthorityNode(ctx); err != nil {
		return fmt.Errorf("ensure authority node identity: %w", err)
	}
	if err := ensureServeWorkspace(ctx, store, os.Getenv("RHIZOME_WORKSPACE_ID"), os.Getenv("RHIZOME_WORKSPACE_PASSWORD")); err != nil {
		return fmt.Errorf("ensure serve workspace: %w", err)
	}
	if err := ensureServeRuntimeFiles(cfg, time.Now().UTC()); err != nil {
		return fmt.Errorf("ensure serve runtime files: %w", err)
	}

	handler := server.NewHandler(store)
	rpcRl := server.NewRateLimiter(600, time.Minute)          // RPC surface
	eventsRl := server.NewRateLimiter(600, time.Minute)       // SSE surface
	restRl := server.NewRateLimiter(600, time.Minute)         // authenticated REST surface
	diagnosticsRl := server.NewRateLimiter(1800, time.Minute) // diagnostics reads can spike during active dashboard fanout
	authRl := server.NewRateLimiter(5, time.Minute)           // 5 req/min per IP/token for PBKDF2-heavy endpoints

	mux := http.NewServeMux()

	withTimeout := func(h http.Handler) http.Handler {
		return http.TimeoutHandler(h, 30*time.Second, `{"error":"request timeout"}`)
	}
	withRPCTimeout := func(h http.Handler) http.Handler {
		// tool.call is allowed to run longer than the default RPC budget, so the
		// outer HTTP timeout must not undercut the inner per-method contract.
		return http.TimeoutHandler(h, tools.DefaultCallTimeout, `{"error":"request timeout"}`)
	}

	mux.Handle("/rpc", server.RateLimitMiddleware(rpcRl, withRPCTimeout(server.AuthMiddlewareWithStore(store, handler))))

	// SSE events endpoint (with auth + rate limit) - no timeout
	mux.Handle("/events", server.RateLimitMiddleware(eventsRl, server.AuthMiddlewareWithStore(store, handler.ServeEventsHTTP())))

	// Dashboard (no auth — token entered in browser)
	mux.Handle("/api/auth/human/login", server.PeerIPRateLimitMiddleware(authRl, withTimeout(handler.ServeHumanLoginHTTP())))
	mux.Handle("/api/auth/human/register", server.PeerIPRateLimitMiddleware(authRl, withTimeout(handler.ServeHumanRegisterHTTP())))
	mux.Handle("/api/auth/human/profile/get", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeHumanProfileGetHTTP()))))
	mux.Handle("/api/auth/human/profile/update", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeHumanProfileUpdateHTTP()))))
	mux.Handle("/api/auth/human/sessions/list", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeHumanSessionsListHTTP()))))
	mux.Handle("/api/auth/human/sessions/revoke", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeHumanSessionsRevokeHTTP()))))
	mux.Handle("/api/auth/agent/register", server.PeerIPRateLimitMiddleware(authRl, withTimeout(handler.ServeAgentRegisterHTTP())))
	mux.Handle("/api/auth/agent/update", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeAgentUpdateHTTP()))))
	mux.Handle("/api/auth/agent/token/rotate", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeAgentTokenRotateHTTP()))))
	mux.Handle("/api/workspace/security/get", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeWorkspaceSecurityGetHTTP()))))
	mux.Handle("/api/workspace/security/update", server.RateLimitMiddleware(authRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeWorkspaceSecurityUpdateHTTP()))))
	mux.Handle("/api/workspace/security/logs", server.RateLimitMiddleware(restRl, withTimeout(server.AuthMiddlewareWithStore(store, handler.ServeWorkspaceSecurityLogsHTTP()))))
	mux.HandleFunc("/dashboard", server.ServeDashboard())
	mux.Handle("/assets/", http.FileServer(http.FS(server.AssetsFS)))

	// Health endpoint (no auth required) - public liveness only
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		payload := collectPublicLivenessPayload()
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	// Readiness registry: honest loop state tracking (P2D-001)
	readiness := NewReadinessRegistry()
	readiness.Register(loopNameDaemon)
	readiness.Register(loopNameFirehose)
	readiness.Register(loopNameMemoryProjection)
	readiness.Register(loopNameOperatorQueueProjection)
	readiness.Register(loopNameRepoMutationActuator)
	readiness.Register(loopNameTimeoutReaper)
	readiness.Register(loopNameGovernanceDeadline)
	readiness.Register(loopNameAuthorityLease)

	// Readiness endpoint (no auth required) - public readiness/deployment gate only
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		payload := collectServeHealthPayload(r.Context(), cfg, store, readiness)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(serveReadinessHTTPStatus(payload))
		if err := json.NewEncoder(w).Encode(collectPublicReadinessPayload(payload)); err != nil {
			log.Printf("encode readiness payload: %v", err)
		}
	})

	// Authenticated diagnostics endpoint
	mux.Handle("/api/diagnostics", server.RateLimitMiddleware(diagnosticsRl, server.AuthMiddlewareWithStoreOrServerToken(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		payload := collectServeHealthPayload(r.Context(), cfg, store, readiness)
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))))

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      server.LoggingMiddleware(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // 0 for SSE long-lived connections
		IdleTimeout:  120 * time.Second,
	}

	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serveSupervisor := newServeLoopSupervisor()

	if legacyRSPHandoffEnabled() {
		log.Println("[rsp] enabling legacy statistical handoff listener")
		handler.StartRSPListener(runCtx)
	}

	// Start daemon loop in background if requested
	if *withDaemon {
		readiness.SetState(loopNameDaemon, LoopRunning)
		serveSupervisor.Go(func() {
			backoff := 1 * time.Second
			for {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[FATAL] daemon loop panic recovered: %v", r)
							readiness.SetError(loopNameDaemon, fmt.Errorf("panic: %v", r))
						}
					}()
					readiness.SetState(loopNameDaemon, LoopRunning)
					daemonErr := runDaemonLoop(runCtx, store, cfg, *pollMS, *maxNodes, *nodeTimeoutSec)
					if daemonErr != nil && !errors.Is(daemonErr, context.Canceled) {
						log.Printf("daemon loop error: %v", daemonErr)
						readiness.SetError(loopNameDaemon, daemonErr)
					}
				}()
				select {
				case <-runCtx.Done():
					readiness.SetState(loopNameDaemon, LoopStopped)
					return
				case <-time.After(backoff):
					backoff *= 2
					if backoff > 30*time.Second {
						backoff = 30 * time.Second
					}
				}
			}
		})
	} else {
		readiness.SetState(loopNameDaemon, LoopDisabled)
	}

	// Agent requests are handled by external runtimes through the control plane.
	// The serve process only runs maintenance loops for expired requests.
	log.Println("[agent] control-plane mode: external agent runtimes handle requests")

	// Request timeout reaper
	readiness.SetState(loopNameTimeoutReaper, LoopRunning)
	serveSupervisor.Go(func() {
		backoff := 1 * time.Second
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[FATAL] timeout reaper panic recovered: %v", r)
						readiness.SetError(loopNameTimeoutReaper, fmt.Errorf("panic: %v", r))
					}
				}()
				readiness.SetState(loopNameTimeoutReaper, LoopRunning)
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-runCtx.Done():
						readiness.SetState(loopNameTimeoutReaper, LoopStopped)
						return
					case <-ticker.C:
						expired, err := store.ExpireRequests(runCtx)
						if err != nil {
							log.Printf("[timeout-reaper] error: %v", err)
						} else if expired > 0 {
							log.Printf("[timeout-reaper] expired %d requests", expired)
						}

						rebooted, err := store.RebootStalledSessions(runCtx, 100)
						if err != nil {
							log.Printf("[tension-anti-stall] error: %v", err)
						} else if len(rebooted) > 0 {
							log.Printf("[tension-anti-stall] rebooted %d stalled sessions", len(rebooted))
						}

						pruned, err := store.PruneMaintenance(runCtx, 30*24*time.Hour)
						if err != nil {
							log.Printf("[prune-maintenance] error: %v", err)
						} else if pruned > 0 {
							log.Printf("[prune-maintenance] pruned %d old rpc logs", pruned)
						}
					}
				}
			}()
			select {
			case <-runCtx.Done():
				readiness.SetState(loopNameTimeoutReaper, LoopStopped)
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}
	})

	// Firehose readiness: sync from Store's actual firehose state
	syncFirehoseReadiness(readiness, store)

	// Local workspace authority lease maintenance: renew active local leases and
	// explicitly expire stale holders after the grace window on a single host.
	startLocalAuthorityLeaseLoop(runCtx, store, readiness, serveSupervisor)

	startServeMemoryProjectionReconciler(runCtx, store, readiness, serveSupervisor)
	startServeOperatorQueueProjectionReconciler(runCtx, store, readiness, serveSupervisor)
	startServeRepoMutationActuator(runCtx, store, readiness, serveSupervisor)
	startServeGovernanceDeadlineLoop(runCtx, store, readiness, serveSupervisor)

	// Start HTTP server
	serveSupervisor.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[FATAL] http server panic recovered: %v", r)
			}
		}()
		log.Printf("rhizome serve listening on %s", listenAddr)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server error: %v", err)
		}
	})

	<-runCtx.Done()
	log.Println("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	return shutdownServe(shutdownCtx, srv, serveSupervisor)
}

func resolveServeListenAddr(flagAddr, envAddr string) string {
	if value := strings.TrimSpace(flagAddr); value != "" {
		return value
	}
	if value := strings.TrimSpace(envAddr); value != "" {
		return value
	}
	return defaultServeListenAddr
}

func validateServeListenAddr(addr string, allowRemote bool) (bool, error) {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return false, fmt.Errorf("invalid listen address %q: %w", addr, err)
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	loopback := strings.EqualFold(host, "localhost")
	if ip := net.ParseIP(host); ip != nil {
		loopback = ip.IsLoopback()
	}
	if loopback {
		return false, nil
	}
	if !allowRemote {
		return true, fmt.Errorf("refusing wildcard or non-loopback listen address %q without --allow-remote", addr)
	}
	return true, nil
}

type serveLoopSupervisor struct {
	wg sync.WaitGroup
}

func newServeLoopSupervisor() *serveLoopSupervisor {
	return &serveLoopSupervisor{}
}

func (s *serveLoopSupervisor) Go(run func()) {
	if run == nil {
		return
	}
	if s == nil {
		go run()
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		run()
	}()
}

func (s *serveLoopSupervisor) Wait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func shutdownServe(ctx context.Context, srv *http.Server, supervisor *serveLoopSupervisor) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var shutdownErr error
	if srv != nil {
		shutdownErr = srv.Shutdown(ctx)
		if shutdownErr != nil {
			if closeErr := srv.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
				log.Printf("force close http server after shutdown error: %v", closeErr)
			}
		}
	}
	if err := supervisor.Wait(ctx); err != nil {
		if shutdownErr != nil {
			return fmt.Errorf("%w; serve loop drain: %v", shutdownErr, err)
		}
		return fmt.Errorf("serve loop drain: %w", err)
	}
	return shutdownErr
}

type serviceReadinessPayload struct {
	Status         string                             `json:"status"`
	TS             string                             `json:"ts"`
	ReasonCodes    []string                           `json:"reason_codes,omitempty"`
	Semantics      TopLevelSemantics                  `json:"semantics"`
	AuthorityNode  sqlite.AuthorityNodeDiagnostics    `json:"authority_node,omitempty"`
	AuthorityLease sqlite.AuthorityLeaseDiagnostics   `json:"authority_lease,omitempty"`
	LoopReadiness  []LoopReadiness                    `json:"loop_readiness,omitempty"`
	Extended       ExtendedReadiness                  `json:"extended_readiness"`
	BudgetLedger   *sqlite.BudgetLedgerHealthSnapshot `json:"budget_ledger,omitempty"`
}

func collectServeHealthPayload(ctx context.Context, cfg app.Config, store *sqlite.Store, readiness *ReadinessRegistry) serviceHealthPayload {
	if ctx == nil {
		ctx = context.Background()
	}
	syncFirehoseReadiness(readiness, store)
	syncMemoryProjectionReadiness(readiness, time.Now().UTC())
	syncOperatorQueueProjectionReadiness(readiness, time.Now().UTC())
	projectionLag := collectProjectionLagSnapshot(store)
	operatorQueueLagSnapshot := store.CurrentOperatorQueueLagSnapshot(ctx)
	operatorQueueLag := DiagnosticSignal{State: operatorQueueLagSnapshot.State, Message: operatorQueueLagSnapshot.Message}
	reviewerScarcitySnapshot := store.CurrentReviewerScarcityHealthSnapshot(ctx)
	reviewerScarcity := reviewerScarcityDiagnosticSignal(reviewerScarcitySnapshot)
	stuckAgentSnapshot := store.CurrentStuckAgentSnapshot(ctx)
	stuckAgents := DiagnosticSignal{State: stuckAgentSnapshot.State, Message: stuckAgentSnapshot.Message}
	noProgressSnapshot := store.CurrentExecutionNoProgressSnapshot(ctx)
	noProgress := DiagnosticSignal{State: noProgressSnapshot.State, Message: noProgressSnapshot.Message}
	authorityNode := store.CurrentAuthorityNodeDiagnostics(ctx)
	authorityLease := store.CurrentAuthorityLeaseDiagnostics(ctx, "workspace")
	budgetLedger := collectBudgetLedgerHealthSnapshot(store)
	patchQueueDurability := store.ProjectPatchQueueDurabilityProof(ctx)
	patchQueueActuatorHealth := store.CurrentProjectPatchQueueActuatorHealthSnapshot(ctx)

	payload := collectServiceHealthPayloadFromStateWithReviewerScarcityHealthAndNoProgressHealthAndBudgetLedger(
		cfg,
		readiness,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		reviewerScarcitySnapshot,
		stuckAgents,
		&stuckAgentSnapshot,
		noProgress,
		&noProgressSnapshot,
		authorityNode,
		authorityLease,
		budgetLedger,
		app.CurrentRuntimeBuildInfo(),
		app.CurrentGitCheckoutInfo(),
	)
	payload.PatchQueue = &patchQueueDurability
	payload.RepoMutationActuator = &patchQueueActuatorHealth
	payload.RepoMutation = collectRepoMutationActivationDiagnosticsFromStore(ctx, store, &patchQueueDurability)
	payload.RepoMutationDryRun = collectRepoMutationActuatorDryRunDiagnostics(payload.RepoMutation)
	applyProjectPatchQueueDurabilityHealth(&payload, patchQueueDurability)
	applyProjectPatchQueueActuatorHealth(&payload, patchQueueActuatorHealth)
	return payload
}

func collectPublicReadinessPayload(payload serviceHealthPayload) serviceReadinessPayload {
	return serviceReadinessPayload{
		Status:         publicReadinessStatus(payload),
		TS:             payload.TS,
		ReasonCodes:    publicReadinessReasonCodes(payload),
		Semantics:      payload.Semantics,
		AuthorityNode:  payload.AuthorityNode,
		AuthorityLease: payload.AuthorityLease,
		LoopReadiness:  payload.LoopReadiness,
		Extended:       payload.Extended,
		BudgetLedger:   payload.BudgetLedger,
	}
}

func serveReadinessHTTPStatus(payload serviceHealthPayload) int {
	if signalOK(payload.Semantics.Readiness.State) &&
		signalOK(payload.Semantics.DeploymentReadiness.State) &&
		!strings.EqualFold(strings.TrimSpace(payload.Status), "degraded") {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

func publicReadinessStatus(payload serviceHealthPayload) string {
	if serveReadinessHTTPStatus(payload) == http.StatusOK {
		return "ready"
	}
	if !signalOK(payload.Semantics.Readiness.State) {
		return "not_ready"
	}
	if state := strings.TrimSpace(payload.Semantics.DeploymentReadiness.State); state != "" && !signalOK(state) {
		return state
	}
	if status := strings.TrimSpace(payload.Status); status != "" {
		return status
	}
	return "not_ready"
}

func publicReadinessReasonCodes(payload serviceHealthPayload) []string {
	if serveReadinessHTTPStatus(payload) == http.StatusOK {
		return nil
	}
	codes := []string{}
	if !signalOK(payload.Semantics.Readiness.State) {
		codes = append(codes, "readiness_"+safeReadinessCodePart(payload.Semantics.Readiness.State))
	}
	if !signalOK(payload.Semantics.DeploymentReadiness.State) {
		codes = append(codes, "deployment_readiness_"+safeReadinessCodePart(payload.Semantics.DeploymentReadiness.State))
	}
	if status := strings.TrimSpace(payload.Status); status != "" && !strings.EqualFold(status, "ok") {
		codes = append(codes, "service_status_"+safeReadinessCodePart(status))
	}
	if strings.TrimSpace(payload.Checkout.Error) != "" {
		codes = append(codes, "checkout_unavailable")
	}
	if payload.Runtime.VCSModified {
		codes = append(codes, "runtime_built_from_modified_checkout")
	}
	if runtimeRevisionDrifts(payload.Runtime, payload.Checkout) {
		codes = append(codes, "runtime_revision_drift")
	}
	if status := strings.TrimSpace(payload.Metrics.Status); status != "" && !strings.EqualFold(status, "ok") {
		codes = append(codes, "runtime_metrics_"+safeReadinessCodePart(status))
	}
	for _, loop := range payload.LoopReadiness {
		switch loop.State {
		case LoopRunning, LoopDisabled:
			continue
		default:
			codes = append(codes, "loop_"+safeReadinessCodePart(loop.Name)+"_"+safeReadinessCodePart(string(loop.State)))
		}
	}
	for _, item := range []struct {
		name string
		sig  DiagnosticSignal
	}{
		{name: "projection_lag", sig: DiagnosticSignal{State: payload.Extended.ProjectionLag.State}},
		{name: "operator_queue_lag", sig: payload.Extended.OperatorQueueLag},
		{name: "reviewer_scarcity", sig: payload.Extended.ReviewerScarcity},
		{name: "stuck_agents", sig: payload.Extended.StuckAgents},
		{name: "no_progress", sig: payload.Extended.NoProgress},
	} {
		switch strings.ToLower(strings.TrimSpace(item.sig.State)) {
		case "", "ok", "unsupported":
			continue
		default:
			codes = append(codes, item.name+"_"+safeReadinessCodePart(item.sig.State))
		}
	}
	for _, item := range []struct {
		name  string
		state string
	}{
		{name: "authority_node", state: payload.AuthorityNode.State},
		{name: "authority_lease", state: payload.AuthorityLease.State},
	} {
		switch strings.ToLower(strings.TrimSpace(item.state)) {
		case "", "ok", "ready":
			continue
		default:
			codes = append(codes, item.name+"_"+safeReadinessCodePart(item.state))
		}
	}
	if payload.BudgetLedger != nil {
		switch strings.ToLower(strings.TrimSpace(payload.BudgetLedger.Status)) {
		case "", "ok", "healthy":
		default:
			codes = append(codes, "budget_ledger_"+safeReadinessCodePart(payload.BudgetLedger.Status))
		}
	}
	return uniqueReadinessReasonCodes(codes)
}

func uniqueReadinessReasonCodes(codes []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, code := range codes {
		code = strings.TrimSpace(code)
		if code == "" || seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, code)
	}
	return out
}

func safeReadinessCodePart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "unknown"
	}
	return out
}

func signalOK(state string) bool {
	return strings.EqualFold(strings.TrimSpace(state), "ok")
}

func ensureServeWorkspace(ctx context.Context, store *sqlite.Store, workspaceID, workspacePassword string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		workspaceID = "rhizome-main"
	}
	if _, err := store.GetWorkspace(ctx, workspaceID); err == nil {
		return nil
	} else if !errors.Is(err, sqlite.ErrWorkspaceNotFound) {
		return err
	}
	workspacePassword = strings.TrimSpace(workspacePassword)
	if workspacePassword == "" {
		return errors.New("RHIZOME_WORKSPACE_PASSWORD is required when creating the initial workspace")
	}
	return store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID:       workspaceID,
		Title:             workspaceID,
		Description:       "Default Rhizome workspace",
		CreatedBy:         "serve-bootstrap",
		Status:            model.WorkspaceStatusActive,
		WorkspacePassword: workspacePassword,
	})
}

func startServeMemoryProjectionReconciler(ctx context.Context, store *sqlite.Store, readiness *ReadinessRegistry, supervisor *serveLoopSupervisor) {
	supervisor.Go(func() {
		if readiness != nil {
			readiness.SetState(loopNameMemoryProjection, LoopRunning)
		}
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic: %v", r)
				log.Printf("[FATAL] memory projection reconciler panic recovered: %v", r)
				if readiness != nil {
					readiness.SetError(loopNameMemoryProjection, err)
				}
			}
		}()

		run := func() {
			processed, failed, err := runServeMemoryProjectionReconcilerOnce(ctx, store)
			if err != nil {
				log.Printf("[memory-projection] reconcile failed after processed=%d failed=%d: %v", processed, failed, err)
				recordMemoryProjectionReconcilerResult(readiness, err)
				return
			}
			recordMemoryProjectionReconcilerResult(readiness, nil)
			if processed > 0 || failed > 0 {
				log.Printf("[memory-projection] reconciled processed=%d failed=%d", processed, failed)
			}
		}

		run()
		ticker := time.NewTicker(serveProjectionSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				if readiness != nil {
					readiness.SetState(loopNameMemoryProjection, LoopStopped)
				}
				return
			case <-ticker.C:
				run()
			}
		}
	})
}

func recordMemoryProjectionReconcilerResult(readiness *ReadinessRegistry, err error) {
	if readiness == nil {
		return
	}
	if err != nil {
		readiness.SetDegraded(loopNameMemoryProjection, err)
		return
	}
	readiness.SetSuccess(loopNameMemoryProjection)
}

func syncMemoryProjectionReadiness(readiness *ReadinessRegistry, now time.Time) {
	if readiness == nil {
		return
	}
	loop := readiness.Get(loopNameMemoryProjection)
	if loop == nil || loop.State != LoopRunning {
		return
	}
	if strings.TrimSpace(loop.LastSuccess) == "" {
		readiness.SetDegraded(
			loopNameMemoryProjection,
			errors.New("memory projection loop has not reported a successful sweep"),
		)
		return
	}
	lastSuccess, err := time.Parse(time.RFC3339Nano, loop.LastSuccess)
	if err != nil {
		readiness.SetDegraded(
			loopNameMemoryProjection,
			fmt.Errorf("memory projection loop has invalid last_success timestamp %q", loop.LastSuccess),
		)
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if now.UTC().Sub(lastSuccess.UTC()) > serveMemoryProjectionReadinessStaleAfter {
		readiness.SetDegraded(
			loopNameMemoryProjection,
			fmt.Errorf("memory projection loop has not completed a successful sweep since %s", loop.LastSuccess),
		)
	}
}

// startServeOperatorQueueProjectionReconciler runs the CA-05/AUD-038 auto-repair
// sweep: it periodically recreates operator queues that are missing for sessions
// in a blocking phase (so an unattended run self-heals instead of leaving a stuck
// agent silently invisible). Mirrors the memory-projection loop's supervision.
func startServeOperatorQueueProjectionReconciler(ctx context.Context, store *sqlite.Store, readiness *ReadinessRegistry, supervisor *serveLoopSupervisor) {
	supervisor.Go(func() {
		if readiness != nil {
			readiness.SetState(loopNameOperatorQueueProjection, LoopRunning)
		}
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic: %v", r)
				log.Printf("[FATAL] operator queue projection reconciler panic recovered: %v", r)
				if readiness != nil {
					readiness.SetError(loopNameOperatorQueueProjection, err)
				}
			}
		}()

		run := func() {
			result, err := store.ReconcileMissingSessionOperatorQueues(ctx, serveOperatorQueueProjectionSweepLimit)
			if err != nil {
				log.Printf("[operator-queue-projection] reconcile failed after missing=%d repaired=%d failed=%d: %v", result.Missing, result.Repaired, result.Failed, err)
				recordOperatorQueueProjectionReconcilerResult(readiness, err)
				return
			}
			recordOperatorQueueProjectionReconcilerResult(readiness, nil)
			if result.Repaired > 0 || result.Failed > 0 {
				log.Printf("[operator-queue-projection] reconciled missing=%d repaired=%d failed=%d unchanged=%d", result.Missing, result.Repaired, result.Failed, result.Unchanged)
			}
		}

		run()
		ticker := time.NewTicker(serveOperatorQueueProjectionSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				if readiness != nil {
					readiness.SetState(loopNameOperatorQueueProjection, LoopStopped)
				}
				return
			case <-ticker.C:
				run()
			}
		}
	})
}

func recordOperatorQueueProjectionReconcilerResult(readiness *ReadinessRegistry, err error) {
	if readiness == nil {
		return
	}
	if err != nil {
		readiness.SetDegraded(loopNameOperatorQueueProjection, err)
		return
	}
	readiness.SetSuccess(loopNameOperatorQueueProjection)
}

func syncOperatorQueueProjectionReadiness(readiness *ReadinessRegistry, now time.Time) {
	if readiness == nil {
		return
	}
	loop := readiness.Get(loopNameOperatorQueueProjection)
	if loop == nil || loop.State != LoopRunning {
		return
	}
	if strings.TrimSpace(loop.LastSuccess) == "" {
		readiness.SetDegraded(
			loopNameOperatorQueueProjection,
			errors.New("operator queue projection loop has not reported a successful sweep"),
		)
		return
	}
	lastSuccess, err := time.Parse(time.RFC3339Nano, loop.LastSuccess)
	if err != nil {
		readiness.SetDegraded(
			loopNameOperatorQueueProjection,
			fmt.Errorf("operator queue projection loop has invalid last_success timestamp %q", loop.LastSuccess),
		)
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if now.UTC().Sub(lastSuccess.UTC()) > serveOperatorQueueProjectionReadinessStaleAfter {
		readiness.SetDegraded(
			loopNameOperatorQueueProjection,
			fmt.Errorf("operator queue projection loop has not completed a successful sweep since %s", loop.LastSuccess),
		)
	}
}

func startServeGovernanceDeadlineLoop(ctx context.Context, store *sqlite.Store, readiness *ReadinessRegistry, supervisor *serveLoopSupervisor) {
	supervisor.Go(func() {
		if readiness != nil {
			readiness.SetState(loopNameGovernanceDeadline, LoopRunning)
		}
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic: %v", r)
				log.Printf("[FATAL] governance deadline loop panic recovered: %v", r)
				if readiness != nil {
					readiness.SetError(loopNameGovernanceDeadline, err)
				}
			}
		}()

		run := func() {
			result, err := store.SweepExpiredGovernanceChallengesWithEvent(ctx, serveGovernanceDeadlineSweepLimit)
			if err != nil {
				log.Printf("[governance-deadline] sweep failed after scanned=%d resolved=%d failed=%d: %v", result.Scanned, result.Resolved, result.Failed, err)
				if readiness != nil {
					readiness.SetDegraded(loopNameGovernanceDeadline, err)
				}
				return
			}
			if readiness != nil {
				readiness.SetSuccess(loopNameGovernanceDeadline)
			}
			if result.Resolved > 0 || result.Failed > 0 {
				log.Printf("[governance-deadline] swept scanned=%d resolved=%d failed=%d", result.Scanned, result.Resolved, result.Failed)
			}
		}

		run()
		ticker := time.NewTicker(serveGovernanceDeadlineSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				if readiness != nil {
					readiness.SetState(loopNameGovernanceDeadline, LoopStopped)
				}
				return
			case <-ticker.C:
				run()
			}
		}
	})
}

func runServeMemoryProjectionReconcilerOnce(ctx context.Context, store *sqlite.Store) (int, int, error) {
	reclaimed, err := store.ReclaimStaleMemoryProjectionProcessing(ctx, serveProjectionProcessingStaleAge, serveProjectionSweepBatchSize)
	if err != nil {
		return 0, 0, fmt.Errorf("reclaim stale memory projection processing: %w", err)
	}
	workspaces, err := store.ListMemoryProjectionPendingWorkspaces(ctx, serveProjectionSweepMaxWorkspaces)
	if err != nil {
		return 0, 0, fmt.Errorf("list memory projection backlog: %w", err)
	}
	processed := reclaimed
	failed := 0
	var firstErr error
	for _, workspaceID := range workspaces {
		result, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, serveProjectionSweepBatchSize)
		processed += result.Processed
		failed += result.Failed
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("reconcile memory projection workspace %s: %w", workspaceID, err)
		}
	}
	return processed, failed, firstErr
}

func ensureServeRuntimeFiles(cfg app.Config, timestamp time.Time) error {
	workspaceRoot := strings.TrimSpace(cfg.WorkspaceRoot)
	if workspaceRoot != "" {
		if err := os.MkdirAll(filepath.Join(workspaceRoot, "shared"), 0o755); err != nil {
			return fmt.Errorf("create workspace shared dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Join(workspaceRoot, "state"), 0o755); err != nil {
			return fmt.Errorf("create workspace state dir: %w", err)
		}
	}
	return writeServeStartupMetricsSnapshot(cfg.MetricsPath, timestamp)
}

func writeServeStartupMetricsSnapshot(metricsPath string, timestamp time.Time) error {
	metricsPath = strings.TrimSpace(metricsPath)
	if metricsPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(metricsPath), 0o755); err != nil {
		return fmt.Errorf("create metrics dir: %w", err)
	}
	snapshot := runtimeMetricsSnapshot{
		SchemaVersion: "1.0",
		Timestamp:     timestamp.UTC().Format(time.RFC3339Nano),
		Profiles: map[string]runtimeProfileMetrics{
			"serve_startup": {
				TotalRuns:    1,
				SuccessCount: 1,
				FailureCount: 0,
				TimeoutCount: 0,
				FailureRate:  0,
				AvgDuration:  0,
				AvgStartupMS: 0,
			},
		},
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode serve startup metrics: %w", err)
	}
	file, err := os.OpenFile(metricsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open metrics file: %w", err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("write serve startup metrics: %w", err)
	}
	return nil
}

func runDaemonLoop(ctx context.Context, store *sqlite.Store, cfg app.Config, pollMS, maxNodes, nodeTimeoutSec int) error {
	runner, err := newDaemonRunner(store, cfg, maxNodes, nodeTimeoutSec, "serve-daemon")
	if err != nil {
		return fmt.Errorf("create daemon runner: %w", err)
	}

	ticker := time.NewTicker(time.Duration(pollMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		tickCtx, tickCancel := context.WithTimeout(ctx, 5*time.Minute)
		_, tickErr := runner.RunOnce(tickCtx)
		tickCancel()
		if tickErr != nil && !errors.Is(tickErr, context.Canceled) {
			log.Printf("daemon tick error: %v", tickErr)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func legacyRSPHandoffEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_ENABLE_LEGACY_RSP_HANDOFF"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
