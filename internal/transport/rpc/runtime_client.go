package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

var (
	ErrRuntimeClientCommand = errors.New("runtime client command failed")
	ErrRuntimeClientOutput  = errors.New("runtime client invalid output")
)

type NodeRunRequest struct {
	TaskID         string
	NodeID         string
	RuntimeProfile string
	ScriptRef      string
	TimeoutSec     int
	Env            map[string]string
	CPUs           string
	Memory         string
	TraceID        string
}

type StdioRuntimeClientConfig struct {
	PythonBin    string
	BridgeScript string
	WorkDir      string
	Env          map[string]string
}

type StdioRuntimeClient struct {
	pythonBin    string
	bridgeScript string
	workDir      string
	env          map[string]string
}

func NewStdioRuntimeClient(cfg StdioRuntimeClientConfig) (*StdioRuntimeClient, error) {
	pythonBin := strings.TrimSpace(cfg.PythonBin)
	if pythonBin == "" {
		return nil, errors.New("python bin is required")
	}
	bridgeScript := strings.TrimSpace(cfg.BridgeScript)
	if bridgeScript == "" {
		return nil, errors.New("bridge script is required")
	}
	workDir := strings.TrimSpace(cfg.WorkDir)
	if workDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("resolve runtime client working dir: %w", err)
		}
		workDir = wd
	}

	clientEnv := map[string]string{}
	for k, v := range cfg.Env {
		clientEnv[k] = v
	}
	if _, ok := clientEnv["PYTHONIOENCODING"]; !ok {
		clientEnv["PYTHONIOENCODING"] = "utf-8"
	}

	pyPath := strings.TrimSpace(os.Getenv("PYTHONPATH"))
	if pyPath == "" {
		clientEnv["PYTHONPATH"] = workDir
	} else {
		clientEnv["PYTHONPATH"] = workDir + string(os.PathListSeparator) + pyPath
	}

	return &StdioRuntimeClient{
		pythonBin:    pythonBin,
		bridgeScript: bridgeScript,
		workDir:      workDir,
		env:          clientEnv,
	}, nil
}

func (c *StdioRuntimeClient) RunNode(ctx context.Context, req NodeRunRequest) (ExecutorRunNodeResponse, error) {
	if strings.TrimSpace(req.TaskID) == "" {
		return ExecutorRunNodeResponse{}, errors.New("task_id is required")
	}
	if strings.TrimSpace(req.NodeID) == "" {
		return ExecutorRunNodeResponse{}, errors.New("node_id is required")
	}
	if strings.TrimSpace(req.RuntimeProfile) == "" {
		return ExecutorRunNodeResponse{}, errors.New("runtime_profile is required")
	}
	if strings.TrimSpace(req.ScriptRef) == "" {
		return ExecutorRunNodeResponse{}, errors.New("script_ref is required")
	}
	if req.TimeoutSec <= 0 {
		req.TimeoutSec = 120
	}
	if req.Env == nil {
		req.Env = map[string]string{}
	}
	if strings.TrimSpace(req.CPUs) == "" {
		req.CPUs = "1.0"
	}
	if strings.TrimSpace(req.Memory) == "" {
		req.Memory = "512m"
	}
	if strings.TrimSpace(req.TraceID) == "" {
		req.TraceID = fmt.Sprintf("tr-%d", time.Now().UTC().UnixNano())
	}

	requestID := fmt.Sprintf("exec-%d", time.Now().UTC().UnixNano())
	envelope := map[string]any{
		"jsonrpc": "2.0",
		"method":  "executor.run_node",
		"params": map[string]any{
			"task_id":         req.TaskID,
			"node_id":         req.NodeID,
			"runtime_profile": req.RuntimeProfile,
			"script_ref":      req.ScriptRef,
			"timeout_sec":     req.TimeoutSec,
			"env":             req.Env,
			"cpus":            req.CPUs,
			"memory":          req.Memory,
			"trace_id":        req.TraceID,
		},
		"id": requestID,
	}

	rawReq, err := json.Marshal(envelope)
	if err != nil {
		return ExecutorRunNodeResponse{}, fmt.Errorf("marshal runtime request: %w", err)
	}

	cmd := exec.CommandContext(ctx, c.pythonBin, c.bridgeScript)
	cmd.Dir = c.workDir
	cmd.Stdin = bytes.NewReader(append(rawReq, '\n'))

	stdout := &limitWriter{limit: 4 * 1024 * 1024}
	stderr := &limitWriter{limit: 4 * 1024 * 1024}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	cmd.Env = os.Environ()
	for k, v := range c.env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	if err := cmd.Run(); err != nil {
		return ExecutorRunNodeResponse{}, fmt.Errorf("%w: %v; stderr=%s", ErrRuntimeClientCommand, err, strings.TrimSpace(stderr.String()))
	}

	rawResp, err := lastJSONLine(stdout.String())
	if err != nil {
		return ExecutorRunNodeResponse{}, fmt.Errorf("%w: %v; stdout=%s", ErrRuntimeClientOutput, err, strings.TrimSpace(stdout.String()))
	}

	parsed, err := ParseExecutorRunNodeResponse(rawResp, requestID)
	if err != nil {
		return ExecutorRunNodeResponse{}, err
	}

	return parsed, nil
}

func lastJSONLine(out string) ([]byte, error) {
	lines := strings.Split(out, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		if !strings.Contains(line, `"jsonrpc"`) {
			continue
		}
		return []byte(line), nil
	}
	return nil, errors.New("no json-rpc response found in stdout")
}

// limitWriter is a simple writer that buffers output up to a byte limit, then silently discards the rest.
type limitWriter struct {
	buf   bytes.Buffer
	limit int
	wrote int
}

func (w *limitWriter) Write(p []byte) (n int, err error) {
	if w.wrote >= w.limit {
		return len(p), nil
	}
	allow := w.limit - w.wrote
	if len(p) > allow {
		w.buf.Write(p[:allow])
		w.wrote += len(p)
		return len(p), nil
	}
	w.buf.Write(p)
	w.wrote += len(p)
	return len(p), nil
}

func (w *limitWriter) String() string {
	return w.buf.String()
}
