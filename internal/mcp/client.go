package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const stdioStderrLimit = 4096

// Client communicates with external MCP servers using the Streamable HTTP transport.
type Client struct {
	httpClient *http.Client
}

// NewClient creates a new MCP Client.
func NewClient() *Client {
	return &Client{
		// Streamable HTTP requests inherit their timeout from the caller context so
		// routed tool.call does not get silently undercut by a second client budget.
		httpClient: &http.Client{},
	}
}

// JSONRPCRequest is a JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      any    `json:"id"`
}

// JSONRPCResponse is a JSON-RPC 2.0 response.
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
	ID      any             `json:"id"`
}

// JSONRPCError is a JSON-RPC 2.0 error.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Tool describes a tool exposed by an MCP server.
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
}

// ToolsListResult is the result of tools/list.
type ToolsListResult struct {
	Tools []Tool `json:"tools"`
}

// ToolCallResult is the result of tools/call.
type ToolCallResult struct {
	Content []ToolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// ToolContent is a content block in a tool call result.
type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// InitializeResult is the result of initialize.
type InitializeResult struct {
	ProtocolVersion string         `json:"protocolVersion"`
	ServerInfo      ServerInfo     `json:"serverInfo"`
	Capabilities    map[string]any `json:"capabilities"`
}

// ServerInfo describes the MCP server.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// sendRequest sends a JSON-RPC request to the MCP server.
func (c *Client) sendRequest(ctx context.Context, url string, headers map[string]string, req JSONRPCRequest) (*JSONRPCResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("MCP server returned %d: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
	}

	contentType := resp.Header.Get("Content-Type")

	// Handle SSE response (text/event-stream)
	if strings.HasPrefix(contentType, "text/event-stream") {
		return c.readSSEResponse(resp.Body)
	}

	// Handle direct JSON response
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var rpcResp JSONRPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &rpcResp, nil
}

// readSSEResponse reads a JSON-RPC response from an SSE stream.
func (c *Client) readSSEResponse(r io.Reader) (*JSONRPCResponse, error) {
	buf, err := io.ReadAll(io.LimitReader(r, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read SSE stream: %w", err)
	}

	// Parse SSE events, looking for data: lines
	lines := strings.Split(string(buf), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		var rpcResp JSONRPCResponse
		if err := json.Unmarshal([]byte(data), &rpcResp); err != nil {
			continue
		}
		if rpcResp.ID != nil {
			return &rpcResp, nil
		}
	}

	return nil, fmt.Errorf("no JSON-RPC response found in SSE stream")
}

// Initialize performs the MCP initialize handshake.
func (c *Client) Initialize(ctx context.Context, url string, headers map[string]string) (*InitializeResult, error) {
	resp, err := c.sendRequest(ctx, url, headers, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "initialize",
		Params: map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "rhizome",
				"version": "1.0.0",
			},
		},
		ID: "init-1",
	})
	if err != nil {
		return nil, fmt.Errorf("initialize: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("initialize error: %s", resp.Error.Message)
	}

	var result InitializeResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("decode initialize result: %w", err)
	}

	// Send initialized notification
	_, _ = c.sendRequest(ctx, url, headers, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	})

	return &result, nil
}

// ListTools calls tools/list on the MCP server.
func (c *Client) ListTools(ctx context.Context, url string, headers map[string]string) ([]Tool, error) {
	resp, err := c.sendRequest(ctx, url, headers, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/list",
		ID:      "tools-list-1",
	})
	if err != nil {
		return nil, fmt.Errorf("list tools: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("list tools error: %s", resp.Error.Message)
	}

	var result ToolsListResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("decode tools list: %w", err)
	}
	return result.Tools, nil
}

// CallTool calls tools/call on the MCP server.
func (c *Client) CallTool(ctx context.Context, url string, headers map[string]string, toolName string, arguments map[string]any) (*ToolCallResult, error) {
	resp, err := c.sendRequest(ctx, url, headers, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/call",
		Params: map[string]any{
			"name":      toolName,
			"arguments": arguments,
		},
		ID: "tool-call-1",
	})
	if err != nil {
		return nil, fmt.Errorf("call tool: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("call tool error: %s", resp.Error.Message)
	}

	var result ToolCallResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("decode tool call result: %w", err)
	}
	return &result, nil
}

// ── Stdio Transport ─────────────────────────────────────────────────

// StdioClient manages an MCP server subprocess and communicates via stdin/stdout.
type StdioClient struct {
	cmd             *exec.Cmd
	stdin           io.WriteCloser
	reader          *bufio.Reader
	mu              sync.Mutex
	stateMu         sync.Mutex
	stdoutClosed    bool
	stderrMu        sync.Mutex
	stderrBuf       []byte
	stderrDone      chan struct{}
	nextID          int64
	pendingRequests map[any]chan *JSONRPCResponse
	pendingMu       sync.Mutex
	cancelPump      context.CancelFunc
}

// StdioTransportDiagnostics is bounded process evidence attached to stdio
// transport failures so callers can persist useful operation-ledger context.
type StdioTransportDiagnostics struct {
	Stderr       string
	StdoutClosed bool
}

// StdioTransportError wraps stdio MCP transport failures with bounded stderr
// and child/pipe state. It preserves the original cause for errors.Is checks.
type StdioTransportError struct {
	Operation   string
	Cause       error
	Diagnostics StdioTransportDiagnostics
}

func (e *StdioTransportError) Error() string {
	operation := strings.TrimSpace(e.Operation)
	if operation == "" {
		operation = "stdio transport"
	}
	parts := []string{operation}
	if e.Cause != nil {
		parts = append(parts, e.Cause.Error())
	}
	if e.Diagnostics.StdoutClosed {
		parts = append(parts, "stdout_closed=true")
	}
	if stderr := strings.TrimSpace(e.Diagnostics.Stderr); stderr != "" {
		parts = append(parts, "stderr="+stderr)
	}
	return strings.Join(parts, ": ")
}

func (e *StdioTransportError) Unwrap() error {
	return e.Cause
}

// StdioDiagnosticsFromError extracts stdio subprocess diagnostics from a
// wrapped error, when present.
func StdioDiagnosticsFromError(err error) (StdioTransportDiagnostics, bool) {
	var transportErr *StdioTransportError
	if errors.As(err, &transportErr) {
		return transportErr.Diagnostics, true
	}
	return StdioTransportDiagnostics{}, false
}

// NewStdioClient creates a new StdioClient but does not start the process.
func NewStdioClient() *StdioClient {
	return &StdioClient{
		pendingRequests: make(map[any]chan *JSONRPCResponse),
	}
}

// Start launches the MCP server subprocess.
func (sc *StdioClient) Start(command string, args []string, env map[string]string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.stateMu.Lock()
	sc.stdoutClosed = false
	sc.stateMu.Unlock()
	sc.stderrMu.Lock()
	sc.stderrBuf = nil
	sc.stderrMu.Unlock()
	sc.stderrDone = make(chan struct{})

	sc.cmd = exec.Command(command, args...)
	sc.cmd.Env = os.Environ()
	for k, v := range env {
		sc.cmd.Env = append(sc.cmd.Env, k+"="+v)
	}

	var err error
	sc.stdin, err = sc.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}

	stdout, err := sc.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	sc.reader = bufio.NewReader(stdout)

	stderr, err := sc.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := sc.cmd.Start(); err != nil {
		return fmt.Errorf("start process: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sc.cancelPump = cancel
	go sc.pumpReader(ctx)
	go sc.pumpStderr(stderr)

	return nil
}

func normalizeJSONRPCID(id any) any {
	switch v := id.(type) {
	case float64:
		asInt := int64(v)
		if v == float64(asInt) {
			return asInt
		}
		return v
	default:
		return id
	}
}

func (sc *StdioClient) appendStderr(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	sc.stderrMu.Lock()
	defer sc.stderrMu.Unlock()
	remaining := stdioStderrLimit - len(sc.stderrBuf)
	if remaining <= 0 {
		return
	}
	if len(chunk) > remaining {
		chunk = chunk[:remaining]
	}
	sc.stderrBuf = append(sc.stderrBuf, chunk...)
}

func (sc *StdioClient) stderrExcerpt() string {
	sc.stderrMu.Lock()
	defer sc.stderrMu.Unlock()
	return strings.TrimSpace(string(sc.stderrBuf))
}

func (sc *StdioClient) stderrExcerptAfterBriefDrain() string {
	if excerpt := sc.stderrExcerpt(); excerpt != "" {
		return excerpt
	}
	done := sc.stderrDone
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return sc.stderrExcerpt()
		case <-ticker.C:
			if excerpt := sc.stderrExcerpt(); excerpt != "" {
				return excerpt
			}
		case <-timer.C:
			return sc.stderrExcerpt()
		}
	}
}

func (sc *StdioClient) markStdoutClosed() {
	sc.stateMu.Lock()
	sc.stdoutClosed = true
	sc.stateMu.Unlock()
}

func (sc *StdioClient) stdoutClosedSnapshot() bool {
	sc.stateMu.Lock()
	defer sc.stateMu.Unlock()
	return sc.stdoutClosed
}

func (sc *StdioClient) diagnosticsSnapshot() StdioTransportDiagnostics {
	return StdioTransportDiagnostics{
		Stderr:       sc.stderrExcerptAfterBriefDrain(),
		StdoutClosed: sc.stdoutClosedSnapshot(),
	}
}

func (sc *StdioClient) transportError(operation string, cause error) error {
	return &StdioTransportError{
		Operation:   operation,
		Cause:       cause,
		Diagnostics: sc.diagnosticsSnapshot(),
	}
}

func (sc *StdioClient) closePendingRequests() {
	sc.pendingMu.Lock()
	defer sc.pendingMu.Unlock()
	for id, ch := range sc.pendingRequests {
		close(ch)
		delete(sc.pendingRequests, id)
	}
}

func (sc *StdioClient) pumpStderr(r io.Reader) {
	defer func() {
		if sc.stderrDone != nil {
			close(sc.stderrDone)
		}
	}()
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			sc.appendStderr(buf[:n])
		}
		if err != nil {
			return
		}
	}
}

// pumpReader asynchronously reads from stdout and dispatches responses.
func (sc *StdioClient) pumpReader(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line, err := sc.reader.ReadBytes('\n')
		if err != nil {
			sc.markStdoutClosed()
			sc.closePendingRequests()
			return
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		var resp JSONRPCResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			continue // skip malformed
		}
		if resp.ID != nil {
			respID := normalizeJSONRPCID(resp.ID)
			sc.pendingMu.Lock()
			ch, ok := sc.pendingRequests[respID]
			if ok {
				delete(sc.pendingRequests, respID)
			}
			sc.pendingMu.Unlock()
			if ok {
				select {
				case ch <- &resp:
				default:
				}
				close(ch)
			}
		}
	}
}

// Stop terminates the subprocess.
func (sc *StdioClient) Stop() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.cancelPump != nil {
		sc.cancelPump()
	}

	if sc.stdin != nil {
		_ = sc.stdin.Close()
	}
	if sc.cmd != nil && sc.cmd.Process != nil {
		_ = sc.cmd.Process.Kill()
		_ = sc.cmd.Wait()
	}

	sc.closePendingRequests()
}

// sendStdioRequest sends a JSON-RPC request via stdin and waits for the response contextually.
func (sc *StdioClient) sendStdioRequest(ctx context.Context, req JSONRPCRequest) (*JSONRPCResponse, error) {
	respChan := make(chan *JSONRPCResponse, 1)

	reqID := normalizeJSONRPCID(req.ID)
	if req.ID != nil {
		sc.pendingMu.Lock()
		sc.pendingRequests[reqID] = respChan
		sc.pendingMu.Unlock()

		defer func() {
			sc.pendingMu.Lock()
			delete(sc.pendingRequests, reqID)
			sc.pendingMu.Unlock()
		}()
	} else {
		close(respChan) // Notifications won't get a response
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Write request + newline
	body = append(body, '\n')

	sc.mu.Lock()
	_, err = sc.stdin.Write(body)
	sc.mu.Unlock()

	if err != nil {
		return nil, sc.transportError("write stdin", err)
	}

	if req.ID == nil {
		return nil, nil // Notification sent successfully
	}

	select {
	case <-ctx.Done():
		return nil, sc.transportError("request deadline", ctx.Err())
	case resp, ok := <-respChan:
		if !ok || resp == nil {
			return nil, sc.transportError("connection closed", io.ErrClosedPipe)
		}
		return resp, nil
	}
}

func (sc *StdioClient) getNextID() int64 {
	return atomic.AddInt64(&sc.nextID, 1)
}

// Initialize performs MCP initialize over stdio.
func (sc *StdioClient) Initialize(ctx context.Context) (*InitializeResult, error) {
	resp, err := sc.sendStdioRequest(ctx, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "initialize",
		Params: map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "rhizome",
				"version": "1.0.0",
			},
		},
		ID: sc.getNextID(),
	})
	if err != nil {
		return nil, fmt.Errorf("initialize: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("initialize error: %s", resp.Error.Message)
	}

	var result InitializeResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("decode initialize result: %w", err)
	}

	// Send initialized notification (no ID = notification)
	_, _ = sc.sendStdioRequest(ctx, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	})

	return &result, nil
}

// ListTools calls tools/list over stdio.
func (sc *StdioClient) ListTools(ctx context.Context) ([]Tool, error) {
	resp, err := sc.sendStdioRequest(ctx, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/list",
		ID:      sc.getNextID(),
	})
	if err != nil {
		return nil, fmt.Errorf("list tools: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("list tools error: %s", resp.Error.Message)
	}

	var result ToolsListResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("decode tools list: %w", err)
	}
	return result.Tools, nil
}

// CallTool calls tools/call over stdio.
func (sc *StdioClient) CallTool(ctx context.Context, toolName string, arguments map[string]any) (*ToolCallResult, error) {
	resp, err := sc.sendStdioRequest(ctx, JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/call",
		Params: map[string]any{
			"name":      toolName,
			"arguments": arguments,
		},
		ID: sc.getNextID(),
	})
	if err != nil {
		return nil, fmt.Errorf("call tool: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("call tool error: %s", resp.Error.Message)
	}

	var result ToolCallResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("decode tool call result: %w", err)
	}
	return &result, nil
}
