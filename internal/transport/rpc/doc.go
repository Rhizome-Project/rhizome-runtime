// Package rpc contains transport-level helpers for specific bridge envelopes.
//
// It is not the authority contract for the HTTP JSON-RPC server. Workspace,
// agent, task, project, and patch-queue authority checks live in internal/server
// and need ServeHTTP-level contract tests for each public method surface.
package rpc
