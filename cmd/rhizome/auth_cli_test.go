package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/auth"
)

func TestValidateAuthLoginOutputMode(t *testing.T) {
	tests := []struct {
		name        string
		noSave      bool
		printAPIKey bool
		wantError   string
	}{
		{name: "saved login", noSave: false, printAPIKey: false},
		{name: "explicit stdout", noSave: true, printAPIKey: true},
		{name: "discarded credential", noSave: true, printAPIKey: false, wantError: "requires --print-api-key"},
		{name: "saved credential cannot be printed", noSave: false, printAPIKey: true, wantError: "requires --no-save"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAuthLoginOutputMode(tt.noSave, tt.printAPIKey)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateAuthLoginOutputMode() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateAuthLoginOutputMode() error = %v, want substring %q", err, tt.wantError)
			}
		})
	}
}

func TestCompleteAuthLoginSavedModeDoesNotPrintAPIKey(t *testing.T) {
	const sentinel = "sk-publication-regression-sentinel"
	credentialPath := filepath.Join(t.TempDir(), "auth.json")

	stdout, stderr, err := captureAuthLoginOutput(t, func() error {
		return completeAuthLogin(&auth.OAuthResult{
			APIKey: sentinel,
			Email:  "researcher@example.test",
		}, false, false, credentialPath)
	})
	if err != nil {
		t.Fatalf("completeAuthLogin() error = %v", err)
	}
	if strings.Contains(stdout, sentinel) || strings.Contains(stderr, sentinel) {
		t.Fatalf("saved login exposed API key: stdout=%q stderr=%q", stdout, stderr)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("saved login wrote unexpected stdout: %q", stdout)
	}

	stored, err := auth.LoadCredentials(credentialPath)
	if err != nil {
		t.Fatalf("LoadCredentials() error = %v", err)
	}
	if stored.APIKey != sentinel {
		t.Fatalf("stored API key = %q, want sentinel", stored.APIKey)
	}
}

func TestCompleteAuthLoginPrintsAPIKeyOnlyInExplicitNoSaveMode(t *testing.T) {
	const sentinel = "sk-explicit-no-save-sentinel"

	stdout, stderr, err := captureAuthLoginOutput(t, func() error {
		return completeAuthLogin(&auth.OAuthResult{APIKey: sentinel}, true, true, "")
	})
	if err != nil {
		t.Fatalf("completeAuthLogin() error = %v", err)
	}
	if stdout != sentinel+"\n" {
		t.Fatalf("explicit no-save stdout = %q, want raw key followed by newline", stdout)
	}
	if strings.Contains(stderr, sentinel) {
		t.Fatalf("explicit no-save duplicated API key on stderr: %q", stderr)
	}
	if !strings.Contains(stderr, "warning") {
		t.Fatalf("explicit no-save stderr = %q, want disclosure warning", stderr)
	}
}

func TestRunAuthLoginRejectsAmbiguousNoSaveModesBeforeOAuth(t *testing.T) {
	for _, args := range [][]string{
		{"--no-save"},
		{"--print-api-key"},
	} {
		err := runAuthLogin(args)
		if err == nil {
			t.Fatalf("runAuthLogin(%v) unexpectedly succeeded", args)
		}
		if !strings.Contains(err.Error(), "requires") {
			t.Fatalf("runAuthLogin(%v) error = %v, want mode guidance", args, err)
		}
	}
}

func TestRunAuthLoginRejectsPositionalArgumentsBeforeOAuth(t *testing.T) {
	for _, args := range [][]string{
		{"unexpected", "--no-save", "--print-api-key"},
		{"--listen-addr", ":1455", "unexpected"},
	} {
		err := runAuthLogin(args)
		if err == nil {
			t.Fatalf("runAuthLogin(%v) unexpectedly succeeded", args)
		}
		if !strings.Contains(err.Error(), "does not accept positional arguments") {
			t.Fatalf("runAuthLogin(%v) error = %v, want positional-argument guidance", args, err)
		}
	}
}

func TestMainUsageDocumentsPairedNoSaveContract(t *testing.T) {
	usagePath := filepath.Join(t.TempDir(), "usage.txt")
	usageFile, err := os.Create(usagePath)
	if err != nil {
		t.Fatalf("create usage capture: %v", err)
	}
	printUsage(usageFile)
	if err := usageFile.Close(); err != nil {
		t.Fatalf("close usage capture: %v", err)
	}
	data, err := os.ReadFile(usagePath)
	if err != nil {
		t.Fatalf("read usage capture: %v", err)
	}
	usage := string(data)
	if !strings.Contains(usage, "--no-save --print-api-key") {
		t.Fatalf("main usage omitted paired no-save contract: %q", usage)
	}
	if strings.Contains(usage, "auth login [--no-save]") {
		t.Fatalf("main usage still advertises ambiguous --no-save mode: %q", usage)
	}
}

func captureAuthLoginOutput(t *testing.T, fn func() error) (stdout, stderr string, runErr error) {
	t.Helper()

	stderrPath := filepath.Join(t.TempDir(), "stderr.txt")
	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		t.Fatalf("create stderr capture: %v", err)
	}
	oldStderr := os.Stderr
	os.Stderr = stderrFile
	defer func() {
		os.Stderr = oldStderr
	}()

	stdout, runErr = captureStdout(t, fn)
	if err := stderrFile.Close(); err != nil {
		t.Fatalf("close stderr capture: %v", err)
	}
	data, err := os.ReadFile(stderrPath)
	if err != nil {
		t.Fatalf("read stderr capture: %v", err)
	}
	return stdout, string(data), runErr
}
