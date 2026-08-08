package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRootCLIHelpExitsSuccessfully(t *testing.T) {
	t.Parallel()

	for _, arg := range []string{"help", "-h", "--help"} {
		arg := arg
		t.Run(arg, func(t *testing.T) {
			t.Parallel()
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := runRootCLI([]string{arg}, &stdout, &stderr); code != 0 {
				t.Fatalf("runRootCLI(%q) exit code = %d, want 0; stderr=%q", arg, code, stderr.String())
			}
			if !strings.Contains(stdout.String(), "rhizome serve") {
				t.Fatalf("runRootCLI(%q) output did not contain root usage: %q", arg, stdout.String())
			}
			if strings.Contains(stdout.String(), "rhizome living") {
				t.Fatalf("runRootCLI(%q) exposed the deprecated living command: %q", arg, stdout.String())
			}
		})
	}
}

func TestRunRootCLISubcommandHelpExitsSuccessfully(t *testing.T) {
	for _, command := range []string{"task", "workspace", "agent", "auth", "tool", "finops", "approval", "daemon", "runtime", "audit", "backup", "config"} {
		for _, help := range []string{"help", "-h", "--help"} {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := runRootCLI([]string{command, help}, &stdout, &stderr); code != 0 {
				t.Fatalf("runRootCLI(%s %s) exit code = %d, want 0; stderr=%q", command, help, code, stderr.String())
			}
			if !strings.Contains(stdout.String(), "rhizome "+command) {
				t.Fatalf("runRootCLI(%s %s) output did not contain group usage: %q", command, help, stdout.String())
			}
		}
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runRootCLI([]string{"serve", "--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("runRootCLI(serve --help) exit code = %d, want 0; stderr=%q", code, stderr.String())
	}
}

func TestRunRootCLIDoesNotDispatchDeprecatedLivingCommand(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runRootCLI([]string{"living", "run"}, &stdout, &stderr); code != 1 {
		t.Fatalf("runRootCLI(living run) exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: living") {
		t.Fatalf("runRootCLI(living run) stderr = %q, want unknown-command error", stderr.String())
	}
}

func TestParseOperatorIDs(t *testing.T) {
	t.Parallel()

	got := parseOperatorIDs("alice, bob ,,carol")
	if len(got) != 3 {
		t.Fatalf("expected 3 operators, got %d", len(got))
	}
	if _, ok := got["alice"]; !ok {
		t.Fatalf("expected alice in operator set")
	}
	if _, ok := got["bob"]; !ok {
		t.Fatalf("expected bob in operator set")
	}
	if _, ok := got["carol"]; !ok {
		t.Fatalf("expected carol in operator set")
	}
}

func TestEnsureApprovalActorAuthorized(t *testing.T) {
	t.Setenv("RHIZOME_OPERATOR_IDS", "alice,bob")

	if err := ensureApprovalActorAuthorized("alice"); err != nil {
		t.Fatalf("expected alice authorized, got error: %v", err)
	}
	if err := ensureApprovalActorAuthorized("carol"); err == nil {
		t.Fatalf("expected carol unauthorized")
	}
}

func TestEnsureApprovalActorAuthorized_DefaultOperator(t *testing.T) {
	t.Setenv("RHIZOME_OPERATOR_IDS", "")

	if err := ensureApprovalActorAuthorized("operator-1"); err != nil {
		t.Fatalf("expected operator-1 authorized by default, got error: %v", err)
	}
	if err := ensureApprovalActorAuthorized("developer"); err == nil {
		t.Fatalf("expected developer unauthorized when RHIZOME_OPERATOR_IDS is empty")
	}
}

func TestNormalizeOutputFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "default empty", input: "", want: outputFormatJSON},
		{name: "json", input: "json", want: outputFormatJSON},
		{name: "json uppercase", input: "JSON", want: outputFormatJSON},
		{name: "jsonl", input: "jsonl", want: outputFormatJSONL},
		{name: "invalid", input: "yaml", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeOutputFormat(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}
