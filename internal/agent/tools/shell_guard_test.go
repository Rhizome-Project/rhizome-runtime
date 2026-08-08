package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBashToolProgramBGuardDeniesWindowsAndPowerShellMutationSyntax(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "owned.txt")
	commands := []string{
		"Set-Content owned.txt blocked",
		"Add-Content owned.txt blocked",
		"echo blocked > owned.txt",
		"echo blocked >> owned.txt",
		"echo blocked 1> owned.txt",
		"echo blocked 2> owned.txt",
		"echo blocked &> owned.txt",
		"echo blocked >| owned.txt",
		"Get-Content source.txt *> owned.txt",
		"Get-Content source.txt | Out-File owned.txt",
		"New-Item owned.txt -ItemType File",
		"Remove-Item owned.txt",
		"Move-Item owned.txt moved.txt",
		"rm owned.txt",
		"mv owned.txt moved.txt",
		"cmd /c copy NUL owned.txt",
		"write owned.txt blocked",
		"python -c \"open('owned.txt','w').write('blocked')\"",
		"node -e \"require('fs').writeFileSync('owned.txt','blocked')\"",
		"ruby -e \"File.write('owned.txt','blocked')\"",
		"perl -e \"open(my $fh, '>', 'owned.txt'); print $fh 'blocked'\"",
		"powershell -NoProfile -Command \"[IO.File]::WriteAllText('owned.txt','blocked')\"",
		"pwsh -NoProfile -Command \"Set-Content owned.txt blocked\"",
		"git checkout -- owned.txt",
		"git status --short; python -c \"open('owned.txt','w').write('blocked')\"",
		"cat owned.txt && node -e \"require('fs').writeFileSync('owned.txt','blocked')\"",
		"echo $(python -c \"open('owned.txt','w').write('blocked')\")",
	}
	denied := []MutationDenyRecord{}
	tool := &bashTool{cfg: BuiltinConfig{
		WorkspaceDir: dir,
		AllowedTiers: []string{"autonomous"},
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
			RecordDeny: func(record MutationDenyRecord) {
				denied = append(denied, record)
			},
		},
	}}

	for _, command := range commands {
		t.Run(command, func(t *testing.T) {
			result, err := tool.Execute(context.Background(), mustInput(map[string]any{"command": command}))
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}
			if !strings.Contains(result, "Permission Denied") || !strings.Contains(result, DirectRepoMutationDeniedReason) {
				t.Fatalf("expected Program B shell mutation denial, got %q", result)
			}
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatalf("denied shell command should not create target; stat err=%v", err)
			}
		})
	}

	if len(denied) != len(commands) {
		t.Fatalf("expected %d deny records, got %+v", len(commands), denied)
	}
	for _, record := range denied {
		if record.ToolName != "bash" || record.ReasonCode != DirectRepoMutationDeniedReason {
			t.Fatalf("unexpected deny record: %+v", record)
		}
		if !stringSliceContains(record.MissingContext, "repo_lease_id") ||
			!stringSliceContains(record.MissingContext, "lease_term") ||
			!stringSliceContains(record.MissingContext, "patch_queue_id") ||
			!stringSliceContains(record.MissingContext, "patch_queue_item_id") {
			t.Fatalf("expected missing authority context in deny record, got %+v", record.MissingContext)
		}
	}
}

func TestShellCommandReadOnlyGuardDeniesUnallowlistedCommandsInProgramBMode(t *testing.T) {
	t.Parallel()

	cfg := BuiltinConfig{
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
		},
	}
	for _, command := range []string{
		"python --version",
		"go test ./...",
		"git checkout main",
		"node -e \"console.log('hi')\"",
	} {
		err := cfg.EnsureShellCommandReadOnly("bash", command)
		if err == nil || !strings.Contains(err.Error(), DirectRepoMutationDeniedReason) {
			t.Fatalf("expected unallowlisted command %q to be denied, got %v", command, err)
		}
	}
}

func TestShellCommandReadOnlyGuardDeniesCompoundReadOnlyPrefixedCommands(t *testing.T) {
	t.Parallel()

	cfg := BuiltinConfig{
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
		},
	}
	for _, command := range []string{
		"git status --short; python -c \"open('owned.txt','w').write('blocked')\"",
		"cat owned.txt && node -e \"require('fs').writeFileSync('owned.txt','blocked')\"",
		"echo $(python -c \"open('owned.txt','w').write('blocked')\")",
		"Get-Content owned.txt | Select-String needle",
	} {
		err := cfg.EnsureShellCommandReadOnly("bash", command)
		if err == nil || !strings.Contains(err.Error(), "shell_compound_") {
			t.Fatalf("expected compound command %q to be denied, got %v", command, err)
		}
	}
}

func TestShellCommandReadOnlyGuardDeniesAdditionalControlSyntax(t *testing.T) {
	t.Parallel()

	cfg := BuiltinConfig{
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
		},
	}
	for _, command := range []string{
		"git status --short || git diff",
		"git status --short\ngit diff",
		"git status --short\r\ngit diff",
		"git status --short `whoami`",
		"git status --short & git status",
		"git status --short < owned.txt",
		"git status --short { git diff }",
		"git status --short ( git diff )",
	} {
		err := cfg.EnsureShellCommandReadOnly("bash", command)
		if err == nil || !strings.Contains(err.Error(), "shell_compound_") {
			t.Fatalf("expected control command %q to be denied as compound, got %v", command, err)
		}
	}
}

func TestShellCommandReadOnlyGuardDeniesGitWriteOutputFlags(t *testing.T) {
	t.Parallel()

	cfg := BuiltinConfig{
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
		},
	}
	for _, command := range []string{
		"git diff --output owned.txt",
		"git diff --output=owned.txt",
		"git diff -oowned.txt",
		"git log --output owned.txt",
		"git log -oowned.txt",
		"git show --output=owned.txt HEAD",
		"git show -oowned.txt HEAD",
	} {
		err := cfg.EnsureShellCommandReadOnly("bash", command)
		if err == nil || !strings.Contains(err.Error(), DirectRepoMutationDeniedReason) {
			t.Fatalf("expected git output command %q to be denied, got %v", command, err)
		}
	}
}

func TestShellCommandReadOnlyGuardDeniesGitExecutionAndPagerBypasses(t *testing.T) {
	t.Parallel()

	cfg := BuiltinConfig{
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
		},
	}
	for _, command := range []string{
		"git diff --ext-diff",
		"git diff --external-diff",
		"git log --ext-diff -p",
		"git show --textconv HEAD:owned.txt",
		"git status --help",
		"git status --paginate",
		"git status --pager=cat",
		"git diff --config-env=GIT_EXTERNAL_DIFF=RHIZOME_EXTERNAL_DIFF",
		"git diff -c diff.external=owned-writer",
		"git log --exec-path=.",
		"git status --git-dir=../.git",
		"git status --work-tree=..",
	} {
		err := cfg.EnsureShellCommandReadOnly("bash", command)
		if err == nil || !strings.Contains(err.Error(), DirectRepoMutationDeniedReason) {
			t.Fatalf("expected git bypass command %q to be denied, got %v", command, err)
		}
	}
}

func TestShellCommandReadOnlyGuardAllowsReadOnlyCommandsInProgramBMode(t *testing.T) {
	t.Parallel()

	cfg := BuiltinConfig{
		RepoMutation: RepoMutationPolicy{
			RequireAuthority: true,
			DisableDirect:    true,
		},
	}
	for _, command := range []string{
		"Get-Content owned.txt",
		"Select-String pattern owned.txt",
		"dir",
		"git status --short",
		"cat owned.txt",
		"ls -la",
	} {
		if err := cfg.EnsureShellCommandReadOnly("bash", command); err != nil {
			t.Fatalf("expected read-only command %q to be allowed by shell guard, got %v", command, err)
		}
	}
}

func TestShellCommandReadOnlyGuardDoesNotChangeLegacyMode(t *testing.T) {
	t.Parallel()

	cfg := BuiltinConfig{}
	if err := cfg.EnsureShellCommandReadOnly("bash", "Set-Content owned.txt legacy"); err != nil {
		t.Fatalf("legacy shell guard should not block without Program B policy: %v", err)
	}
}
