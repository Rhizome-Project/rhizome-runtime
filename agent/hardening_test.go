package main

import (
	"testing"
)

func TestShellCommandForGOOS(t *testing.T) {
	tests := []struct {
		goos string
		name string
		args []string
	}{
		{goos: "linux", name: "sh", args: []string{"-c", "echo hi"}},
		{goos: "darwin", name: "sh", args: []string{"-c", "echo hi"}},
		{goos: "windows", name: "cmd", args: []string{"/C", "echo hi"}},
	}

	for _, tc := range tests {
		name, args := shellCommandForGOOS(tc.goos, "echo hi")
		if name != tc.name {
			t.Fatalf("%s: name = %q, want %q", tc.goos, name, tc.name)
		}
		if len(args) != len(tc.args) {
			t.Fatalf("%s: args = %#v, want %#v", tc.goos, args, tc.args)
		}
		for i := range args {
			if args[i] != tc.args[i] {
				t.Fatalf("%s: args[%d] = %q, want %q", tc.goos, i, args[i], tc.args[i])
			}
		}
	}
}

func TestBrowserCommandForGOOS(t *testing.T) {
	tests := []struct {
		goos string
		name string
		args []string
	}{
		{goos: "linux", name: "xdg-open", args: []string{"https://example.com?a=1&b=2"}},
		{goos: "darwin", name: "open", args: []string{"https://example.com?a=1&b=2"}},
		{goos: "windows", name: "rundll32", args: []string{"url.dll,FileProtocolHandler", "https://example.com?a=1&b=2"}},
	}

	for _, tc := range tests {
		name, args := browserCommandForGOOS(tc.goos, "https://example.com?a=1&b=2")
		if name != tc.name {
			t.Fatalf("%s: name = %q, want %q", tc.goos, name, tc.name)
		}
		if len(args) != len(tc.args) {
			t.Fatalf("%s: args = %#v, want %#v", tc.goos, args, tc.args)
		}
		for i := range args {
			if args[i] != tc.args[i] {
				t.Fatalf("%s: args[%d] = %q, want %q", tc.goos, i, args[i], tc.args[i])
			}
		}
	}
}
