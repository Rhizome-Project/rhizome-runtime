//go:build !unix && !windows

package tools

import "os/exec"

type fallbackToolProcessGuard struct{}

func newToolProcessGuard() toolProcessGuard {
	return &fallbackToolProcessGuard{}
}

func (g *fallbackToolProcessGuard) prepare(cmd *exec.Cmd) {}

func (g *fallbackToolProcessGuard) afterStart(cmd *exec.Cmd) error {
	return nil
}

func (g *fallbackToolProcessGuard) terminate(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}

func (g *fallbackToolProcessGuard) close() error {
	return nil
}
