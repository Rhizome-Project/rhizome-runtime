package tools

import "os/exec"

type toolProcessGuard interface {
	prepare(*exec.Cmd)
	afterStart(*exec.Cmd) error
	terminate(*exec.Cmd) error
	close() error
}
