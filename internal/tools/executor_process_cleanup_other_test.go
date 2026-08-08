//go:build !unix && !windows

package tools

func isProcessAlive(pid int) (bool, error) {
	return false, nil
}
