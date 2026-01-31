//go:build !windows

package core

import (
	"os"
	"strconv"
	"syscall"
)

// ApplyEnvUMASK reads an octal umask value from env and applies it.
func ApplyEnvUMASK() {
	umask := os.Getenv("UMASK")
	if umask == "" {
		umask = "077"
	}
	v, err := strconv.ParseUint(umask, 8, 16)
	if err != nil || v > 0o777 {
		Abort("Invalid UMASK %q", umask)
	}
	syscall.Umask(int(v))
}
