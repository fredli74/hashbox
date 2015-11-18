//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package core

import (
	"os"
)

func ProcessRunning(pid int) bool {
	p, _ := os.FindProcess(pid)   // On unix the FindProcess never returns an error
	err := p.Signal(os.Interrupt) // Returns error if process is not running
	return err == nil
}
