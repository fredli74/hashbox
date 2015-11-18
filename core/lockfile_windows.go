//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// +build windows

package core

import (
	"os"
)

func ProcessRunning(pid int) bool {
	_, err := os.FindProcess(pid)
	return err == nil
}
