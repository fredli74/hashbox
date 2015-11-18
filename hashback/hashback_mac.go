//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// +build darwin

package main

import (
	"bufio"
	"os/exec"
)

func addMacIgnore() {
	platformList := []string{
		"$TMPDIR",
		".Spotlight-V*",
		"/.Trashes/",
		"/cores/", // Core dumps
		"/Library/Caches/",
		"/net/", //
		"/Network/",
		"/private/tmp/",
		"/System/",
		"/Users/*/.Trash/",
		"/Users/*/Library/Caches/",
		"/Volumes/",
	}

	DefaultIgnoreList = append(DefaultIgnoreList, platformList...)

	// Collect from mdfind on Mac
	func() {
		cmd := exec.Command("mdfind", "com_apple_backup_excludeItem = 'com.apple.backupd'")
		cmdReader, err := cmd.StdoutPipe()
		if err != nil {
			return
		}
		if cmd.Start() != nil {
			return
		}

		var osXcludeList []string
		scanner := bufio.NewScanner(cmdReader)
		for scanner.Scan() {
			osXcludeList = append(osXcludeList, scanner.Text())
		}
		if cmd.Wait() == nil {
			DefaultIgnoreList = append(DefaultIgnoreList, osXcludeList...)
		}
	}()
}

func init() {
	addMacIgnore()
}
