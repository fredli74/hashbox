//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

//go:build darwin

package main

func addMacIgnore() {
	platformList := []string{
		"$TMPDIR",
		".Spotlight-V*",
		"/.Trashes/",
		"/cores/", // Core dumps
		"/Library/Caches/",
		"/Library/Logs/",
		"/net/",
		"/Network/",
		"/private/tmp/",
		"/private/var/folders/", // Standard temp location
		"/private/var/log/",     // System logs
		"/private/var/vm/",      // Swap files
		"/System/",
		"/Users/*/.Trash/",
		"/Users/*/Library/Caches/",
		"/Users/*/Library/Calendars/Calendar Cache/",
		"/Users/*/Library/Logs/",
		"/Volumes/",
	}

	DefaultIgnoreList = append(DefaultIgnoreList, platformList...)

	// Collect from mdfind on Mac
	// Were not really happy about this idea since it does not update if custom ignores
	// are set, it ignores all xcode build dirs and there is no -unignore
	/*func() {
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
	}()*/
}

func init() {
	addMacIgnore()
}
