//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// +build windows

package main

func addWindowsIgnore() {
	platformList := []string{
		"$TMP",
		"$TEMP",
		"Thumbs.db",
		"$$RECYCLE.BIN\\",
		"System Volume Information\\",
		"*\\pagefile.sys",
		"*\\swapfile.sys",
		"*\\hiberfil.sys",
		"*\\Windows.old\\",
		"*\\Windows\\Temp\\",
		"*\\$$Windows.*",

		"$USERPROFILE\\Local Settings\\Temporary Internet Files\\",
	}

	DefaultIgnoreList = append(DefaultIgnoreList, platformList...)
}

func init() {
	addWindowsIgnore()
}
