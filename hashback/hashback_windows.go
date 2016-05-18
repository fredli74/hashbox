//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// +build windows

package main

import (
	"strings"
	"os"
)

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

// TODO: Add other windows filename restrictions
// CON, PRN, AUX, NUL, COM1, COM2, COM3, COM4, COM5, COM6, COM7, COM8, COM9, LPT1, LPT2, LPT3, LPT4, LPT5, LPT6, LPT7, LPT8, and LPT9.
// Filenames containing only spaces or periods
func platformSafeFilename(src string) (dst string) {
	r := strings.NewReplacer(
		"\\", "_",
		"/", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	dst = r.Replace(src)
	return dst
}

func userHomeFolder() string {
	drive := os.Getenv("HOMEDRIVE");
	path := os.Getenv("HOMEPATH");
	if drive == "" || path == "" {
		return os.Getenv("USERPROFILE")
	} else {
		return drive + path
	}
}
