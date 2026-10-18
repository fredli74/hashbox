//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

//go:build windows

package main

import (
	"os"
	"strings"
	"syscall"
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
// built once, Replace is called for every restored and every backed up entry
var platformReplacer = strings.NewReplacer(
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

func platformSafeFilename(src string) (dst string) {
	return platformReplacer.Replace(src)
}

func userHomeFolder() string {
	drive := os.Getenv("HOMEDRIVE")
	path := os.Getenv("HOMEPATH")
	if drive == "" || path == "" {
		return os.Getenv("USERPROFILE")
	}
	return drive + path
}

// Special detection for Dropbox Smart Sync placeholder files
const FILE_ATTRIBUTE_OFFLINE = 0x1000

func isOfflineFile(fileInfo os.FileInfo) bool {
	sys := fileInfo.Sys().(*syscall.Win32FileAttributeData)
	return sys != nil && sys.FileAttributes&FILE_ATTRIBUTE_OFFLINE == FILE_ATTRIBUTE_OFFLINE
}

// the ones syscall does not already declare on windows
const (
	ERROR_WRITE_PROTECT     = syscall.Errno(19)
	ERROR_SHARING_VIOLATION = syscall.Errno(32)
	ERROR_LOCK_VIOLATION    = syscall.Errno(33)
	ERROR_HANDLE_DISK_FULL  = syscall.Errno(39)
	ERROR_DISK_FULL         = syscall.Errno(112)
	ERROR_NOT_READY         = syscall.Errno(21)
	ERROR_BAD_NETPATH       = syscall.Errno(53)
	ERROR_UNEXP_NET_ERR     = syscall.Errno(59)
)

// the destination itself is unusable, every remaining entry would fail the same way
func isFatalRestoreError(err error) bool {
	return errorIsAny(err,
		// no room, or the volume will not take writes
		ERROR_DISK_FULL, ERROR_HANDLE_DISK_FULL, ERROR_WRITE_PROTECT,
		// the destination itself went away
		ERROR_NOT_READY, ERROR_BAD_NETPATH, ERROR_UNEXP_NET_ERR, syscall.ERROR_NETNAME_DELETED)
}

func minorPathError(r interface{}) error {
	e, ok := r.(*os.PathError)
	if ok && (e.Err == ERROR_SHARING_VIOLATION ||
		e.Err == ERROR_LOCK_VIOLATION ||
		e.Err == syscall.ERROR_ACCESS_DENIED) {
		return e
	}
	return nil
}
