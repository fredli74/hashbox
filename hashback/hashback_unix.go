//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2024
//	+---+´

//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd || solaris

package main

import (
	"os"
	"syscall"
)

func addUnixIgnore() {
	platformList := []string{
		//	"/bin/",  	// User Binaries
		//	"/boot/", 	// Boot Loader Files
		"/dev/*", // Devices
		//	"/etc/",  	// Configuration Files
		//	"/initrd/",
		//	"/lib/",   	// System Libraries
		//	"/opt/",   	// Optional add-on Apps
		"/proc/*", // Process Information
		//	"/sbin/",  	// System Binaries
		"/selinux/*",
		//	"/srv/", 	// Service Data
		"/sys/*",
		"/tmp/*",
		//	"/usr/",	// User Programs
		//	"/var/", 	// Variable Files
		"lost+found/",
	}

	DefaultIgnoreList = append(DefaultIgnoreList, platformList...)
}

func init() {
	addUnixIgnore()
}

func platformSafeFilename(src string) (dst string) {
	dst = src
	return dst
}

func userHomeFolder() string {
	return os.Getenv("HOME")
}

// Special detection for Dropbox Smart Sync placeholder files
func isOfflineFile(fileInfo os.FileInfo) bool {
	sys := fileInfo.Sys().(*syscall.Stat_t)
	return sys != nil && sys.Size > 0 && sys.Blocks == 0
}

func minorPathError(r interface{}) error {
	e, ok := r.(*os.PathError)
	if ok && e.Err == syscall.EBADF { // "bad file descriptor" while reading some files on OSX
		return e
	}
	return nil
}
