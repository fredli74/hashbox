//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package main

import (
	"os"
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
	return os.Getenv("HOME");
}