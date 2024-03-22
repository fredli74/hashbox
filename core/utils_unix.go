//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2024
//	+---+´

//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd || solaris

package core

import "syscall"

func FreeSpace(path string) (int64, error) {
	stat := syscall.Statfs_t{}
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Bsize) * int64(stat.Bfree), nil
}
