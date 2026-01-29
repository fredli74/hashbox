//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

//go:build windows

package lockablefile

import (
	"github.com/fredli74/hashbox/pkg/core"
	"golang.org/x/sys/windows"
)

// Lock acquires an exclusive lock on the file using LockFileEx.
func (l *LockableFile) Lock() {
	core.ASSERT(l != nil && l.File != nil, "Lock called on nil file")
	lockFileEx(l.Fd(), windows.LOCKFILE_EXCLUSIVE_LOCK)
}

// LockShared acquires a shared lock on the file using LockFileEx.
func (l *LockableFile) LockShared() {
	core.ASSERT(l != nil && l.File != nil, "LockShared called on nil file")
	lockFileEx(l.Fd(), 0)
}

// Unlock releases the lock using UnlockFileEx.
func (l *LockableFile) Unlock() {
	core.ASSERT(l != nil && l.File != nil, "Unlock called on nil file")
	unlockFileEx(l.Fd())
}

func lockFileEx(fd uintptr, flags uint32) {
	var ol windows.Overlapped
	err := windows.LockFileEx(windows.Handle(fd), flags, 0, 1, 0, &ol)
	core.AbortOn(err)
}

func unlockFileEx(fd uintptr) {
	var ol windows.Overlapped
	err := windows.UnlockFileEx(windows.Handle(fd), 0, 1, 0, &ol)
	core.AbortOn(err)
}
