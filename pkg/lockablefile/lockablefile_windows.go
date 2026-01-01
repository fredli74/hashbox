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
	core.ASSERT(l != nil && l.f != nil, "Lock called on nil file")
	lockFileEx(l.f.Fd(), windows.LOCKFILE_EXCLUSIVE_LOCK)
}

// LockShared acquires a shared lock on the file using LockFileEx.
func (l *LockableFile) LockShared() {
	core.ASSERT(l != nil && l.f != nil, "LockShared called on nil file")
	lockFileEx(l.f.Fd(), 0)
}

// Unlock releases the lock using UnlockFileEx.
func (l *LockableFile) Unlock() {
	core.ASSERT(l != nil && l.f != nil, "Unlock called on nil file")
	unlockFileEx(l.f.Fd())
}

func lockFileEx(fd uintptr, flags uint32) {
	var ol windows.Overlapped
	core.AbortOn(windows.LockFileEx(windows.Handle(fd), flags, 0, 1, 0, &ol))
}

func unlockFileEx(fd uintptr) {
	var ol windows.Overlapped
	core.AbortOn(windows.UnlockFileEx(windows.Handle(fd), 0, 1, 0, &ol))
}
