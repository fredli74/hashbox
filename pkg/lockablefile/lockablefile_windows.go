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
	l.lock(windows.LOCKFILE_EXCLUSIVE_LOCK)
}

// TryLock attempts an exclusive lock without blocking.
func (l *LockableFile) TryLock() bool {
	return l.tryLock(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
}

// LockShared acquires a shared lock on the file using LockFileEx.
func (l *LockableFile) LockShared() {
	l.lock(0)
}

// TryLockShared attempts a shared lock without blocking.
func (l *LockableFile) TryLockShared() bool {
	return l.tryLock(windows.LOCKFILE_FAIL_IMMEDIATELY)
}

// Unlock releases the lock using UnlockFileEx.
func (l *LockableFile) Unlock() {
	core.ASSERT(l != nil && l.File != nil, "Unlock called on nil file")
	core.ASSERT(l.locked, "Unlock called on unlocked file")
	unlockFileEx(l.Fd())
	l.locked = false
}

func lockFileEx(fd uintptr, flags uint32) {
	var ol windows.Overlapped
	core.AbortOnError(windows.LockFileEx(windows.Handle(fd), flags, 0, 1, 0, &ol))
}

func lockFileExTry(fd uintptr, flags uint32) error {
	var ol windows.Overlapped
	return windows.LockFileEx(windows.Handle(fd), flags, 0, 1, 0, &ol)
}

func unlockFileEx(fd uintptr) {
	var ol windows.Overlapped
	core.AbortOnError(windows.UnlockFileEx(windows.Handle(fd), 0, 1, 0, &ol))
}

func (l *LockableFile) lock(flags uint32) {
	core.ASSERT(l != nil && l.File != nil, "Lock called on nil file")
	core.ASSERT(!l.locked, "Lock called on already locked file")
	lockFileEx(l.Fd(), flags)
	l.locked = true
}

func (l *LockableFile) tryLock(flags uint32) bool {
	core.ASSERT(l != nil && l.File != nil, "TryLock called on nil file")
	core.ASSERT(!l.locked, "TryLock called on already locked file")
	err := lockFileExTry(l.Fd(), flags)
	if err == nil {
		l.locked = true
		return true
	}
	if err == windows.ERROR_LOCK_VIOLATION {
		return false
	}
	core.AbortOnError(err)
	return false
}
