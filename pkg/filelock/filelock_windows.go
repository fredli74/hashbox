//go:build windows

package filelock

import (
	"os"

	"golang.org/x/sys/windows"
)

// Lock acquires an exclusive lock on the file using LockFileEx.
func (l *Locker) Lock() error {
	if l == nil || l.f == nil {
		return os.ErrInvalid
	}
	return lockFileEx(l.f.Fd(), windows.LOCKFILE_EXCLUSIVE_LOCK)
}

// Unlock releases the lock using UnlockFileEx.
func (l *Locker) Unlock() error {
	if l == nil || l.f == nil {
		return os.ErrInvalid
	}
	return unlockFileEx(l.f.Fd())
}

func lockFileEx(fd uintptr, flags uint32) error {
	var ol windows.Overlapped
	return windows.LockFileEx(windows.Handle(fd), flags, 0, 1, 0, &ol)
}

func unlockFileEx(fd uintptr) error {
	var ol windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(fd), 0, 1, 0, &ol)
}
