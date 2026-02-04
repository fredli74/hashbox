//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

//go:build unix

package lockablefile

import (
	"io"

	"github.com/fredli74/hashbox/pkg/core"
	"golang.org/x/sys/unix"
)

// Lock acquires an exclusive advisory lock on the file descriptor.
func (l *LockableFile) Lock() {
	l.lock(unix.F_WRLCK)
}

// TryLock attempts an exclusive lock without blocking.
func (l *LockableFile) TryLock() bool {
	return l.tryLock(unix.F_WRLCK)
}

// LockShared acquires a shared advisory lock on the file descriptor.
func (l *LockableFile) LockShared() {
	l.lock(unix.F_RDLCK)
}

// TryLockShared attempts a shared lock without blocking.
func (l *LockableFile) TryLockShared() bool {
	return l.tryLock(unix.F_RDLCK)
}

// Unlock releases the advisory lock.
func (l *LockableFile) Unlock() {
	core.ASSERT(l != nil && l.File != nil, "Unlock called on nil file")
	core.ASSERT(l.locked, "Unlock called on unlocked file")
	flock := unix.Flock_t{
		Type:   unix.F_UNLCK,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0,
	}
	core.AbortOnError(unix.FcntlFlock(l.Fd(), unix.F_SETLK, &flock))
	l.locked = false
}

func (l *LockableFile) lock(lockType int16) {
	core.ASSERT(l != nil && l.File != nil, "Lock called on nil file")
	core.ASSERT(!l.locked, "Lock called on already locked file")
	flock := unix.Flock_t{
		Type:   lockType,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0,
	}
	core.AbortOnError(unix.FcntlFlock(l.Fd(), unix.F_SETLKW, &flock))
	l.locked = true
}

func (l *LockableFile) tryLock(lockType int16) bool {
	core.ASSERT(l != nil && l.File != nil, "TryLock called on nil file")
	core.ASSERT(!l.locked, "TryLock called on already locked file")
	flock := unix.Flock_t{
		Type:   lockType,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0,
	}
	err := unix.FcntlFlock(l.Fd(), unix.F_SETLK, &flock)
	if err == nil {
		l.locked = true
		return true
	}
	if err == unix.EACCES || err == unix.EAGAIN {
		return false
	}
	core.AbortOnError(err)
	return false
}
