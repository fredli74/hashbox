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
	core.ASSERT(l != nil && l.File != nil, "Lock called on nil file")
	core.ASSERT(!l.locked, "Lock called on already locked file")
	flock := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0, // whole file
	}
	core.AbortOnError(unix.FcntlFlock(l.Fd(), unix.F_SETLKW, &flock))
	l.locked = true
}

// LockShared acquires a shared advisory lock on the file descriptor.
func (l *LockableFile) LockShared() {
	core.ASSERT(l != nil && l.File != nil, "LockShared called on nil file")
	core.ASSERT(!l.locked, "LockShared called on already locked file")
	flock := unix.Flock_t{
		Type:   unix.F_RDLCK,
		Whence: int16(io.SeekStart),
		Start:  0,
		Len:    0, // whole file
	}
	core.AbortOnError(unix.FcntlFlock(l.Fd(), unix.F_SETLKW, &flock))
	l.locked = true
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
