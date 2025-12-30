//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

//go:build unix

package lockablefile

import (
	"os"

	"golang.org/x/sys/unix"
)

// Lock acquires an exclusive advisory lock on the file descriptor.
func (l *LockableFile) Lock() error {
	if l == nil || l.f == nil {
		return os.ErrInvalid
	}
	flock := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: int16(os.SEEK_SET),
		Start:  0,
		Len:    0, // whole file
	}
	return unix.FcntlFlock(l.f.Fd(), unix.F_SETLKW, &flock)
}

// LockShared acquires a shared advisory lock on the file descriptor.
func (l *LockableFile) LockShared() error {
	if l == nil || l.f == nil {
		return os.ErrInvalid
	}
	flock := unix.Flock_t{
		Type:   unix.F_RDLCK,
		Whence: int16(os.SEEK_SET),
		Start:  0,
		Len:    0, // whole file
	}
	return unix.FcntlFlock(l.f.Fd(), unix.F_SETLKW, &flock)
}

// Unlock releases the advisory lock.
func (l *LockableFile) Unlock() error {
	if l == nil || l.f == nil {
		return os.ErrInvalid
	}
	flock := unix.Flock_t{
		Type:   unix.F_UNLCK,
		Whence: int16(os.SEEK_SET),
		Start:  0,
		Len:    0,
	}
	return unix.FcntlFlock(l.f.Fd(), unix.F_SETLK, &flock)
}
