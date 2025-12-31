//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package lockablefile

import (
	"os"

	"github.com/fredli74/hashbox/pkg/core"
)

// LockableFile wraps an *os.File with lock helpers.
type LockableFile struct {
	f *os.File
}

// Open wraps os.Open (read-only) and returns a LockableFile without taking the lock.
func Open(path string) (*LockableFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &LockableFile{f: f}, nil
}

// OpenFile wraps os.OpenFile with the provided flags/perm and returns a LockableFile without taking the lock.
func OpenFile(path string, flag int, perm os.FileMode) (*LockableFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &LockableFile{f: f}, nil
}

func (l *LockableFile) File() *os.File {
	return l.f
}

// Close releases any lock (via close) and closes the file.
func (l *LockableFile) Close() {
	core.ASSERT(l != nil && l.f != nil, "Close called on nil file")
	core.AbortOn(l.f.Close())
}
