//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package filelock

import (
	"os"
)

// Locker wraps a platform-specific file lock handle.
type Locker struct {
	f *os.File
}

// Open opens the file (creating it if needed) and returns a Locker without taking the lock.
func Open(path string) (*Locker, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &Locker{f: f}, nil
}

func (l *Locker) File() *os.File {
	return l.f
}

// Close releases any lock (via close) and closes the file.
func (l *Locker) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	return l.f.Close()
}
