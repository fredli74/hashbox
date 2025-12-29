//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package filelock

import "os"

// Locker wraps a platform-specific file lock handle.
type Locker struct {
	f *os.File
}

// Open wraps os.Open (read-only) and returns a Locker without taking the lock.
func Open(path string) (*Locker, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Locker{f: f}, nil
}

// OpenFile wraps os.OpenFile with the provided flags/perm and returns a Locker without taking the lock.
func OpenFile(path string, flag int, perm os.FileMode) (*Locker, error) {
	f, err := os.OpenFile(path, flag, perm)
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
