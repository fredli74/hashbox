//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core client routines
package core

import (
	"errors"
	"fmt"
	"os"
)

type LockFile struct {
	name string
	file *os.File
}

func NewLockFile(name string) (*LockFile, error) {
	var err error

	lock := LockFile{name: name}
	if lock.file, err = os.OpenFile(lock.name, os.O_CREATE|os.O_RDWR, os.ModeTemporary); err == nil {
		var pid int
		if _, err = fmt.Fscanf(lock.file, "%d\n", &pid); err == nil {
			if pid != os.Getpid() {
				if _, err := os.FindProcess(pid); err == nil {
					return nil, errors.New("Locked by other process")
				}
			}
		}
		lock.file.Seek(0, 0)
		if n, err := fmt.Fprintf(lock.file, "%d\n", os.Getpid()); err == nil {
			lock.file.Truncate(int64(n))
			return &lock, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (l *LockFile) Close() {
	l.file.Close()
	os.Remove(l.name)
}
