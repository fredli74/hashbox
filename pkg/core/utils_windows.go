//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

//go:build windows

package core

import (
	"syscall"
	"unsafe"
)

func FreeSpace(path string) (int64, error) {
	kernel32, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		return 0, err
	}
	defer func() {
		AbortOnError(syscall.FreeLibrary(kernel32))
	}()

	GetDiskFreeSpaceEx, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceExW")
	if err != nil {
		return 0, err
	}
	lpFreeBytesAvailable := int64(0)
	utf16Path, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	if r1, _, e1 := syscall.SyscallN(uintptr(GetDiskFreeSpaceEx),
		uintptr(unsafe.Pointer(utf16Path)),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)), 0, 0); r1 == 0 {
		if e1 != 0 {
			return 0, error(e1)
		}
		return 0, syscall.EINVAL
	}
	return lpFreeBytesAvailable, nil
}
