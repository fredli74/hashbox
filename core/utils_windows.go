//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2016
//	+---+´

// +build windows

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
	defer syscall.FreeLibrary(kernel32)

	GetDiskFreeSpaceEx, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceExW")
	if err != nil {
		return 0, err
	}
	lpFreeBytesAvailable := int64(0)
	if r1, _, e1 := syscall.Syscall6(uintptr(GetDiskFreeSpaceEx), 4,
			uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
			uintptr(unsafe.Pointer(&lpFreeBytesAvailable)), 0, 0, 0, 0); r1 == 0 {
		if e1 != 0 {
			return 0, error(e1)
		} else {
			return 0, syscall.EINVAL
		}
	}
	return lpFreeBytesAvailable, nil
}