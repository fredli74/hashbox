//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

//go:build !release

package core

import "fmt"

// ASSERT panics if ok is false; intended for developer errors.
func ASSERT(ok bool, v ...interface{}) {
	if !ok {
		panic(fmt.Errorf("ASSERT failed: %v", v))
	}
}
