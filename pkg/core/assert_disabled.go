//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

//go:build release

package core

// ASSERT is a no-op in release builds.
func ASSERT(_ bool, _ ...interface{}) {}
