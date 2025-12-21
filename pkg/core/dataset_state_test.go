//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2024
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"sort"
	"testing"
)

func TestDatasetStateArraySortsByStateID(t *testing.T) {
	makeID := func(prefix byte) Byte128 {
		var b Byte128
		for i := range b {
			b[i] = prefix
		}
		return b
	}

	arr := DatasetStateArray{
		{State: DatasetState{StateID: makeID('b')}},
		{State: DatasetState{StateID: makeID('a')}},
		{State: DatasetState{StateID: makeID('c')}},
	}
	sort.Sort(arr)

	expected := []byte{'a', 'b', 'c'}
	for i, exp := range expected {
		if arr[i].State.StateID[0] != exp {
			t.Fatalf("unexpected sort order at %d: got %q want %q", i, arr[i].State.StateID[0], exp)
		}
	}
}
