//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core, version 0.1
package core

import (
	//	"fmt"
	"math/rand"
	"testing"
)

func TestSplit(t *testing.T) {
	var A ByteArray
	buf := make([]byte, 6000)
	for x := range buf {
		buf[x] = byte(x)
	}
	{
		i, err := A.Write(buf)
		if i != len(buf) {
			t.Fatalf("expected Write to return: %d, got: %d", len(buf), i)
		}
		if err != nil {
			t.Fatalf("expected Write to return no error, got: %s", err.Error())
		}
	}
	{
		i := A.WriteSeek(6008, SEEK_SET)
		if i != 6008 {
			t.Fatalf("expected WriteSeek to return: %d, got: %d", 6008, i)
		}
	}
	{
		i, err := A.ReadSeek(2000, SEEK_CUR)
		if i != 2000 {
			t.Fatalf("expected ReadSeek to return: %d, got: %d", 2000, i)
		}
		if err != nil {
			t.Fatalf("expected ReadSeek to return no error, got: %s", err.Error())
		}
	}
	{
		i, err := A.ReadSeek(3000, SEEK_CUR)
		if i != 5000 {
			t.Fatalf("expected ReadSeek to return: %d, got: %d", 5000, i)
		}
		if err != nil {
			t.Fatalf("expected ReadSeek to return no error, got: %s", err.Error())
		}
	}
	{
		i, err := A.ReadSeek(2000, SEEK_CUR)
		if i != 6008 {
			t.Fatalf("expected ReadSeek to return: %d, got: %d", 6008, i)
		}
		if err.Error() != "EOF" {
			t.Fatalf("expected ReadSeek to return EOF error")
		}
	}

	{
		B := A.Split(5000)
		if B.Len() != 1008 {
			t.Fatalf("expected Split to return a new %d ByteArray, got: %d", 1008, B.Len())
		}
		if A.Len() != 5000 {
			t.Fatalf("expected Split to leave %d bytes, have: %d", 5000, A.Len())
		}
		B.Release()
	}

	{
		B := A.Split(1024)
		if B.Len() != 3976 {
			t.Fatalf("expected Split to return a new %d ByteArray, got: %d", 3976, B.Len())
		}
		if A.Len() != 1024 {
			t.Fatalf("expected Split to leave %d bytes, have: %d", 1024, A.Len())
		}
		B.Release()
	}

	{
		B := A.Split(1024)
		if B.Len() != 0 {
			t.Fatalf("expected Split to return a new %d ByteArray, got: %d", 0, B.Len())
		}
		if A.Len() != 1024 {
			t.Fatalf("expected Split to leave %d bytes, have: %d", 1024, A.Len())
		}
		B.Release()
	}

	{
		B := A.Split(5)
		if B.Len() != 1019 {
			t.Fatalf("expected Split to return a new %d ByteArray, got: %d", 1019, B.Len())
		}
		if A.Len() != 5 {
			t.Fatalf("expected Split to leave %d bytes, have: %d", 5, A.Len())
		}
		B.Release()
	}

	{
		B := A.Split(0)
		if B.Len() != 5 {
			t.Fatalf("expected Split to return a new %d ByteArray, got: %d", 5, B.Len())
		}
		if A.Len() != 0 {
			t.Fatalf("expected Split to leave %d bytes, have: %d", 0, A.Len())
		}
		B.Release()
	}
	A.Release()
	a, g, r, m := Stats()
	if a != 1 {
		t.Fatalf("expected allocation of 1 slab, got: %d", a)
	}
	if g != 11 {
		t.Fatalf("expected 11 grabbed chunks, got: %d", g)
	}
	if r != 11 {
		t.Fatalf("expected 11 released chunks, got: %d", r)
	}
	if m != 0 {
		t.Fatalf("expected 0 bytes of memory to be left allocated, got: %d", m)
	}
}

func TestAllocate(t *testing.T) {
	var testArray [200]ByteArray
	var testSize [200]int

	for i := 0; i < 200; i++ {
		testSize[i] = rand.Intn(10000) //000)
		buf := make([]byte, testSize[i])
		for x := range buf {
			buf[x] = byte(x)
		}
		testArray[i].Write(buf)

		if testArray[i].Len() != testSize[i] {
			t.Fatalf("expected len(): %d, got: %d", testSize[i], testArray[i].Len())
		}

		count := 0
		for {
			buf, err := testArray[i].ReadSlice()
			if buf != nil {
				for _, b := range buf {
					if b != byte(count) {
						t.Fatalf("expected data %d, got: %d", i, b)
					}
					count++
				}
			} else {
				if err == nil {
					t.Fatalf("exptected EOF, but received no error")
				}
				break
			}
			o, err := testArray[i].ReadSeek(len(buf), SEEK_CUR)
			if o != count {
				t.Fatalf("expected read offset to be %d, got: %d", count, o)
			}
		}
		if count != testArray[i].Len() {
			t.Fatalf("expected to read: %d bytes, got: %d", testSize[i], count)
		}
		resize := rand.Intn(100000)
		testArray[i].Truncate(resize)
		if testArray[i].Len() != resize {
			t.Fatalf("expected len(): %d, got: %d", resize, testArray[i].Len())
		}
		testArray[i].Release()
		if testArray[i].rootChunk != emptyLocation {
			t.Fatalf("release did not clear")
		}
	}
	a, g, r, m := Stats()
	if a == 0 {
		t.Fatalf("expected allocation of >0 slabs, got: %d", a)
	}
	if g != r {
		t.Fatalf("%d grabbed chunks, %d released chunks, leaking!", g, r)
	}
	if m != 0 {
		t.Fatalf("expected 0 bytes of memory to be left allocated, got: %d", m)
	}

}

func BenchmarkAllocate(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(1024 * 1024)

	buf := make([]byte, 1024*1024)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var a ByteArray
			a.Write(buf)
			a.Release()
		}
	})

	// fmt.Println(Stats())
}
