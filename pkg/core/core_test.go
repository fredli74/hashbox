//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Package core provides Hashbox core primitives.
package core

import (
	"crypto/hmac"
	"crypto/md5"
	"fmt"
	"testing"
)

type hmacTest struct {
	out   string
	depth int
	key   []byte
	text  []byte
}

var hmacTests = []hmacTest{
	{"74e6f7298a9c2d168935f58c001bad88", 1, []byte(""), []byte("")},
	{"750c783e6ab0b503eaa86e310a5db738", 1, []byte("Jefe"), []byte("what do ya want for nothing?")},
	{"80070713463e7749b90c2dc24911e275", 1, []byte("key"), []byte("The quick brown fox jumps over the lazy dog")},
	{"64139538fd1a40dae8c3f99324f1f9a9", 20, []byte(""), []byte("")},
	{"36b3f7b29692fbbf076d4cce1f9bbd2f", 20, []byte("Jefe"), []byte("what do ya want for nothing?")},
	{"56881e862a44cbf94e92c5bf0bdbc497", 20, []byte("key"), []byte("The quick brown fox jumps over the lazy dog")},
}

func TestDeepHmac(t *testing.T) {
	for _, a := range hmacTests {
		var key Byte128
		copy(key[:], a.key)
		out := fmt.Sprintf("%x", DeepHmac(a.depth, a.text, key))
		if out != a.out {
			t.Errorf("%s != %s", out, a.out)
		} else {
			t.Logf("%s == %s", out, a.out)
		}
	}
}
func TestGoHmac(t *testing.T) {
	for _, a := range hmacTests {
		if a.depth == 1 {
			H := hmac.New(md5.New, a.key)
			H.Write(a.text)
			out := fmt.Sprintf("%x", H.Sum(nil))
			if out != a.out {
				t.Errorf("%s != %s", out, a.out)
			} else {
				t.Logf("%s == %s", out, a.out)
			}
		}
	}
}

func BenchmarkGoHmac(b *testing.B) {
	var size int64
	for _, a := range hmacTests {
		size += int64(len(a.text))
	}
	b.SetBytes(size)
	b.ResetTimer()

	for b.Loop() {
		for _, a := range hmacTests {

			H := hmac.New(md5.New, a.key[:])
			H.Write(a.text)
			var h Byte128
			copy(h[:], H.Sum(nil))
		}
	}
}
func BenchmarkDeepHmac(b *testing.B) {
	var key Byte128
	var size int64
	for _, a := range hmacTests {
		size += int64(len(a.text))
	}
	b.SetBytes(size)
	b.ResetTimer()

	for b.Loop() {
		for _, a := range hmacTests {
			copy(key[:], a.key)
			DeepHmac(1, a.text, key)
		}
	}
}
func Benchmark20kGoHmac(b *testing.B) {
	var size int64
	for _, a := range hmacTests {
		size += int64(len(a.text))
	}
	b.SetBytes(size)
	b.ResetTimer()

	for b.Loop() {
		for _, a := range hmacTests {

			var data []byte = a.text
			for N := 0; N < 20000; N++ {
				H := hmac.New(md5.New, a.key[:])
				H.Write(data)
				data = H.Sum(nil)
			}

			var h Byte128
			copy(h[:], data)
		}
	}
}
func Benchmark20kDeepHmac(b *testing.B) {
	var key Byte128
	var size int64
	for _, a := range hmacTests {
		size += int64(len(a.text))
	}
	b.SetBytes(size)
	b.ResetTimer()

	for b.Loop() {
		for _, a := range hmacTests {
			copy(key[:], a.key)
			DeepHmac(20000, a.text, key)
		}
	}
}
