//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"bitbucket.org/fredli74/bytearray"

	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func ReadOrPanic(r io.Reader, data interface{}) int {
	err := binary.Read(r, binary.BigEndian, data)
	if err != nil {
		panic(err)
	}
	return binary.Size(data)
}
func WriteOrPanic(w io.Writer, data interface{}) int {
	err := binary.Write(w, binary.BigEndian, data)
	if err != nil {
		panic(err)
	}
	return binary.Size(data)
}
func CopyOrPanic(dst io.Writer, src io.Reader) int {
	written, err := io.Copy(dst, src)
	if err != nil {
		panic(err)
	}
	return int(written)
}
func CopyNOrPanic(dst io.Writer, src io.Reader, n int) int {
	written, err := io.CopyN(dst, src, int64(n))
	if err != nil {
		panic(err)
	}
	return int(written)
}

var humanUnitName []string = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
var shortHumanUnitName []string = []string{"B", "K", "M", "G", "T", "P", "E"}

func unitize(size int64, limit int) (floatSize float64, unit int, precision int) {
	floatSize = float64(size)
	for ; unit < limit && floatSize > 1000; floatSize /= 1024 {
		unit++
	}
	if unit > 0 && floatSize < 10 {
		precision = 2
	} else if unit > 0 && floatSize < 100 {
		precision = 1
	}
	return floatSize, unit, precision
}

func HumanSize(size int64) string {
	s, u, p := unitize(size, len(humanUnitName))
	return fmt.Sprintf("%.*f %s", p, s, humanUnitName[u])
}
func ShortHumanSize(size int64) string {
	s, u, p := unitize(size, len(shortHumanUnitName))
	return fmt.Sprintf("%.*f%s", p, s, shortHumanUnitName[u])
}

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

func BytesInt64(bytes []byte) (v int64) {
	for _, b := range bytes {
		v <<= 8
		v |= int64(b)
	}
	return v
}

func LimitInt(big int64) (v int) {
	v = MaxInt
	if big < int64(v) {
		v = int(big)
	}
	return v
}

func ExpandEnv(s string) string {
	return os.Expand(s, func(key string) string {
		if key == "$" {
			return key
		} else {
			v, _ := syscall.Getenv(key)
			return v
		}
	})
}
func SplitPath(path string) []string {
	list := strings.Split(filepath.Clean(path), string(filepath.Separator))
	filtered := list[:0]
	for _, l := range list {
		if l != "" {
			filtered = append(filtered, l)
		}
	}
	return filtered
}

func MemoryStats() string {
	a, _, _, m, u := bytearray.Stats()
	return fmt.Sprintf("Memory stats: %d slabs, %s allocated, %s used", a, ShortHumanSize(m), ShortHumanSize(u))
}
