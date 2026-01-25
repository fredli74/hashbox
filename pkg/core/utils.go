//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"github.com/fredli74/bytearray"

	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Abort panics with a formatted error message.
func Abort(format string, a ...interface{}) {
	panic(fmt.Errorf(format, a...))
}

// AbortOn panics if err is non-nil, with optional formatted message.
func AbortOn(err error, a ...interface{}) {
	if err != nil {
		if len(a) > 0 {
			if format, ok := a[0].(string); ok {
				panic(fmt.Errorf(format, a[1:]...))
			}
		}
		panic(err)
	}
}

func ReadBytes(r io.Reader, data []byte) int {
	n, err := io.ReadFull(r, data)
	AbortOn(err)
	return n
}
func ReadUint8(r io.Reader, data *uint8) int {
	var b [1]byte
	n := ReadBytes(r, b[:])
	*data = b[0]
	return n
}
func ReadUint16(r io.Reader, data *uint16) int {
	var b [2]byte
	n := ReadBytes(r, b[:])
	*data = uint16(b[1]) | uint16(b[0])<<8
	return n
}
func ReadUint32(r io.Reader, data *uint32) int {
	var b [4]byte
	n := ReadBytes(r, b[:])
	*data = uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
	return n
}
func ReadInt64(r io.Reader, data *int64) int {
	var b [8]byte
	n := ReadBytes(r, b[:])
	*data = int64(uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 | uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56)
	return n
}
func WriteBytes(w io.Writer, data []byte) int {
	n, err := w.Write(data)
	AbortOn(err)
	return n
}
func WriteUint8(w io.Writer, data uint8) int {
	var b [1]byte = [1]byte{data}
	return WriteBytes(w, b[:])
}
func WriteUint16(w io.Writer, data uint16) int {
	var b [2]byte = [2]byte{byte(data >> 8), byte(data)}
	return WriteBytes(w, b[:])
}
func WriteUint32(w io.Writer, data uint32) int {
	var b [4]byte = [4]byte{byte(data >> 24), byte(data >> 16), byte(data >> 8), byte(data)}
	return WriteBytes(w, b[:])
}
func WriteInt64(w io.Writer, data int64) int {
	var b [8]byte = [8]byte{byte(data >> 56), byte(data >> 48), byte(data >> 40), byte(data >> 32), byte(data >> 24), byte(data >> 16), byte(data >> 8), byte(data)}
	return WriteBytes(w, b[:])
}

func CopyOrPanic(dst io.Writer, src io.Reader) int {
	written, err := io.Copy(dst, src)
	AbortOn(err)
	return int(written)
}
func CopyNOrPanic(dst io.Writer, src io.Reader, n int) int {
	written, err := io.CopyN(dst, src, int64(n))
	AbortOn(err)
	return int(written)
}

const LOGTIMEFORMAT string = "20060102 15:04:05"
const (
	LogError = iota
	LogWarning
	LogInfo
	LogDebug
	LogTrace
)

var LogLevel int = LogInfo
var logMarks = []string{"!", "*", ".", "(", "?"}

var LogMutex sync.Mutex

func Escape(v interface{}) string {
	var s string
	switch t := v.(type) {
	case string:
		s = t
	case String:
		s = string(t)
	default:
		panic(fmt.Errorf("Escape expects string or core.String"))
	}
	var b strings.Builder
	for _, r := range s {
		if r < 32 || r == 127 {
			fmt.Fprintf(&b, "\\x%02x", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func Log(level int, format string, a ...interface{}) {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if level > LogLevel {
		return
	}

	args := make([]interface{}, len(a))
	for i, v := range a {
		switch s := v.(type) {
		case string:
			args[i] = Escape(s)
		case String:
			args[i] = Escape(s)
		default:
			args[i] = v
		}
	}

	prefix := []interface{}{time.Now().UTC().Format(LOGTIMEFORMAT), logMarks[level]}
	fmt.Printf("%s %s "+format+"\n", append(prefix, args...)...)
}

var shortHumanUnitName []string = []string{"B", "K", "M", "G", "T", "P", "E"}
var humanUnitName []string = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

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

func ShortHumanSize(size int64) string {
	s, u, p := unitize(size, len(shortHumanUnitName))
	return fmt.Sprintf("%.*f%s", p, s, shortHumanUnitName[u])
}
func HumanSize(size int64) string {
	s, u, p := unitize(size, len(humanUnitName))
	return fmt.Sprintf("%.*f %s", p, s, humanUnitName[u])
}

// CompactHumanSize is like HumanSize but removes whitespace (e.g. "1.2KiB").
func CompactHumanSize(size int64) string {
	return strings.ReplaceAll(HumanSize(size), " ", "")
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
		}
		v, _ := syscall.Getenv(key)
		return v
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
