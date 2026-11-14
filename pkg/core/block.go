//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Package core provides Hashbox core primitives.
package core

import (
	"github.com/fredli74/bytearray"

	"bytes"
	"compress/zlib"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	BlockDataTypeRaw  = 0xff
	BlockDataTypeZlib = 0x01
)

// HashboxBlock is serialized with counters before all arrays
type HashboxBlock struct {
	BlockID  Byte128   // = md5( LinkLength Links DataLength Data )
	Links    []Byte128 // Array of BlockIDs
	DataType uint8     // 1 byte data type

	Data bytearray.ByteArray

	Compressed       bool
	CompressedSize   int
	UncompressedSize int
}

func NewHashboxBlock(dataType byte, data bytearray.ByteArray, links []Byte128) *HashboxBlock {
	block := HashboxBlock{Links: links, DataType: dataType, Data: data, Compressed: false, CompressedSize: -1, UncompressedSize: data.Len()}
	block.BlockID = block.HashData()
	return &block
}

func (b *HashboxBlock) Release() {
	b.Data.Release()
}

// Frees a pooled buffer that a panic is about to orphan. Chunks are hand
// managed, so an unwound allocation is never reclaimed.
type releaser interface{ Release() }

func releaseOnPanic(r releaser) {
	if p := recover(); p != nil {
		r.Release()
		panic(p)
	}
}

func (b *HashboxBlock) SerializeLinks(w io.Writer) (size int) {
	size += WriteUint32(w, uint32(len(b.Links)))
	for i := range b.Links {
		size += b.Links[i].Serialize(w)
	}
	return
}
func (b *HashboxBlock) Serialize(w io.Writer) (size int) {
	size += b.BlockID.Serialize(w)
	size += b.SerializeLinks(w)
	size += WriteUint8(w, b.DataType)

	b.CompressData()
	size += WriteUint32(w, uint32(b.Data.Len()))
	l := CopyOrPanic(w, &b.Data)
	if l != b.Data.Len() {
		panic(fmt.Errorf("writing block %x with data length %d bytes, but wrote %d bytes", b.BlockID[:], b.Data.Len(), l))
	}
	size += l

	return
}
func (b *HashboxBlock) UnserializeHeader(r io.Reader) (size int) {
	size += b.BlockID.Unserialize(r)
	var nLinks uint32
	size += ReadUint32(r, &nLinks)
	// grown as links arrive, the count is unchecked wire input
	b.Links = []Byte128{}
	for range nLinks {
		var l Byte128
		size += l.Unserialize(r)
		b.Links = append(b.Links, l)
	}
	size += ReadUint8(r, &b.DataType)
	return
}
func (b *HashboxBlock) Unserialize(r io.Reader) (size int) {
	size += b.UnserializeHeader(r)

	var nBytes uint32
	size += ReadUint32(r, &nBytes) // Data length
	b.Data.Truncate(0)
	size += CopyNOrPanic(&b.Data, r, int(nBytes))

	b.Compressed = true
	b.CompressedSize = int(nBytes)
	b.UncompressedSize = -1
	return
}

func (b *HashboxBlock) HashData() (BlockID Byte128) {
	ASSERT(!b.Compressed, "HashData called on compressed data")

	hash := md5.New()
	b.SerializeLinks(hash)
	//	WriteUint8(hash, b.DataType)   not part of the hash

	WriteUint32(hash, uint32(b.Data.Len()))
	_, err := b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
	CopyOrPanic(hash, &b.Data)

	copy(BlockID[:], hash.Sum(nil)[:16])

	_, err = b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
	return BlockID
}

func (b *HashboxBlock) UncompressData() {
	if b.Compressed {
		switch b.DataType {
		case BlockDataTypeRaw:
			// Do nothing, data is already uncompressed
			b.Compressed = false
		case BlockDataTypeZlib:
			c := b.zlibUncompress()
			b.Data.Release()
			b.Data = c
			b.Compressed = false
		default:
			panic(errors.New("unsupported block data type"))
		}
		b.UncompressedSize = b.Data.Len()
	}
	_, err := b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
}

func (b *HashboxBlock) CompressData() {
	if !b.Compressed {
		switch b.DataType {
		case BlockDataTypeRaw:
			// DO nothing data does not compress
		case BlockDataTypeZlib:
			c := b.zlibCompress()
			b.Data.Release()
			b.Data = c
			b.Compressed = true
		default:
			panic(errors.New("unsupported block data type"))
		}
		b.CompressedSize = b.Data.Len()
	}
	_, err := b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
}

func (b *HashboxBlock) VerifyBlock() bool {
	var verifyID Byte128
	if b.Compressed {
		switch b.DataType {
		case BlockDataTypeRaw:
			b.Compressed = false
			verifyID = b.HashData()
		case BlockDataTypeZlib:
			verifyID = b.hashUncompressed()
		default:
			panic(errors.New("unsupported block data type"))
		}
	} else {
		verifyID = b.HashData()
	}
	return bytes.Equal(verifyID[:], b.BlockID[:])
}

// Hashes the inflated form without keeping it, for a block still compressed.
func (b *HashboxBlock) hashUncompressed() (id Byte128) {
	c := b.zlibUncompress()
	defer c.Release()

	hash := md5.New()
	b.SerializeLinks(hash)
	WriteUint32(hash, uint32(c.Len()))
	CopyOrPanic(hash, &c)
	copy(id[:], hash.Sum(nil)[:16])
	return id
}

func (b *HashboxBlock) zlibCompress() (dst bytearray.ByteArray) {
	defer releaseOnPanic(&dst)

	_, err := b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
	zw := zpool.GetWriter(&dst)
	CopyOrPanic(zw, &b.Data)
	AbortOnError(zw.Close())
	zpool.PutWriter(zw)
	return dst
}
func (b *HashboxBlock) zlibUncompress() bytearray.ByteArray {
	var dst bytearray.ByteArray
	// registered first so it unwinds last and covers the whole function, a
	// truncated or corrupt stream must not strand the chunks already written
	defer releaseOnPanic(&dst)

	_, err := b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
	zr, err := zlib.NewReader(&b.Data)
	AbortOnError(err, "Error during ZlibUncompress of block %x: %v", b.BlockID[:], err)
	defer func() {
		err := zr.Close()
		AbortOnError(err, "Error during ZlibUncompress of block %x: %v", b.BlockID[:], err)
	}()
	_, err = io.Copy(&dst, zr)
	AbortOnError(err, "Error during ZlibUncompress of block %x: %v", b.BlockID[:], err)
	return dst
}

var zpool zlibPool

type zlibPool struct {
	mutex     sync.Mutex
	writepool []*zlib.Writer
}

func (z *zlibPool) GetWriter(w io.Writer) (zw *zlib.Writer) {
	z.mutex.Lock()
	defer z.mutex.Unlock()
	if len(z.writepool) > 0 {
		zw = z.writepool[len(z.writepool)-1]
		z.writepool = z.writepool[:len(z.writepool)-1]
		zw.Reset(w)
	} else {
		var err error
		zw, err = zlib.NewWriterLevel(w, zlib.DefaultCompression)
		AbortOnError(err)
	}
	return zw
}
func (z *zlibPool) PutWriter(zw *zlib.Writer) {
	z.mutex.Lock()
	defer z.mutex.Unlock()
	z.writepool = append(z.writepool, zw)
}
