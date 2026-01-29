//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Hashbox core, version 0.1
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
		panic(fmt.Errorf("Writing block %x with data length %d bytes, but wrote %d bytes", b.BlockID[:], b.Data.Len(), l))
	}
	size += l

	return
}
func (b *HashboxBlock) UnserializeHeader(r io.Reader) (size int) {
	size += b.BlockID.Unserialize(r)
	var nLinks uint32
	size += ReadUint32(r, &nLinks)
	b.Links = make([]Byte128, nLinks)
	for i := 0; i < int(nLinks); i++ {
		size += b.Links[i].Unserialize(r)
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
			panic(errors.New("Unsupported Block Data Type"))
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
			panic(errors.New("Unsupported Block Data Type"))
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
			c := b.zlibUncompress()
			hash := md5.New()
			b.SerializeLinks(hash)
			WriteUint32(hash, uint32(c.Len()))
			CopyOrPanic(hash, &c)
			copy(verifyID[:], hash.Sum(nil)[:16])
			c.Release()
		default:
			panic(errors.New("Unsupported Block Data Type"))
		}
	} else {
		verifyID = b.HashData()
	}
	return bytes.Equal(verifyID[:], b.BlockID[:])
}

func (b *HashboxBlock) zlibCompress() (dst bytearray.ByteArray) {
	_, err := b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
	zw := zpool.GetWriter(&dst)
	CopyOrPanic(zw, &b.Data)
	err = zw.Close()
	AbortOnError(err)
	zpool.PutWriter(zw)
	return dst
}
func (b *HashboxBlock) zlibUncompress() bytearray.ByteArray {
	_, err := b.Data.ReadSeek(0, bytearray.SEEK_SET)
	AbortOnError(err)
	zr, err := zlib.NewReader(&b.Data)
	AbortOnError(err, "Error during ZlibUncompress of block %x: %v", b.BlockID[:], err)
	defer func() {
		err := zr.Close()
		AbortOnError(err, "Error during ZlibUncompress of block %x: %v", b.BlockID[:], err)
	}()
	var dst bytearray.ByteArray
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
