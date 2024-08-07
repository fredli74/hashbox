//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2024
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
		panic(fmt.Errorf("ASSERT! Writing block %x with data length %d bytes, but wrote %d bytes", b.BlockID[:], b.Data.Len(), l))
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
	if b.Compressed {
		panic("ASSERT! Called HashData on compressed data")
	}

	hash := md5.New()
	b.SerializeLinks(hash)
	//	WriteUint8(hash, b.DataType)   not part of the hash

	WriteUint32(hash, uint32(b.Data.Len()))
	b.Data.ReadSeek(0, bytearray.SEEK_SET)
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
			c := ZlibUncompress(b.Data)
			b.Data.Release()
			b.Data = c
			b.Compressed = false
		default:
			panic(errors.New("Unsupported Block Data Type"))
		}
		b.UncompressedSize = b.Data.Len()
	}
	b.Data.ReadSeek(0, bytearray.SEEK_SET)
}

func (b *HashboxBlock) CompressData() {
	if !b.Compressed {
		switch b.DataType {
		case BlockDataTypeRaw:
			// DO nothing data does not compress
		case BlockDataTypeZlib:
			c := ZlibCompress(b.Data)
			b.Data.Release()
			b.Data = c
			b.Compressed = true
		default:
			panic(errors.New("Unsupported Block Data Type"))
		}
		b.CompressedSize = b.Data.Len()
	}
	b.Data.ReadSeek(0, bytearray.SEEK_SET)
}

func (b *HashboxBlock) VerifyBlock() bool {
	var verifyID Byte128
	if b.Compressed {
		switch b.DataType {
		case BlockDataTypeRaw:
			b.Compressed = false
			verifyID = b.HashData()
		case BlockDataTypeZlib:
			c := ZlibUncompress(b.Data)
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

func ZlibCompress(src bytearray.ByteArray) (dst bytearray.ByteArray) {
	src.ReadSeek(0, bytearray.SEEK_SET)
	zw := zpool.GetWriter(&dst)
	CopyOrPanic(zw, &src)
	if err := zw.Close(); err != nil {
		panic(err)
	}
	zpool.PutWriter(zw)
	return dst
}
func ZlibUncompress(src bytearray.ByteArray) (dst bytearray.ByteArray) {
	src.ReadSeek(0, bytearray.SEEK_SET)
	zr, err := zlib.NewReader(&src)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := zr.Close(); err != nil {
			panic(err)
		}
	}()
	CopyOrPanic(&dst, zr)
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
		if err != nil {
			panic(err)
		}
	}
	return zw
}
func (z *zlibPool) PutWriter(zw *zlib.Writer) {
	z.mutex.Lock()
	defer z.mutex.Unlock()
	z.writepool = append(z.writepool, zw)
}
