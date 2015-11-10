//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"errors"
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
	DataType byte      // 1 byte data type
	Data     []byte    // Block Data

	encodedData []byte
}

func NewHashboxBlock(dataType byte, data []byte, links []Byte128) *HashboxBlock {
	block := HashboxBlock{Links: links, DataType: dataType, Data: data}
	block.BlockID = block.HashData()
	return &block
}

func (b *HashboxBlock) SerializeLinks(w io.Writer) {
	WriteOrPanic(w, uint32(len(b.Links)))
	for i := range b.Links {
		b.Links[i].Serialize(w)
	}
}
func (b *HashboxBlock) Serialize(w io.Writer) {
	b.BlockID.Serialize(w)
	b.SerializeLinks(w)
	WriteOrPanic(w, b.DataType)
	b.EncodeData()
	WriteOrPanic(w, uint32(len(b.encodedData)))
	WriteOrPanic(w, b.encodedData)
}
func (b *HashboxBlock) HashData() (BlockID Byte128) {
	hash := md5.New()
	b.SerializeLinks(hash)
	//	WriteOrPanic(hash, b.DataType)
	b.DecodeData()
	WriteOrPanic(hash, uint32(len(b.Data)))
	WriteOrPanic(hash, b.Data)

	copy(BlockID[:], hash.Sum(nil)[:16])
	return BlockID
}
func (b *HashboxBlock) Unserialize(r io.Reader) {
	b.BlockID.Unserialize(r)
	var n uint32
	ReadOrPanic(r, &n)
	if n > 0 {
		b.Links = make([]Byte128, n)
		for i := 0; i < int(n); i++ {
			b.Links[i].Unserialize(r)
		}
	}
	ReadOrPanic(r, &b.DataType)
	ReadOrPanic(r, &n)
	b.encodedData = make([]byte, n)
	ReadOrPanic(r, &b.encodedData)
	b.Data = nil // reset to make sure we decode the data
	b.DecodeData()
}

func (b *HashboxBlock) DecodeData() []byte {
	if b.Data == nil {
		switch b.DataType {
		case BlockDataTypeRaw:
			b.Data = b.encodedData
		case BlockDataTypeZlib:
			inbuf := bytes.NewReader(b.encodedData)
			zr, err := zlib.NewReader(inbuf)
			if err != nil {
				panic(err)
			}
			defer zr.Close()
			outbuf := &bytes.Buffer{}
			read, err := outbuf.ReadFrom(zr)
			if err != nil {
				panic(err)
			}
			_ = read
			b.Data = outbuf.Bytes()

		default:
			panic(errors.New("Unsupported Block Data Type"))
		}
	}
	return b.Data
}
func (b *HashboxBlock) DecodedSize() int64 {
	b.DecodeData()
	return int64(len(b.Data))
}

func (b *HashboxBlock) EncodeData() []byte {
	if b.encodedData == nil {
		switch b.DataType {
		case BlockDataTypeRaw:
			b.encodedData = b.Data
		case BlockDataTypeZlib:
			zw := zpool.Get()
			zw.w.Write(b.Data)
			zw.w.Close()
			b.encodedData = make([]byte, zw.buf.Len())
			copy(b.encodedData, zw.buf.Bytes())
			zpool.Put(zw)

		default:
			panic(errors.New("Unsupported Block Data Type"))
		}
	}
	return b.encodedData
}
func (b *HashboxBlock) EncodedSize() int64 {
	b.EncodeData()
	return int64(len(b.encodedData))
}

func (b *HashboxBlock) VerifyBlock() bool {
	verifyID := b.HashData()
	return bytes.Equal(verifyID[:], b.BlockID[:])
}

var zpool zlibPool

type zlibWriter struct {
	w   *zlib.Writer
	buf *bytes.Buffer
}
type zlibPool struct {
	mutex sync.Mutex
	pool  []*zlibWriter
	count int
}

func (z *zlibPool) Get() (zw *zlibWriter) {
	z.mutex.Lock()
	defer z.mutex.Unlock()
	if z.count > 0 {
		z.count--
		return z.pool[z.count]
	} else {
		var err error
		zw := zlibWriter{}
		buf := make([]byte, 0, 8*1024*1024)
		zw.buf = bytes.NewBuffer(buf)
		zw.w, err = zlib.NewWriterLevel(zw.buf, zlib.DefaultCompression)
		if err != nil {
			panic(err)
		}
		return &zw
	}
}
func (z *zlibPool) Put(zw *zlibWriter) {
	z.mutex.Lock()
	defer z.mutex.Unlock()
	zw.buf.Reset()
	zw.w.Reset(zw.buf)
	z.pool = append(z.pool[:z.count], zw)
	z.count++
}

func init() {

}
