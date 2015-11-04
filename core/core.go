//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"crypto/md5"
	"encoding/binary"
	"io"
)

const (
	MAX_BLOCKS_IN_PIPE = 256
)

// Unserializer is our own form of BinaryUnmarshaler but it works directly off a stream so we do not need to know the full size beforehand
type Unserializer interface {
	Unserialize(r io.Reader)
}

// Serializer is our own form of BinaryMarshaler but it works directly off a stream to be compatible with Unserializer
type Serializer interface {
	Serialize(w io.Writer)
}

// Byte128 is just an alias to a 16 byte array since we use a lot of 128-bit key and check values in Hashbox
type Byte128 [16]byte

func (b Byte128) Serialize(w io.Writer) {
	WriteOrPanic(w, b[:])
}
func (b *Byte128) Unserialize(r io.Reader) {
	ReadOrPanic(r, b[:])
}

// String is serialized as uint32(length) + [length]byte arrays
type String string

func (m String) Serialize(w io.Writer) {
	WriteOrPanic(w, int32(len(m)))
	WriteOrPanic(w, []byte(m))
}
func (m *String) Unserialize(r io.Reader) {
	var l int32
	ReadOrPanic(r, &l)
	b := make([]byte, l)
	ReadOrPanic(r, b)
	*m = String(b)
}

// HashboxBlock is serialized with counters before all arrays
type HashboxBlock struct {
	BlockID Byte128   // = md5( LinkLength Links DataLength Data )
	Links   []Byte128 // Array of BlockIDs
	Data    []byte    // Block Data
}

func (b HashboxBlock) SerializeData(w io.Writer) {
	WriteOrPanic(w, uint32(len(b.Links)))
	for i := range b.Links {
		b.Links[i].Serialize(w)
	}
	WriteOrPanic(w, uint32(len(b.Data)))
	WriteOrPanic(w, b.Data)
}
func (b HashboxBlock) HashData() (BlockID Byte128) {
	hash := md5.New()
	b.SerializeData(hash)
	copy(BlockID[:], hash.Sum(nil)[:16])
	return BlockID
}
func (b HashboxBlock) Serialize(w io.Writer) {
	b.BlockID.Serialize(w)
	b.SerializeData(w)
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
	ReadOrPanic(r, &n)
	b.Data = make([]byte, n)
	ReadOrPanic(r, &b.Data)
}

// Dataset stores the information regarding a dataset. ListH is used so that the client can make sure it has the correct listing.
type Dataset struct {
	Name  String  // Name of the Dataset
	Size  uint64  // Size of all data referenced by this dataset
	ListH Byte128 // = md5(DatasetContentList)
}

func (d Dataset) Serialize(w io.Writer) {
	d.Name.Serialize(w)
	WriteOrPanic(w, d.Size)
	d.ListH.Serialize(w)
}
func (d *Dataset) Unserialize(r io.Reader) {
	d.Name.Unserialize(r)
	ReadOrPanic(r, &d.Size)
	d.ListH.Unserialize(r)
}

type DatasetArray []Dataset

func (a DatasetArray) Serialize(w io.Writer) {
	WriteOrPanic(w, uint32(len(a)))
	for i := range a {
		a[i].Serialize(w)
	}
}
func (a *DatasetArray) Unserialize(r io.Reader) {
	var n uint32
	ReadOrPanic(r, &n)
	if n > 0 {
		A := make([]Dataset, n)
		for i := 0; i < int(n); i++ {
			A[i].Unserialize(r)
		}
		*a = A
		return
	}
	*a = nil
}

// DatasetState stores a specific state (snapshot) of a Dataset.
type DatasetState struct {
	StateID    Byte128 // Unique ID of the state
	BlockID    Byte128 // ID of the Block this Dataset is referring to
	Size       uint64  // Size of all data referenced by this dataset state
	UniqueSize uint64  // Size of unique data (added blocks)
}

func (d DatasetState) Serialize(w io.Writer) {
	d.StateID.Serialize(w)
	d.BlockID.Serialize(w)
	WriteOrPanic(w, d.Size)
	WriteOrPanic(w, d.UniqueSize)
}
func (m *DatasetState) Unserialize(r io.Reader) {
	m.StateID.Unserialize(r)
	m.BlockID.Unserialize(r)
	ReadOrPanic(r, &m.Size)
	ReadOrPanic(r, &m.UniqueSize)
}

type DatasetStateArray []DatasetState

func (a DatasetStateArray) Serialize(w io.Writer) {
	WriteOrPanic(w, uint32(len(a)))
	for i := range a {
		a[i].Serialize(w)
	}
}
func (a *DatasetStateArray) Unserialize(r io.Reader) {
	var n uint32
	ReadOrPanic(r, &n)
	if n > 0 {
		A := make([]DatasetState, n)
		for i := 0; i < int(n); i++ {
			A[i].Unserialize(r)
		}
		*a = A
		return
	}
	*a = nil
}
func (a DatasetStateArray) Len() int      { return len(a) }
func (a DatasetStateArray) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DatasetStateArray) Less(i, j int) bool {
	for c := 0; c < len(a[i].StateID); c++ {
		if a[i].StateID[c] < a[j].StateID[c] {
			return true
		} else if a[i].StateID[c] > a[j].StateID[c] {
			return false
		}
	}
	return false
}

// LEGACY STUFF (to be moved)

// DEFAULTS

const DEFAULT_SERVER_IP_PORT int = 7411

// HB CONSTANTS

const BLUPP_ID_SIZE int32 = 16 // 128 bits

// HB TYPES

type BluppData struct {
	DataLength int32
	DataBytes  []byte // bytes is "owned" by 'BluppData', never shared. Destroy when finished with this entire struct.
	LinkCount  int32
	Links      []BluppId
}
type BluppId struct {
	IDBytes [BLUPP_ID_SIZE]byte // initial cap to make exported
}

// Hash wrapper that returns Byte128 type
func Hash(data []byte) Byte128 {
	return md5.Sum(data)
}

// Hmac is a standard HMAC-MD5 that runs on Byte128 types
func Hmac(data []byte, key Byte128) Byte128 {
	// ipad = 0x363636... (64 bytes)
	// opad = 0x5c5c5c... (64 bytes)
	// hmac(data, key) = md5([key ^ opad] md5([key ^ ipad] data))

	// Setup ipad and opad keys
	ipadkey := make([]byte, md5.BlockSize)
	opadkey := make([]byte, md5.BlockSize)
	copy(ipadkey, key[:])
	copy(opadkey, ipadkey)
	for i := range ipadkey {
		ipadkey[i] ^= 0x36
		opadkey[i] ^= 0x5c
	}

	// Calculate the hashes
	inner := md5.Sum(append(ipadkey, data...))
	return md5.Sum(append(opadkey, inner[:]...))
}

// DeepHmac runs N number of Hmac on the data
func DeepHmac(depth int, data []byte, key Byte128) Byte128 {
	var hash Byte128

	for N := 0; N < depth; N++ {
		hash = Hmac(data, key)
		data = hash[:]
	}
	return hash
}

func ReadOrPanic(r io.Reader, data interface{}) {
	err := binary.Read(r, binary.BigEndian, data)
	if err != nil {
		panic(err)
	}
}
func WriteOrPanic(w io.Writer, data interface{}) {
	err := binary.Write(w, binary.BigEndian, data)
	if err != nil {
		panic(err)
	}
}
