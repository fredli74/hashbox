//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2018
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"bytes"
	"crypto/md5"
	"io"
)

// Unserializer is our own form of BinaryUnmarshaler but it works directly off a stream so we do not need to know the full size beforehand
type Unserializer interface {
	Unserialize(r io.Reader) (size int)
}

// Serializer is our own form of BinaryMarshaler but it works directly off a stream to be compatible with Unserializer
type Serializer interface {
	Serialize(w io.Writer) (size int)
}

// Byte128 is just an alias to a 16 byte array since we use a lot of 128-bit key and check values in Hashbox
type Byte128 [16]byte

func (b Byte128) Serialize(w io.Writer) (size int) {
	size += WriteBytes(w, b[:])
	return
}
func (b *Byte128) Unserialize(r io.Reader) (size int) {
	size += ReadBytes(r, b[:])
	return
}
func (b *Byte128) Compare(a Byte128) int {
	return bytes.Compare((*b)[:], a[:])
}
func (b *Byte128) Set(from []byte) {
	copy(b[:16], from)
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

type Uint32 uint32

func (m Uint32) Serialize(w io.Writer) (size int) {
	return WriteUint32(w, uint32(m))
}
func (m *Uint32) Unserialize(r io.Reader) (size int) {
	var l uint32
	size += ReadUint32(r, &l)
	*m = Uint32(l)
	return
}

// String is serialized as uint32(length) + [length]byte arrays
type String string

func (m String) Serialize(w io.Writer) (size int) {
	size += WriteUint32(w, uint32(len(m)))
	size += WriteBytes(w, []byte(m))
	return
}
func (m *String) Unserialize(r io.Reader) (size int) {
	var l uint32
	size += ReadUint32(r, &l)
	b := make([]byte, l)
	size += ReadBytes(r, b)
	*m = String(b)
	return
}

// Dataset stores the information regarding a dataset. ListH is used so that the client can make sure it has the correct listing.
type Dataset struct {
	Name  String  // Name of the Dataset
	Size  int64   // Size of all data referenced by this dataset
	ListH Byte128 // = md5(DatasetContentList)
}

func (d *Dataset) Serialize(w io.Writer) (size int) {
	size += d.Name.Serialize(w)
	size += WriteInt64(w, d.Size)
	size += d.ListH.Serialize(w)
	return
}
func (d *Dataset) Unserialize(r io.Reader) (size int) {
	size += d.Name.Unserialize(r)
	size += ReadInt64(r, &d.Size)
	size += d.ListH.Unserialize(r)
	return
}

type DatasetArray []Dataset

func (a DatasetArray) Serialize(w io.Writer) (size int) {
	size += WriteUint32(w, uint32(len(a)))
	for i := range a {
		size += a[i].Serialize(w)
	}
	return
}
func (a *DatasetArray) Unserialize(r io.Reader) (size int) {
	var n uint32
	size += ReadUint32(r, &n)
	A := make([]Dataset, n)
	for i := 0; i < int(n); i++ {
		size += A[i].Unserialize(r)
	}
	*a = A
	return
}
func (a DatasetArray) Len() int           { return len(a) }
func (a DatasetArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DatasetArray) Less(i, j int) bool { return string(a[i].Name) < string(a[j].Name) }

// DatasetState stores a specific state (snapshot) of a Dataset.
type DatasetState struct {
	StateID Byte128 // Unique ID of the state
	BlockID Byte128 // ID of the Block this Dataset is referring to
	// TODO: figure out how to calculate size and uniquesize for real, going through metadata tree when saving takes too long
	Size       int64 // Size of all data referenced by this dataset state
	UniqueSize int64 // Size of unique data (added blocks)
}

func (d DatasetState) Serialize(w io.Writer) (size int) {
	size += d.StateID.Serialize(w)
	size += d.BlockID.Serialize(w)
	size += WriteInt64(w, d.Size)
	size += WriteInt64(w, d.UniqueSize)
	return
}
func (m *DatasetState) Unserialize(r io.Reader) (size int) {
	size += m.StateID.Unserialize(r)
	size += m.BlockID.Unserialize(r)
	size += ReadInt64(r, &m.Size)
	size += ReadInt64(r, &m.UniqueSize)
	return
}

const ( // 1 byte, max 8 flags
	StateFlagInvalid = 1 << 7 // Dataset State is invalid
)

type DatasetStateEntry struct {
	StateFlags uint8 // 1 byte  Dataset state flags
	State      DatasetState
}

func (e DatasetStateEntry) Serialize(w io.Writer) (size int) {
	size += WriteUint8(w, e.StateFlags)
	size += e.State.Serialize(w)
	return
}
func (e *DatasetStateEntry) Unserialize(r io.Reader) (size int) {
	size += ReadUint8(r, &e.StateFlags)
	size += e.State.Unserialize(r)
	return
}

type DatasetStateArray []DatasetStateEntry

func (a DatasetStateArray) Serialize(w io.Writer) (size int) {
	size += WriteUint32(w, uint32(len(a)))
	for i := range a {
		size += a[i].Serialize(w)
	}
	return
}
func (a *DatasetStateArray) Unserialize(r io.Reader) (size int) {
	var n uint32
	size += ReadUint32(r, &n)
	A := make([]DatasetStateEntry, n)
	for i := 0; i < int(n); i++ {
		size += A[i].Unserialize(r)
	}
	*a = A
	return
}
func (a DatasetStateArray) Len() int      { return len(a) }
func (a DatasetStateArray) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DatasetStateArray) Less(i, j int) bool {
	for c := 0; c < len(a[i].State.StateID); c++ {
		if a[i].State.StateID[c] < a[j].State.StateID[c] {
			return true
		} else if a[i].State.StateID[c] > a[j].State.StateID[c] {
			return false
		}
	}
	return false
}
