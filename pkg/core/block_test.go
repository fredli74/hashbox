//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"testing"

	"github.com/fredli74/bytearray"
)

func TestBlockCompressVerifyAndUncompress(t *testing.T) {
	var data bytearray.ByteArray
	for i := 0; i < 1000; i++ {
		data.Write([]byte("lots-of-repeated-data-1234567890-"))
	}

	block := NewHashboxBlock(BlockDataTypeZlib, data, nil)
	originalID := block.BlockID
	block.Data.ReadSeek(0, bytearray.SEEK_SET)
	originalBytes, _ := block.Data.ReadSlice()
	originalCopy := append([]byte(nil), originalBytes...) // keep a copy before compressing

	block.CompressData()
	if !block.Compressed {
		t.Fatal("block should be marked compressed after CompressData")
	}
	if block.BlockID != originalID {
		t.Fatalf("BlockID changed after compression: %x -> %x", originalID, block.BlockID)
	}
	if !block.VerifyBlock() {
		t.Fatal("compressed block failed verification")
	}

	block.UncompressData()
	if block.Compressed {
		t.Fatal("block should be uncompressed after UncompressData")
	}
	uncompressed, _ := block.Data.ReadSlice()
	if string(uncompressed) != string(originalCopy) {
		t.Fatalf("uncompressed data mismatch: %q != %q", uncompressed, originalCopy)
	}
	if !block.VerifyBlock() {
		t.Fatal("uncompressed block failed verification")
	}
}

func TestHashDataPanicsWhenCompressed(t *testing.T) {
	var data bytearray.ByteArray
	data.Write([]byte("hash-me"))
	block := NewHashboxBlock(BlockDataTypeZlib, data, nil)
	block.CompressData()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("HashData did not panic on compressed block")
		}
	}()
	block.HashData()
}

func TestVerifyBlockFailsWhenCompressedDataCorrupt(t *testing.T) {
	var data bytearray.ByteArray
	data.Write([]byte("compress-me"))
	block := NewHashboxBlock(BlockDataTypeZlib, data, nil)
	block.CompressData()

	// Corrupt the stored checksum to simulate tampering without causing zlib parsing panics
	block.BlockID[0] ^= 0xFF
	if block.VerifyBlock() {
		t.Fatal("VerifyBlock should fail on compressed block with mismatched ID")
	}
}

func TestVerifyBlockFailsWhenUncompressedDataCorrupt(t *testing.T) {
	var data bytearray.ByteArray
	data.Write([]byte("plain-data"))
	block := NewHashboxBlock(BlockDataTypeRaw, data, nil)

	// Corrupt the raw payload
	block.Data.ReadSeek(0, bytearray.SEEK_SET)
	if buf, _ := block.Data.ReadSlice(); len(buf) > 0 {
		buf[0] ^= 0xFF
	}
	if block.VerifyBlock() {
		t.Fatal("VerifyBlock should fail on corrupted uncompressed data")
	}
}

func TestVerifyBlockFailsWhenLinksCorrupt(t *testing.T) {
	var data bytearray.ByteArray
	data.Write([]byte("linked"))
	var link Byte128
	copy(link[:], []byte("link-123456789012"))
	block := NewHashboxBlock(BlockDataTypeRaw, data, []Byte128{link})

	// Tamper with the link list after BlockID was calculated
	block.Links[0][0] ^= 0xFF
	if block.VerifyBlock() {
		t.Fatal("VerifyBlock should fail on block with tampered links")
	}
}
