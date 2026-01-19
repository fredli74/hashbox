//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package storagedb

import (
	"testing"

	"github.com/fredli74/bytearray"
	"github.com/fredli74/hashbox/pkg/core"
)

func TestStoreWriteReadBlock(t *testing.T) {
	dataDir := t.TempDir()
	indexDir := t.TempDir()

	store := NewStore(dataDir, indexDir)
	t.Cleanup(store.Close)

	var data bytearray.ByteArray
	data.Write([]byte("storagedb-smoke-data-12345"))
	block := core.NewHashboxBlock(core.BlockDataTypeZlib, data, nil)
	originalID := block.BlockID

	if ok := store.WriteBlock(block); !ok {
		t.Fatal("expected WriteBlock to store new block")
	}
	if ok := store.WriteBlock(block); ok {
		t.Fatal("expected WriteBlock to reject duplicate block")
	}
	if !store.DoesBlockExist(block.BlockID) {
		t.Fatal("expected block to exist after write")
	}

	meta := store.ReadBlockMeta(block.BlockID)
	if meta == nil {
		t.Fatal("expected metadata for stored block")
	}
	if meta.BlockID() != originalID {
		t.Fatalf("metadata block id mismatch: %x != %x", meta.BlockID(), originalID)
	}
	if meta.DataSize == 0 {
		t.Fatal("expected metadata data size to be set")
	}

	loaded := store.ReadBlock(block.BlockID)
	if loaded == nil {
		t.Fatal("expected stored block to be readable")
	}
	t.Cleanup(loaded.Release)
	if loaded.BlockID != originalID {
		t.Fatalf("stored block id mismatch: %x != %x", loaded.BlockID, originalID)
	}

	loaded.UncompressData()
	loaded.Data.ReadSeek(0, bytearray.SEEK_SET)
	readBytes, _ := loaded.Data.ReadSlice()
	if string(readBytes) != "storagedb-smoke-data-12345" {
		t.Fatalf("stored block data mismatch: %q", string(readBytes))
	}
}
