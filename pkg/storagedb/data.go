//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package storagedb

import (
	"bytes"
	"io"

	"github.com/fredli74/hashbox/pkg/core"
)

//***********************************************************************//
//                         storageDataEntry                              //
//***********************************************************************//

type StorageDataEntry struct {
	datamarker uint32 // = storageDataMarker, used to find / align blocks in case of recovery
	block      *core.HashboxBlock
}

func (e *StorageDataEntry) BlockID() core.Byte128 {
	return e.block.BlockID
}
func (e *StorageDataEntry) Release() {
	if e.block != nil {
		e.block.Release()
	}
}

func (e *StorageDataEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteUint32(w, storageDataMarker)
	size += e.block.Serialize(w)
	return
}

func (e *StorageDataEntry) UnserializeHeader(r io.Reader) (size int) {
	size += core.ReadUint32(r, &e.datamarker)
	if e.datamarker != storageDataMarker {
		core.Abort("Incorrect datamarker %x (should be %x)", e.datamarker, storageDataMarker)
	}
	return
}

// Unserialize allocates memory that needs to be freed manually.
// IMPORTANT Unserialize allocates memory that needs to be freed manually
func (e *StorageDataEntry) Unserialize(r io.Reader) (size int) {
	size += e.UnserializeHeader(r)
	if e.block == nil {
		e.block = &core.HashboxBlock{}
	}
	size += e.block.Unserialize(r)
	return
}
func (e *StorageDataEntry) VerifyLocation(store *Store, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := store.readIXEntry(e.block.BlockID)
	core.AbortOnError(err)
	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := store.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOnError(err)

	f, o := metaEntry.location.Get()
	return f == fileNumber && o == fileOffset
}

func (store *Store) writeBlockFile(block *core.HashboxBlock) bool {
	_, ixFileNumber, ixOffset, err := store.readIXEntry(block.BlockID)
	if err == nil {
		// Block already exists
		return false
	}

	for _, r := range block.Links {
		if r.Compare(block.BlockID) == 0 {
			core.Abort("Invalid self reference in block links")
		}
	}

	datFileNumber, datOffset, datFile := store.findFreeOffset(storageFileTypeData, -1)

	dataEntry := StorageDataEntry{block: block}
	var data = new(bytes.Buffer)
	dataSize := uint32(dataEntry.Serialize(data))
	_, err = data.WriteTo(datFile.Writer)
	core.AbortOnError(err)
	// flush notice: manually flush datFile before creating the meta entry
	core.AbortOnError(datFile.Sync())

	metaEntry := StorageMetaEntry{blockID: block.BlockID, DataSize: dataSize, Links: block.Links}
	metaEntry.location.Set(datFileNumber, datOffset)
	// flush notice: writeMetaEntry always flushes meta file
	metaFileNumber, metaOffset := store.writeMetaEntry(0, 0, &metaEntry)

	ixEntry := StorageIXEntry{flags: entryFlagExists, blockID: block.BlockID}
	if len(block.Links) == 0 {
		ixEntry.flags |= entryFlagNoLinks
	}
	ixEntry.location.Set(metaFileNumber, metaOffset)
	// flush notice: no need to force writeIXEntry to flush
	store.writeIXEntry(ixFileNumber, ixOffset, &ixEntry, false)
	return true
}

// IMPORTANT readBlockFile allocates memory that needs to be freed manually
func (store *Store) readBlockFile(blockID core.Byte128) (*core.HashboxBlock, error) {
	indexEntry, _, _, err := store.readIXEntry(blockID)
	if err != nil {
		return nil, err
	}

	metaFileNumber, metaOffset := indexEntry.location.Get()
	metaEntry, err := store.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOnError(err)

	dataFileNumber, dataOffset := metaEntry.location.Get()
	dataFile := store.getNumberedFile(storageFileTypeData, dataFileNumber, false)
	if dataFile == nil {
		core.Abort("Error reading block from file %x, file does not exist", dataFileNumber)
	}
	_, err = dataFile.Reader.Seek(dataOffset, io.SeekStart)
	core.AbortOnError(err)

	var dataEntry StorageDataEntry
	dataEntry.Unserialize(dataFile.Reader)
	return dataEntry.block, nil
}

func (store *Store) changeDataLocation(blockID core.Byte128, fileNumber int32, fileOffset int64) {
	ixEntry, _, _, err := store.readIXEntry(blockID)
	core.AbortOnError(err)

	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := store.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOnError(err)

	metaEntry.location.Set(fileNumber, fileOffset)
	store.writeMetaEntry(metaFileNumber, metaOffset, metaEntry)
}

func writeDataGap(writer *core.BufferedWriter, size int64) {
	core.WriteBytes(writer, []byte("Cgap"))
	core.WriteInt64(writer, size)
}

func skipDataGap(reader *core.BufferedReader) (int64, error) {
	offset := int64(0)
	peek, err := reader.Peek(12)
	if err != nil {
		return 0, err
	}
	if bytes.Equal(peek[:4], []byte("Cgap")) {
		skip := core.BytesInt64(peek[4:12])
		core.Log(core.LogDebug, "Cgap marker found in %s, jumping %d bytes", reader.File.Name(), skip)
		// Special skip logic since Discard only takes int32 and not int64
		for skip > 0 {
			n, _ := reader.Discard(core.LimitInt(skip))
			offset += int64(n)
			skip -= int64(n)
		}
	}
	return offset, nil
}
