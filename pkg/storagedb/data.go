//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
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

type storageDataEntry struct {
	datamarker uint32 // = storageDataMarker, used to find / align blocks in case of recovery
	block      *core.HashboxBlock
}

func (e *storageDataEntry) BlockID() core.Byte128 {
	return e.block.BlockID
}
func (e *storageDataEntry) Release() {
	if e.block != nil {
		e.block.Release()
	}
}

func (e *storageDataEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteUint32(w, storageDataMarker)
	size += e.block.Serialize(w)
	return
}

func (e *storageDataEntry) UnserializeHeader(r io.Reader) (size int) {
	size += core.ReadUint32(r, &e.datamarker)
	if e.datamarker != storageDataMarker {
		core.Abort("Incorrect datamarker %x (should be %x)", e.datamarker, storageDataMarker)
	}
	return
}

// IMPORTANT Unserialize allocates memory that needs to be freed manually
func (e *storageDataEntry) Unserialize(r io.Reader) (size int) {
	size += e.UnserializeHeader(r)
	if e.block == nil {
		e.block = &core.HashboxBlock{}
	}
	size += e.block.Unserialize(r)
	return
}
func (e *storageDataEntry) VerifyLocation(handler *Store, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := handler.readIXEntry(e.block.BlockID)
	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOn(err)

	f, o := metaEntry.location.Get()
	return f == fileNumber && o == fileOffset
}

func (handler *Store) writeBlockFile(block *core.HashboxBlock) bool {
	_, ixFileNumber, ixOffset, err := handler.readIXEntry(block.BlockID)
	if err == nil {
		// Block already exists
		return false
	}

	for _, r := range block.Links {
		if r.Compare(block.BlockID) == 0 {
			core.Abort("Invalid self reference in block links")
		}
	}

	datFileNumber, datOffset, datFile := handler.findFreeOffset(storageFileTypeData, -1)

	dataEntry := storageDataEntry{block: block}
	var data = new(bytes.Buffer)
	dataSize := uint32(dataEntry.Serialize(data))
	data.WriteTo(datFile.Writer)
	// flush notice: manually flush datFile before creating the meta entry
	datFile.Sync()

	metaEntry := storageMetaEntry{blockID: block.BlockID, dataSize: dataSize, links: block.Links}
	metaEntry.location.Set(datFileNumber, datOffset)
	// flush notice: writeMetaEntry always flushes meta file
	metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

	ixEntry := storageIXEntry{flags: entryFlagExists, blockID: block.BlockID}
	if len(block.Links) == 0 {
		ixEntry.flags |= entryFlagNoLinks
	}
	ixEntry.location.Set(metaFileNumber, metaOffset)
	// flush notice: no need to force writeIXEntry to flush
	handler.writeIXEntry(ixFileNumber, ixOffset, &ixEntry, false)
	return true
}

// IMPORTANT readBlockFile allocates memory that needs to be freed manually
func (handler *Store) readBlockFile(blockID core.Byte128) (*core.HashboxBlock, error) {
	indexEntry, _, _, err := handler.readIXEntry(blockID)
	if err != nil {
		return nil, err
	}

	metaFileNumber, metaOffset := indexEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOn(err)

	dataFileNumber, dataOffset := metaEntry.location.Get()
	dataFile := handler.getNumberedFile(storageFileTypeData, dataFileNumber, false)
	if dataFile == nil {
		core.Abort("Error reading block from file %x, file does not exist", dataFileNumber)
	}
	dataFile.Reader.Seek(dataOffset, io.SeekStart)

	var dataEntry storageDataEntry
	dataEntry.Unserialize(dataFile.Reader)
	return dataEntry.block, nil
}

func (handler *Store) changeDataLocation(blockID core.Byte128, fileNumber int32, fileOffset int64) {
	ixEntry, _, _, err := handler.readIXEntry(blockID)
	core.AbortOn(err)

	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOn(err)

	metaEntry.location.Set(fileNumber, fileOffset)
	handler.writeMetaEntry(metaFileNumber, metaOffset, metaEntry)
}

func writeDataGap(writer *core.BufferedWriter, size int64) {
	core.WriteBytes(writer, []byte("Cgap"))
	core.WriteInt64(writer, size)
}

func skipDataGap(reader *core.BufferedReader) int64 {
	offset := int64(0)
	peek, err := reader.Peek(12)
	core.AbortOn(err)
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
	return offset
}
