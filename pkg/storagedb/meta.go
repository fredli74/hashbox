//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package storagedb

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/fredli74/hashbox/pkg/core"
)

//***********************************************************************//
//                         storageMetaEntry                              //
//***********************************************************************//

// storageMetaEntry used for double-storing data links, size and location (this is to speed up links and size checking)
type storageMetaEntry struct {
	datamarker uint32          // = storageDataMarker, used to find / align blocks in case of recovery
	blockID    core.Byte128    // 16 bytes
	location   sixByteLocation // 6 bytes
	dataSize   uint32          // Size of data in .dat file (used for deadsize tracking)
	links      []core.Byte128  // Array of BlockIDs
}

func (e *storageMetaEntry) BlockID() core.Byte128 {
	return e.blockID
}

func (e *storageMetaEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteUint32(w, storageDataMarker)
	size += e.blockID.Serialize(w)
	size += e.location.Serialize(w)
	size += core.WriteUint32(w, e.dataSize)
	size += core.WriteUint32(w, uint32(len(e.links)))
	for i := range e.links {
		size += e.links[i].Serialize(w)
	}
	return
}
func (e *storageMetaEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadUint32(r, &e.datamarker)
	if e.datamarker != storageDataMarker {
		core.Abort("Incorrect metadata cache marker %x (should be %x)", e.datamarker, storageDataMarker)
	}
	size += e.blockID.Unserialize(r)
	size += e.location.Unserialize(r)
	size += core.ReadUint32(r, &e.dataSize)
	var n uint32
	size += core.ReadUint32(r, &n)
	e.links = make([]core.Byte128, n)
	for i := 0; i < int(n); i++ {
		size += e.links[i].Unserialize(r)
	}
	return
}
func (e *storageMetaEntry) VerifyLocation(handler *Store, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := handler.readIXEntry(e.blockID)
	core.AbortOn(err)
	f, o := ixEntry.location.Get()
	return f == fileNumber && o == fileOffset
}

func (handler *Store) killMetaEntry(blockID core.Byte128, metaFileNumber int32, metaOffset int64) (size int64) {
	entry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOn(err)
	if entry.blockID.Compare(blockID) == 0 {
		var data = new(bytes.Buffer)
		entrySize := entry.Serialize(data)
		handler.setDeadSpace(storageFileTypeMeta, metaFileNumber, int64(entrySize), true)
		size += int64(entrySize)

		dataFileNumber, _ := entry.location.Get()
		handler.setDeadSpace(storageFileTypeData, dataFileNumber, int64(entry.dataSize), true)
		size += int64(entry.dataSize)
	} else {
		core.Abort("Incorrect block %x (should be %x) read on metadata location %x:%x", entry.blockID[:], blockID[:], metaFileNumber, metaOffset)
	}
	return size
}
func (handler *Store) writeMetaEntry(metaFileNumber int32, metaOffset int64, entry *storageMetaEntry) (int32, int64) {
	var data = new(bytes.Buffer)
	entry.Serialize(data)

	var metaFile *core.BufferedFile
	if metaOffset == 0 { // Offset 0 does not exist as it is in the header
		metaFileNumber, metaOffset, metaFile = handler.findFreeOffset(storageFileTypeMeta, -1)
	} else {
		metaFile = handler.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
		metaFile.Writer.Seek(metaOffset, os.SEEK_SET)
	}
	data.WriteTo(metaFile.Writer)
	// flush notice: always force a flush because index will be updated next and it must point to something
	metaFile.Sync()
	core.Log(core.LogTrace, "writeMetaEntry %x:%x", metaFileNumber, metaOffset)

	return metaFileNumber, metaOffset
}
func (handler *Store) readMetaEntry(metaFileNumber int32, metaOffset int64) (metaEntry *storageMetaEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
		return
	}()

	metaFile := handler.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
	if metaFile == nil {
		return nil, fmt.Errorf("Error reading metadata cache from file %x, file does not exist", metaFileNumber)
	}
	metaFile.Reader.Seek(metaOffset, os.SEEK_SET)

	metaEntry = new(storageMetaEntry)
	metaEntry.Unserialize(metaFile.Reader)
	return
}

func (handler *Store) changeMetaLocation(blockID core.Byte128, fileNumber int32, fileOffset int64) {
	ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(blockID)
	core.AbortOn(err)
	ixEntry.location.Set(fileNumber, fileOffset)
	// flush notice: no need to flush, changeMetaLocation is only used during compacting and compact flushes between files
	handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry, false)
}
