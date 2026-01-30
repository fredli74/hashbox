//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package storagedb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fredli74/hashbox/pkg/core"
)

//***********************************************************************//
//                         StorageMetaEntry                              //
//***********************************************************************//

// StorageMetaEntry used for double-storing data links, size and location (this is to speed up links and size checking)
type StorageMetaEntry struct {
	datamarker uint32          // = storageDataMarker, used to find / align blocks in case of recovery
	blockID    core.Byte128    // 16 bytes
	location   sixByteLocation // 6 bytes
	DataSize   uint32          // Size of data in .dat file (used for deadsize tracking)
	Links      []core.Byte128  // Array of BlockIDs
}

func (e *StorageMetaEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteUint32(w, storageDataMarker)
	size += e.blockID.Serialize(w)
	size += e.location.Serialize(w)
	size += core.WriteUint32(w, e.DataSize)
	size += core.WriteUint32(w, uint32(len(e.Links)))
	for i := range e.Links {
		size += e.Links[i].Serialize(w)
	}
	return
}
func (e *StorageMetaEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadUint32(r, &e.datamarker)
	if e.datamarker != storageDataMarker {
		core.Abort("Incorrect metadata cache marker %x (should be %x)", e.datamarker, storageDataMarker)
	}
	size += e.blockID.Unserialize(r)
	size += e.location.Unserialize(r)
	size += core.ReadUint32(r, &e.DataSize)
	var n uint32
	size += core.ReadUint32(r, &n)
	e.Links = make([]core.Byte128, n)
	for i := 0; i < int(n); i++ {
		size += e.Links[i].Unserialize(r)
	}
	return
}
func (e *StorageMetaEntry) BlockID() core.Byte128 {
	return e.blockID
}
func (e *StorageMetaEntry) VerifyLocation(store *Store, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := store.readIXEntry(e.blockID)
	core.AbortOnError(err)
	f, o := ixEntry.location.Get()
	return f == fileNumber && o == fileOffset
}

func (store *Store) killMetaEntry(blockID core.Byte128, metaFileNumber int32, metaOffset int64) (size int64) {
	entry, err := store.readMetaEntry(metaFileNumber, metaOffset)
	core.AbortOnError(err)
	if entry.blockID.Compare(blockID) == 0 {
		var data = new(bytes.Buffer)
		entrySize := entry.Serialize(data)
		store.setDeadSpace(storageFileTypeMeta, metaFileNumber, int64(entrySize), true)
		size += int64(entrySize)

		dataFileNumber, _ := entry.location.Get()
		store.setDeadSpace(storageFileTypeData, dataFileNumber, int64(entry.DataSize), true)
		size += int64(entry.DataSize)
	} else {
		core.Abort("Incorrect block %x (should be %x) read on metadata location %x:%x", entry.blockID[:], blockID[:], metaFileNumber, metaOffset)
	}
	return size
}
func (store *Store) writeMetaEntry(metaFileNumber int32, metaOffset int64, entry *StorageMetaEntry) (int32, int64) {
	var data = new(bytes.Buffer)
	entry.Serialize(data)

	var metaFile *core.BufferedFile
	if metaOffset == 0 { // Offset 0 does not exist as it is in the header
		metaFileNumber, metaOffset, metaFile = store.findFreeOffset(storageFileTypeMeta, -1)
	} else {
		metaFile = store.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
		_, err := metaFile.Writer.Seek(metaOffset, io.SeekStart)
		core.AbortOnError(err)
	}
	_, err := data.WriteTo(metaFile.Writer)
	core.AbortOnError(err)
	// flush notice: always force a flush because index will be updated next and it must point to something
	core.AbortOnError(metaFile.Sync())
	core.Log(core.LogTrace, "writeMetaEntry %x:%x", metaFileNumber, metaOffset)

	return metaFileNumber, metaOffset
}
func (store *Store) readMetaEntry(metaFileNumber int32, metaOffset int64) (metaEntry *StorageMetaEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	metaFile := store.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
	if metaFile == nil {
		return nil, fmt.Errorf("error reading metadata cache from file %x, file does not exist", metaFileNumber)
	}
	_, err = metaFile.Reader.Seek(metaOffset, io.SeekStart)
	core.AbortOnError(err)

	metaEntry = new(StorageMetaEntry)
	metaEntry.Unserialize(metaFile.Reader)
	return
}

func (store *Store) changeMetaLocation(blockID core.Byte128, fileNumber int32, fileOffset int64) {
	ixEntry, ixFileNumber, ixOffset, err := store.readIXEntry(blockID)
	core.AbortOnError(err)
	ixEntry.location.Set(fileNumber, fileOffset)
	// flush notice: no need to flush, changeMetaLocation is only used during compacting and compact flushes between files
	store.writeIXEntry(ixFileNumber, ixOffset, ixEntry, false)
}
