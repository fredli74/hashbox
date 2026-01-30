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
//                         storageIXEntry                                //
//***********************************************************************//

const storageIXEntrySize int64 = 24                                                  // 24 bytes
const storageIXEntryProbeLimit int = 682                                             // 682*24 bytes = 16368 < 16k
const storageIXFileSize = storageIXEntrySize * int64(1<<24+storageIXEntryProbeLimit) // last 24 bits of hash, plus max probe = 24*(2^24+682) = 384MiB indexes

type StorageIXEntry struct { // 24 bytes data
	flags    uint16          // 2 bytes
	blockID  core.Byte128    // 16 bytes
	location sixByteLocation // 6 bytes
}

func (e *StorageIXEntry) BlockID() core.Byte128 {
	return e.blockID
}
func (e *StorageIXEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteUint16(w, e.flags)
	size += e.blockID.Serialize(w)
	size += e.location.Serialize(w)
	return
}
func (e *StorageIXEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadUint16(r, &e.flags)
	size += e.blockID.Unserialize(r)
	size += e.location.Unserialize(r)
	return
}

// calculateIXEntryOffset calculates a start position into the index file where the blockID could be found using the following formula:
// Use only the last 24 bits of a hash, multiply that by 24 (which is the byte size of an IXEntry)  (2^24*24 = 384MiB indexes)
func calculateIXEntryOffset(blockID core.Byte128) uint32 {
	return uint32(storageFileHeaderSize) + ((uint32(blockID[15]) | uint32(blockID[14])<<8 | uint32(blockID[13])<<16) * 24)
}

// findIXOffset probes for a blockID and returns entry+file+offset if found, regardless if it is invalid or not
// if stopOnFree == true, it returns the first possible location where the block could be stored (used for compacting)
func (store *Store) findIXOffset(blockID core.Byte128, stopOnFree bool) (*StorageIXEntry, int32, int64) {
	var entry StorageIXEntry

	baseOffset := int64(calculateIXEntryOffset(blockID))
	ixFileNumber, freeFileNumber := int32(0), int32(0)
	ixOffset, freeOffset := baseOffset, baseOffset
	foundFree := false

OuterLoop:
	for {
		ixFile := store.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			core.Log(core.LogTrace, "ran out of index files")
			break
		}

		ixSize := ixFile.Size()
		if ixOffset > ixSize {
			core.Log(core.LogTrace, "%x > %x", ixOffset, ixSize)
			break
		}
		_, err := ixFile.Reader.Seek(ixOffset, io.SeekStart)
		core.AbortOnError(err)

		for i := 0; i < storageIXEntryProbeLimit; i++ {
			if ixOffset >= ixSize {
				break OuterLoop // there is room at the end of the file
			}
			entry.Unserialize(ixFile.Reader)

			if entry.flags&entryFlagExists == entryFlagExists && entry.flags&entryFlagInvalid == 0 {
				if bytes.Equal(blockID[:], entry.blockID[:]) { // found a valid entry, use it
					return &entry, ixFileNumber, ixOffset
				}
			} else { // found an invalid entry or empty space
				if !foundFree {
					freeFileNumber = ixFileNumber
					freeOffset = ixOffset
					foundFree = true
				}
				if stopOnFree || entry.flags&entryFlagExists == 0 {
					break OuterLoop // always stop on gaps because there is no way the block can exist after this position
				}
			}
			ixOffset += storageIXEntrySize
		}
		ixFileNumber++
		ixOffset = baseOffset
	}
	if foundFree {
		return nil, freeFileNumber, freeOffset
	}
	return nil, ixFileNumber, ixOffset
}

func (store *Store) readIXEntry(blockID core.Byte128) (entry *StorageIXEntry, ixFileNumber int32, ixOffset int64, err error) {
	entry, ixFileNumber, ixOffset = store.findIXOffset(blockID, false)
	if entry == nil {
		err = fmt.Errorf("block index entry not found for %x", blockID[:])
	}
	return
}

func (store *Store) writeIXEntry(ixFileNumber int32, ixOffset int64, entry *StorageIXEntry, forceFlush bool) {
	var ixFile = store.getNumberedFile(storageFileTypeIndex, ixFileNumber, true)
	_, err := ixFile.Writer.Seek(ixOffset, io.SeekStart)
	core.AbortOnError(err)
	finalFlags := entry.flags
	entry.flags |= entryFlagInvalid // Write record as invalid first
	entry.Serialize(ixFile.Writer)

	_, err = ixFile.Writer.Seek(ixOffset, io.SeekStart)
	core.AbortOnError(err)
	core.WriteUint16(ixFile.Writer, finalFlags) // Write correct flags
	if forceFlush {
		core.AbortOnError(ixFile.Sync())
	}
	core.Log(core.LogTrace, "writeIXEntry %x:%x", ixFileNumber, ixOffset)
}

func (store *Store) InvalidateIXEntry(blockID core.Byte128) {
	core.Log(core.LogDebug, "InvalidateIXEntry %x", blockID[:])

	e, f, o, err := store.readIXEntry(blockID)
	core.AbortOnError(err)

	core.ASSERT(e != nil, e)
	e.flags |= entryFlagInvalid
	// flush notice: no need to force flush index invalidation
	store.writeIXEntry(f, o, e, false)
}
