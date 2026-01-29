//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package storagedb

import (
	"fmt"
	"io"
	"time"

	"github.com/fredli74/hashbox/pkg/core"
)

// storageEntry is used when compacting to treat data/meta uniformly.
type storageEntry interface {
	BlockID() core.Byte128
	VerifyLocation(handler *Store, fileNumber int32, fileOffset int64) bool
	core.Serializer
	core.Unserializer
}

func (handler *Store) MarkIndexes(roots []core.Byte128, Paint bool) {
	var chain []core.Byte128
	visited := make(map[core.Byte128]bool) // Keep track if we have read the links from the block already

	var progress time.Time
	chain = append(chain, roots...)
	depth := 0
	depthMarker := len(chain)
	for i := 0; len(chain) > 0; i++ {
		blockID := chain[0]
		chain = chain[1:]
		if !visited[blockID] {
			entry, ixFileNumber, ixOffset, err := handler.readIXEntry(blockID)
			core.AbortOnError(err)

			if entry.flags&entryFlagNoLinks == 0 {
				metaFileNumber, metaOffset := entry.location.Get()
				metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
				core.AbortOnError(err)
				chain = append(chain, metaEntry.Links...)
			}

			entry.flags |= entryFlagMarked // Mark the entry
			ixFile := handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
			if ixFile == nil {
				core.Abort("Error marking index entry in file %x, offset %x, file does not exist", ixFileNumber, ixOffset)
			}

			_, err = ixFile.Writer.Seek(ixOffset, io.SeekStart)
			core.AbortOnError(err)
			core.WriteUint16(ixFile.Writer, entry.flags)

			visited[blockID] = true // Mark that we do not need to check this block again
		}

		depthMarker--
		if depthMarker < 0 {
			depth++
			depthMarker = len(chain)
		}
		if Paint && time.Now().After(progress) {
			fmt.Printf("%10d links, depth %d (%d)             \r", len(chain), depth, depthMarker)
			progress = time.Now().Add(500 * time.Millisecond)
		}
	}
}
func (handler *Store) SweepIndexes(Paint bool) {
	var sweepedSize int64

	deletedBlocks := 0
	for ixFileNumber := int32(0); ; ixFileNumber++ {
		var ixFile = handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		if Paint {
			core.Log(core.LogInfo, "Sweeping index file #%d (%s)", ixFileNumber, core.HumanSize(ixSize))
		}

		// Open a separate reader that is not moved by any other routines
		reader, err := core.OpenBufferedReader(ixFile.Path, 32768, ixFile.Flag)
		core.AbortOnError(err)
		_, err = reader.Seek(storageFileHeaderSize, io.SeekStart)
		core.AbortOnError(err)

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; offset += storageIXEntrySize {
			var entry StorageIXEntry
			entry.Unserialize(reader)

			if entry.flags&entryFlagInvalid == entryFlagInvalid {
				core.Log(core.LogDebug, "Skipping invalid block index at %x:%x", ixFileNumber, offset)
			} else if entry.flags&entryFlagExists == entryFlagExists {
				if entry.flags&entryFlagMarked == 0 {
					metaFileNumber, metaOffset := entry.location.Get()
					sweepedSize += handler.killMetaEntry(entry.blockID, metaFileNumber, metaOffset)

					// Mark it as invalid as it is now deleted
					deletedBlocks++
					entry.flags |= entryFlagInvalid
					core.Log(core.LogDebug, "Deleted orphan block index %x at %x:%x", entry.blockID[:], ixFileNumber, offset)
				} else {
					entry.flags &^= entryFlagMarked // remove entryFlagMarked

					e, eFileNumber, eOffset := handler.findIXOffset(entry.blockID, true)
					if eFileNumber == ixFileNumber && eOffset == offset { // already at best location
						core.Log(core.LogDebug, "Removed mark from block index at %x:%x", ixFileNumber, offset)
					} else if e != nil { // obsolete entry
						entry.flags |= entryFlagInvalid
						core.Log(core.LogDebug, "Deleted obsolete block index %x at %x:%x", entry.blockID[:], ixFileNumber, offset)
					} else if eFileNumber < ixFileNumber {
						handler.writeIXEntry(eFileNumber, eOffset, &entry, false) // move it to an earlier file
						entry.flags |= entryFlagInvalid                           // delete old entry
						core.Log(core.LogDebug, "Moved block index %x from %x:%x to %x:%x", entry.blockID[:], ixFileNumber, offset, eFileNumber, eOffset)
					} else if eFileNumber == ixFileNumber && eOffset < offset {
						handler.writeIXEntry(eFileNumber, eOffset, &entry, false) // move it to an earlier position
						entry.flags |= entryFlagInvalid                           // delete old entry
						core.Log(core.LogDebug, "Moved block index %x from %x:%x to %x", entry.blockID[:], ixFileNumber, offset, eOffset)
					} else {
						core.Abort("findIXOffset for %x (%x:%x) returned an invalid offset %x:%x", entry.blockID[:], ixFileNumber, offset, eFileNumber, eOffset)
					}
				}
				_, err = ixFile.Writer.Seek(offset, io.SeekStart)
				core.AbortOnError(err)
				core.WriteUint16(ixFile.Writer, entry.flags)
			}

			if ixSize > 0 {
				p := int(offset * 100 / ixSize)
				if Paint && p > lastProgress {
					lastProgress = p
					fmt.Printf("%d%% %d\r", lastProgress, -deletedBlocks)
				}
			}
		}
	}
	if Paint {
		core.Log(core.LogInfo, "Removed %d blocks (referencing %s data)", deletedBlocks, core.ShortHumanSize(sweepedSize))
	}
}

func (handler *Store) CompactIndexes(Paint bool) {
	var blankEntry StorageIXEntry
	clearedBlocks := 0
	var err error

	for ixFileNumber := int32(0); ; ixFileNumber++ {
		var ixFile = handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		if Paint {
			core.Log(core.LogInfo, "Compacting index file #%d (%s)", ixFileNumber, core.HumanSize(ixSize))
		}

		_, err = ixFile.Reader.Seek(storageFileHeaderSize, io.SeekStart)
		core.AbortOnError(err)

		truncPoint := int64(storageFileHeaderSize)

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; offset += storageIXEntrySize {
			var entry StorageIXEntry
			entry.Unserialize(ixFile.Reader)

			if entry.flags&entryFlagInvalid == entryFlagInvalid {
				clearedBlocks++
				_, err = ixFile.Writer.Seek(offset, io.SeekStart)
				core.AbortOnError(err)
				blankEntry.Serialize(ixFile.Writer)
				core.Log(core.LogDebug, "Cleared index at %x:%x", ixFileNumber, offset)
			} else if entry.flags&entryFlagExists == entryFlagExists {
				truncPoint = offset + storageIXEntrySize
			}

			if ixSize > 0 {
				p := int(offset * 100 / ixSize)
				if Paint && p > lastProgress {
					lastProgress = p
					fmt.Printf("%d%% %d\r", lastProgress, -clearedBlocks)
				}
			}
		}

		if truncPoint < ixSize {
			core.Log(core.LogDebug, "Truncating index file #%d at %x", ixFileNumber, truncPoint)
			core.AbortOnError(ixFile.Writer.File.Truncate(truncPoint))
		}
	}
	if Paint {
		core.Log(core.LogInfo, "Cleared %d blocks", clearedBlocks)
	}
}

func (handler *Store) CompactFile(fileType int, fileNumber int32) int64 {
	var entry storageEntry
	switch fileType {
	case storageFileTypeMeta:
		metaEntry := new(StorageMetaEntry)
		entry = metaEntry
	case storageFileTypeData:
		dataEntry := new(StorageDataEntry)
		defer dataEntry.Release()
		entry = dataEntry
	}

	file := handler.getNumberedFile(fileType, fileNumber, false)
	fileSize, deadSpace, err := handler.getNumberedFileSize(fileType, fileNumber)
	core.AbortOnError(err)
	core.Log(core.LogInfo, "Compacting file %s, %s (est. dead data %s)", file.Path, core.HumanSize(fileSize), core.HumanSize(deadSpace))

	reader, err := core.OpenBufferedReader(file.Path, 32768, file.Flag)
	core.AbortOnError(err)
	_, err = reader.Seek(storageFileHeaderSize, io.SeekStart)
	core.AbortOnError(err)

	type relocation struct {
		blockID    core.Byte128
		fileNumber int32
		fileOffset int64
	}
	relocations := []relocation{}

	handler.topFileNumber[fileType] = 0

	var lastProgress = -1
	deleted, moved := int64(0), int64(0)
	offset, highWaterMark := int64(storageFileHeaderSize), int64(storageFileHeaderSize)
	for offset < fileSize {
		skip, err := skipDataGap(reader)
		core.AbortOnError(err)
		deleted += skip
		offset += skip

		readOffset := offset
		entrySize := entry.Unserialize(reader)
		offset += int64(entrySize)
		entryBlockID := entry.BlockID()

		core.Log(core.LogTrace, "Read %x:%x block %x (%d bytes)", fileNumber, readOffset, entryBlockID[:], entrySize)

		if _, _, _, err = handler.readIXEntry(entryBlockID); err != nil {
			core.Log(core.LogDebug, "Removed %x:%x block %x (%s)", fileNumber, readOffset, entryBlockID[:], err.Error())
			deleted += int64(entrySize)
		} else if !entry.VerifyLocation(handler, fileNumber, readOffset) {
			core.Log(core.LogDebug, "Removed %x:%x block %x (obsolete entry)", fileNumber, readOffset, entryBlockID[:])
			deleted += int64(entrySize)
		} else {
			freeFileNum, freeOffset, freeFile := handler.findFreeOffset(fileType, fileNumber)
			if freeFileNum >= fileNumber && readOffset == highWaterMark {
				highWaterMark = offset
				core.Log(core.LogTrace, "No need to write, moving highWaterMark to %x", highWaterMark)
			} else if free, _ := core.FreeSpace(handler.DataDir); free < int64(entrySize)+MINIMUM_DAT_FREE {
				highWaterMark = fileSize
				core.Log(core.LogWarning, "Unable to move block %x (%d bytes) because there is not enough free space on data path", entryBlockID[:], entrySize)
				break
			} else {
				written := int64(entry.Serialize(freeFile.Writer))
				core.ASSERT(written == int64(entrySize), "incorrect written size")
				core.Log(core.LogDebug, "Copied block %x (%d bytes) from %x:%x to %x:%x", entryBlockID[:], written, fileNumber, readOffset, freeFileNum, freeOffset)
				moved += int64(entrySize)

				if freeFileNum == fileNumber {
					core.Log(core.LogTrace, "File %s, increased in size from %x to %x", freeFile.Path, fileSize, fileSize+written)
					fileSize += written
				}

				relocations = append(relocations, relocation{blockID: entryBlockID, fileNumber: freeFileNum, fileOffset: freeOffset})
			}
		}

		p := int(offset * 100 / fileSize)
		if p > lastProgress {
			lastProgress = p
			fmt.Printf("%d (%d%%)\r", 0-deleted, lastProgress)
		}
	}

	if len(relocations) > 0 {
		handler.SyncAll()
		for n := 0; n < len(relocations); n++ {
			core.Log(core.LogDebug, "Relocate block %x to %x:%x", relocations[n].blockID[:], relocations[n].fileNumber, relocations[n].fileOffset)
			switch fileType {
			case storageFileTypeMeta:
				handler.changeMetaLocation(relocations[n].blockID, relocations[n].fileNumber, relocations[n].fileOffset)
			case storageFileTypeData:
				handler.changeDataLocation(relocations[n].blockID, relocations[n].fileNumber, relocations[n].fileOffset)
			}
		}
		handler.SyncAll()
	}

	if highWaterMark < fileSize {
		file.Writer.File.Truncate(highWaterMark)
		handler.setDeadSpace(fileType, fileNumber, 0, false)
		core.Log(core.LogInfo, "Released %s (%s deleted, %s moved) from file %s (size %s)", core.HumanSize(offset-highWaterMark), core.HumanSize(deleted), core.HumanSize(moved), file.Path, core.HumanSize(highWaterMark))
	} else {
		core.Log(core.LogInfo, "Skipped (%s deleted, %s moved) file %s (size %s)", core.HumanSize(deleted), core.HumanSize(moved), file.Path, core.HumanSize(highWaterMark))
	}

	return deleted
}
func (handler *Store) CompactAll(fileType int, threshold int) {
	var compacted int64
	for fileNumber := int32(0); ; fileNumber++ {
		file := handler.getNumberedFile(fileType, fileNumber, false)
		if file == nil {
			break // no more data
		}
		fileSize, deadSpace, err := handler.getNumberedFileSize(fileType, fileNumber)
		core.AbortOnError(err)
		if fileSize < storageOffsetLimit/100 || int(deadSpace*100/fileSize) >= threshold {
			compacted += handler.CompactFile(fileType, fileNumber)
		} else {
			core.Log(core.LogInfo, "Skipping compact on file %s, est. dead data %s is less than %d%%", file.Path, core.HumanSize(deadSpace), threshold)
		}
		if err != nil {
			break // no more data files
		}

	}
	core.Log(core.LogInfo, "All %s files compacted, %s released", storageFileTypeInfo[fileType].Extension, core.HumanSize(compacted))
}
