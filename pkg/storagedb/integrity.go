//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package storagedb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/fredli74/hashbox/pkg/core"
)

func forwardToDataMarker(reader *core.BufferedReader) (int64, error) {
	offset := int64(0)
	for {
		peek, err := reader.Peek(4)
		if err != nil {
			return offset, err
		}
		if uint32(peek[3])|uint32(peek[2])<<8|uint32(peek[1])<<16|uint32(peek[0])<<24 == storageDataMarker {
			core.Log(core.LogDebug, "Jumped %d bytes to next data marker in %s", offset, reader.File.Name())
			return offset, nil
		}
		reader.Discard(1)
		offset++
	}
}

func (handler *Store) CheckStorageFiles() (errorCount int) {
	for fileType := 0; fileType < len(storageFileTypeInfo); fileType++ {
		for fileNumber := int32(0); ; fileNumber++ {
			filename := handler.getNumberedFileName(fileType, fileNumber)

			f, err := core.OpenBufferedFile(filename, storageFileTypeInfo[fileType].BufferSize, 0, 0666)
			if err != nil {
				break // no more files
			}

			var header storageFileHeader
			err = binary.Read(f.Reader, binary.BigEndian, &header.filetype)
			if err != nil {
				core.Log(core.LogError, "Unable to read file type from file header in file %s", filename)
				errorCount++
				continue
			} else if header.filetype != storageFileTypeInfo[fileType].Type {
				core.Log(core.LogError, "Incorrect filetype in file %s: %x (should be %x)", filename, header.filetype, storageFileTypeInfo[fileType].Type)
				errorCount++
				continue
			} else {
				err := binary.Read(f.Reader, binary.BigEndian, &header.version)
				core.AbortOn(err)
				if header.version != storageVersion {
					core.Log(core.LogError, "Incorrect file version in file %s: %x (should be %x)", filename, header.version, storageVersion)
					errorCount++
					continue
				}
			}
		}
	}
	return errorCount
}

func (handler *Store) RecoverData(startfile int32, endfile int32) (repairCount int) {
	var dataEntry storageDataEntry
	defer dataEntry.Release()

	for datFileNumber := int32(startfile); ; datFileNumber++ {
		if endfile >= 0 && datFileNumber > endfile {
			break
		}
		var datFile = handler.getNumberedFile(storageFileTypeData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}

		datSize := datFile.Size()
		core.Log(core.LogInfo, "Scanning data file #%d (%s)", datFileNumber, core.HumanSize(datSize))
		datFile.Reader.Seek(storageFileHeaderSize, io.SeekStart)

		var lastProgress = -1
		brokenSpot := int64(0)
		for offset := int64(storageFileHeaderSize); offset < datSize; {
			offset += skipDataGap(datFile.Reader)

			blockOffset := offset

			skipToNextBlock := false
			if err := func() (err interface{}) {
				defer func() { err = recover() }()
				offset += int64(dataEntry.Unserialize(datFile.Reader))
				return nil
			}(); err != nil {
				err := fmt.Errorf("Error reading dataEntry at %x:%x (%s)", datFileNumber, blockOffset, err)
				core.Log(core.LogError, "%v", err)
				skipToNextBlock = true
			} else if err := func() (err interface{}) {
				defer func() { err = recover() }()
				if !dataEntry.block.VerifyBlock() {
					panic(errors.New("Content verification failed"))
				}
				return nil
			}(); err != nil {
				err := fmt.Errorf("Error verifying block %x (type %d, size %d) at %x:%x (%s)", dataEntry.block.BlockID, dataEntry.block.DataType, dataEntry.block.Data.Len(), datFileNumber, blockOffset, err)
				core.Log(core.LogError, "%v", err)
				skipToNextBlock = true
			}
			if skipToNextBlock {
				if brokenSpot == 0 {
					brokenSpot = blockOffset
				}
				offset = blockOffset + 1
				datFile.Reader.Seek(offset, io.SeekStart)
				if o, err := forwardToDataMarker(datFile.Reader); err == nil {
					offset += o
					core.Log(core.LogInfo, "Skipped forward to next block at %x:%x", datFileNumber, offset)
					continue
				} else {
					core.Log(core.LogInfo, "Skipped forward until %v", err)
					break
				}
			}

			dataSize := uint32(offset - blockOffset)

			if blockOffset > storageOffsetLimit {
				core.Log(core.LogError, "Offset %x for block %x is beyond offset limit, forcing a move", blockOffset, dataEntry.block.BlockID[:])
				brokenSpot = blockOffset
			}
			if brokenSpot > 0 && blockOffset-brokenSpot < 12 {
				moveFileNum, moveOffset, moveFile := handler.findFreeOffset(storageFileTypeData, -1)
				core.Log(core.LogDebug, "Rewriting block %x at (%x:%x)", dataEntry.block.BlockID[:], moveFileNum, moveOffset)
				moveSize := uint32(dataEntry.Serialize(moveFile.Writer))
				core.ASSERT(moveSize == dataSize, "incorrect move size")

				core.Log(core.LogTrace, "Creating new meta for block %x", dataEntry.block.BlockID[:])
				metaEntry := storageMetaEntry{blockID: dataEntry.block.BlockID, dataSize: dataSize, links: dataEntry.block.Links}
				metaEntry.location.Set(moveFileNum, moveOffset)
				moveFile.Sync()
				metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

				core.Log(core.LogTrace, "Creating new index for block %x", dataEntry.block.BlockID[:])
				ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(dataEntry.block.BlockID)
				if err != nil {
					ixEntry = &storageIXEntry{flags: entryFlagExists, blockID: dataEntry.block.BlockID}
				}
				ixEntry.location.Set(metaFileNumber, metaOffset)
				handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry, false)
				continue
			} else if brokenSpot > 0 {
				core.Log(core.LogTrace, "Creating a free space marker at %x:%x (skip %d bytes)", datFileNumber, brokenSpot, blockOffset-brokenSpot)
				datFile.Writer.Seek(brokenSpot, io.SeekStart)
				writeDataGap(datFile.Writer, blockOffset-brokenSpot)
				brokenSpot = 0
			}

			rewriteIX := false
			ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(dataEntry.block.BlockID)
			if err != nil {
				core.Log(core.LogDebug, "Orphan block at %x:%x (%s)", datFileNumber, blockOffset, err.Error())
				ixEntry = &storageIXEntry{flags: entryFlagExists, blockID: dataEntry.block.BlockID}
				rewriteIX = true
			} else {
				metaFileNumber, metaOffset := ixEntry.location.Get()
				metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
				if err != nil {
					core.Log(core.LogWarning, "Metadata cache error for block %x (%s)", dataEntry.block.BlockID[:], err.Error())
					rewriteIX = true
				} else {
					f, o := metaEntry.location.Get()
					if f != datFileNumber || o != blockOffset {
						core.Log(core.LogWarning, "Metadata cache location error for block %x (%x:%x != %x:%x)", dataEntry.block.BlockID[:], f, o, datFileNumber, blockOffset)
						rewriteIX = true
					}
					if metaEntry.dataSize != dataSize {
						core.Log(core.LogWarning, "Metadata cache size error for block %x (%x != %x)", dataEntry.block.BlockID[:], metaEntry.dataSize, dataSize)
						rewriteIX = true
					}
					linksOk := true
					if len(metaEntry.links) != len(dataEntry.block.Links) {
						linksOk = false
					} else {
						for i := range metaEntry.links {
							if metaEntry.links[i].Compare(dataEntry.block.Links[i]) != 0 {
								linksOk = false
							}
						}
					}
					if !linksOk {
						core.Log(core.LogWarning, "Metadata cache block links mismatch for block %x", dataEntry.block.BlockID[:])
						rewriteIX = true
					}
				}
			}
			if ixEntry.flags&entryFlagNoLinks == entryFlagNoLinks && len(dataEntry.block.Links) > 0 {
				if !rewriteIX {
					core.Log(core.LogWarning, "Block %x has %d links but the index NoLinks flag is set", dataEntry.block.BlockID[:], len(dataEntry.block.Links))
				}
				ixEntry.flags &^= entryFlagNoLinks
				rewriteIX = true
			} else if ixEntry.flags&entryFlagNoLinks == 0 && len(dataEntry.block.Links) == 0 {
				if !rewriteIX {
					core.Log(core.LogWarning, "Block %x has no links but the index NoLinks flag is not set", dataEntry.block.BlockID[:])
				}
				ixEntry.flags |= entryFlagNoLinks
				rewriteIX = true
			}

			if !rewriteIX {
				core.Log(core.LogTrace, "Block %x (%x:%x) verified", dataEntry.block.BlockID[:], datFileNumber, blockOffset)
			} else {
				core.Log(core.LogTrace, "REPAIRING meta for block %x", dataEntry.block.BlockID[:])
				metaEntry := storageMetaEntry{blockID: dataEntry.block.BlockID, dataSize: dataSize, links: dataEntry.block.Links}
				metaEntry.location.Set(datFileNumber, blockOffset)
				metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

				core.Log(core.LogTrace, "REPAIRING index for block %x", dataEntry.block.BlockID[:])
				ixEntry.location.Set(metaFileNumber, metaOffset)
				handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry, false)
				repairCount++
			}

			p := int(offset * 100 / datSize)
			if p > lastProgress {
				lastProgress = p
				fmt.Printf("%d%%\r", lastProgress)
			}
		}
		if brokenSpot > 0 {
			core.Log(core.LogDebug, "Truncating file %x at %x", datFileNumber, brokenSpot)
			datFile.Writer.File.Truncate(brokenSpot)
		}
		handler.setDeadSpace(storageFileTypeData, datFileNumber, 0, false)
	}
	return
}

func (handler *Store) checkBlockFromIXEntry(ixEntry *storageIXEntry, verifiedBlocks map[core.Byte128]bool, fullVerify bool, readOnly bool) error {
	err := (func() error {
		metaFileNumber, metaOffset := ixEntry.location.Get()
		metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
		if err != nil {
			return err
		}

		dataFileNumber, dataOffset := metaEntry.location.Get()
		dataFile := handler.getNumberedFile(storageFileTypeData, dataFileNumber, false)
		if dataFile == nil {
			return fmt.Errorf("Error reading block from file %x, file does not exist", dataFileNumber)
		}

		var dataEntry storageDataEntry
		defer dataEntry.Release()

		core.Log(core.LogTrace, "Read %x:%x block %x", dataFileNumber, dataOffset, ixEntry.blockID[:])
		dataFile.Reader.Seek(dataOffset, io.SeekStart)
		if fullVerify {
			dataEntry.Unserialize(dataFile.Reader)
			dataEntry.block.UncompressData()
			if !dataEntry.block.VerifyBlock() {
				return fmt.Errorf("Error reading block %x, content verification failed", ixEntry.blockID[:])
			}
			core.Log(core.LogTrace, "Block %x location %x:%x and content verified (%s, %.0f%% compr)", ixEntry.blockID[:], dataFileNumber, dataOffset, core.HumanSize(int64(dataEntry.block.CompressedSize)), float64(100.0*float64(dataEntry.block.UncompressedSize-dataEntry.block.CompressedSize)/float64(dataEntry.block.UncompressedSize)))
		} else {
			dataEntry.UnserializeHeader(dataFile.Reader)
			if dataEntry.block == nil {
				dataEntry.block = &core.HashboxBlock{}
			}
			dataEntry.block.UnserializeHeader(dataFile.Reader)
			if dataEntry.block.BlockID.Compare(ixEntry.blockID) != 0 {
				return fmt.Errorf("Error reading block %x, metadata cache is pointing to block %x", ixEntry.blockID[:], dataEntry.block.BlockID[:])
			}
			core.Log(core.LogTrace, "Block %x location %x:%x verified", ixEntry.blockID[:], dataFileNumber, dataOffset)
		}

		if len(metaEntry.links) > 0 && ixEntry.flags&entryFlagNoLinks == entryFlagNoLinks {
			return fmt.Errorf("Error reading block %x, index is marked having no links but the metadata cache has %d links", ixEntry.blockID[:], len(metaEntry.links))
		}
		if len(metaEntry.links) == 0 && ixEntry.flags&entryFlagNoLinks == 0 {
			return fmt.Errorf("Error reading block %x, index is marked having links but the metadata cache has 0 links", ixEntry.blockID[:])
		}
		if len(metaEntry.links) != len(dataEntry.block.Links) {
			return fmt.Errorf("Error reading block %x, metadata cache links mismatch", ixEntry.blockID[:])
		}
		for i := range metaEntry.links {
			if metaEntry.links[i].Compare(dataEntry.block.Links[i]) != 0 {
				return fmt.Errorf("Error reading block %x, metadata cache links mismatch", ixEntry.blockID[:])
			}
		}

		for _, r := range metaEntry.links {
			v, checked := verifiedBlocks[r]
			if !checked {
				rIX, _, _, err := handler.readIXEntry(r)
				if err != nil {
					return fmt.Errorf("Error in block %x, link %x does not exist", ixEntry.blockID[:], r[:])
				}
				if rIX.flags&entryFlagInvalid == entryFlagInvalid {
					return fmt.Errorf("Error in block %x, link %x is invalid", ixEntry.blockID[:], r[:])
				}
				if err := handler.checkBlockFromIXEntry(rIX, verifiedBlocks, fullVerify, readOnly); err != nil {
					return err
				}
			} else if !v {
				return fmt.Errorf("Error in block %x, link %x is invalid", ixEntry.blockID[:], r[:])
			}
		}
		return nil
	})()
	if err == nil {
		verifiedBlocks[ixEntry.blockID] = true
	} else {
		verifiedBlocks[ixEntry.blockID] = false
		if readOnly {
			core.Abort("%v", err)
		} else {
			core.Log(core.LogDebug, "%v", err)
			handler.InvalidateIXEntry(ixEntry.blockID)
		}
	}
	return err
}

func (handler *Store) CheckBlockTree(blockID core.Byte128, verifiedBlocks map[core.Byte128]bool, fullVerify bool, readOnly bool) error {
	ixEntry, _, _, err := handler.readIXEntry(blockID)
	if err != nil {
		return err
	}
	return handler.checkBlockFromIXEntry(ixEntry, verifiedBlocks, fullVerify, readOnly)
}

func (handler *Store) CheckIndexes(verifiedBlocks map[core.Byte128]bool, fullVerify bool, readOnly bool) {
	var ixEntry storageIXEntry

	for ixFileNumber := int32(0); ; ixFileNumber++ {
		ixFile := handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		core.Log(core.LogInfo, "Checking index file #%d (%s)", ixFileNumber, core.HumanSize(ixSize))
		if ixSize%storageIXEntrySize != storageFileHeaderSize {
			core.Abort("Index file %x size is not evenly divisable by the index entry size, file must be damaged", ixFileNumber)
		}

		reader, err := core.OpenBufferedReader(ixFile.Path, 32768, ixFile.Flag)
		core.AbortOn(err)
		_, err = reader.Seek(storageFileHeaderSize, io.SeekStart)
		core.AbortOn(err)

		lastProgress := -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; offset += storageIXEntrySize {
			n := int64(ixEntry.Unserialize(reader))
			core.ASSERT(n == storageIXEntrySize, n) // ixEntry unserialize broken ?

			if ixEntry.flags&entryFlagInvalid == entryFlagInvalid {
				core.Log(core.LogDebug, "Skipping invalid index entry for %x found at %x:%x", ixEntry.blockID[:], ixFileNumber, offset)
			} else if ixEntry.flags&entryFlagExists == entryFlagExists {
				o := int64(calculateIXEntryOffset(ixEntry.blockID))
				if offset < o {
					core.Abort("Block %x found on an invalid offset %x, it should be >= %x", ixEntry.blockID[:], offset, o)
				}

				v, checked := verifiedBlocks[ixEntry.blockID]
				if !checked {
					if err := handler.checkBlockFromIXEntry(&ixEntry, verifiedBlocks, fullVerify, readOnly); err != nil {
						core.Log(core.LogWarning, "Block tree for %x was marked invalid: %v", ixEntry.blockID[:], err)
					}
				} else {
					core.ASSERT(v == true, "Block %x was previously marked invalid", ixEntry.blockID[:])
				}
			}

			p := int(offset * 100 / ixSize)
			if p > lastProgress {
				lastProgress = p
				fmt.Printf("%d%%\r", lastProgress)
			}
		}
	}
}
