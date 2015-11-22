//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/hashbox/core"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ChannelCommand struct {
	command int
	data    interface{}
	result  chan interface{}
}

type StorageHandler struct {
	signal  chan error // goroutine signal channel, returns raised errors and stops goroutine when closed
	closing bool
	queue   chan ChannelCommand
	wg      sync.WaitGroup

	filepool         map[string]*BufferedFile
	topDatFileNumber int32
}

const (
	storagehandler_findBlock = iota
	storagehandler_readBlock
	storagehandler_writeBlock
)

func (handler *StorageHandler) dispatcher() {
	defer func() {
		// queue cleanup
		close(handler.queue)
		for q := range handler.queue {
			close(q.result)
		}

		// did this goroutine panic?
		switch r := recover().(type) {
		case error:
			serverLog(r)
			handler.signal <- r
		}
		handler.wg.Done()
	}()

	for {
		select { // Command type priority queue, top commands get executed first
		case q := <-handler.queue:
			func() {
				defer close(q.result) // Always close the result channel after returning
				switch q.command {
				case storagehandler_findBlock:
					blockID := q.data.(core.Byte128)
					_, _, _, err := handler.readIXEntry(blockID)
					q.result <- (err == nil)
				case storagehandler_readBlock:
					blockID := q.data.(core.Byte128)
					block, _ := handler.readBlockFile(blockID)
					q.result <- block
				case storagehandler_writeBlock:
					block := q.data.(*core.HashboxBlock)
					q.result <- handler.writeBlockFile(block)
				default:
					panic(errors.New(fmt.Sprintf("Unknown query in StorageHandler causing hangup: %d", q.command)))
				}
			}()
		case _, ok := <-handler.signal: // Signal is closed?
			if ok {
				panic(errors.New("We should not reach this point, it means someone outside this goroutine sent a signal on the channel"))
			}
			return
		}
	}
}

func (handler *StorageHandler) doCommand(q ChannelCommand) interface{} {
	defer func() {
		r := recover()
		select {
		case err := <-handler.signal:
			if err != nil {
				panic(errors.New("StorageHandler panic: " + err.Error()))
			}
		default:
			switch t := r.(type) {
			case error:
				panic(errors.New("StorageHandler panic: " + t.Error()))
			}
		}
	}()
	handler.queue <- q
	r := <-q.result
	return r
}

func (handler *StorageHandler) Close() {
	handler.closing = true
	close(handler.signal)
	handler.wg.Wait()

	// Close the filepool
	for s, f := range handler.filepool {
		serverLog("Closing", s)
		f.Close()
	}
}
func NewStorageHandler() *StorageHandler {
	handler := &StorageHandler{
		queue:    make(chan ChannelCommand, 32),
		signal:   make(chan error), // cannot be buffered
		filepool: make(map[string]*BufferedFile),
	}
	handler.wg.Add(1)

	go handler.dispatcher()
	return handler
}

func (handler *StorageHandler) doesBlockExist(BlockID core.Byte128) bool {
	q := ChannelCommand{storagehandler_findBlock, BlockID, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(bool)
	} else {
		return false
	}
}
func (handler *StorageHandler) readBlock(BlockID core.Byte128) *core.HashboxBlock {
	q := ChannelCommand{storagehandler_readBlock, BlockID, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(*core.HashboxBlock)
	} else {
		return nil
	}
}
func (handler *StorageHandler) writeBlock(Block *core.HashboxBlock) bool {
	q := ChannelCommand{storagehandler_writeBlock, Block, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(bool)
	} else {
		return false
	}
}

//*****************************************************************************************************************//
//
// Storage database handling below.
//
//*****************************************************************************************************************//

const (
	storageVersion            uint32 = 1
	storageFileTypeIndex      uint32 = 0x48534958 // "HSIX" Hashbox Storage Index
	storageFileExtensionIndex string = ".idx"
	storageFileTypeData       uint32 = 0x48534442 // "HSDB" Hashbox Storage Database
	storageFileExtensionData  string = ".dat"

	storageIXEntrySize       int = 24  // 24 bytes
	storageIXEntryProbeLimit int = 682 // 682*24 bytes = 16368 < 16k

	storageDataFileSize int64  = (1 << 34) - 1 // 0x3ffffffff = 16 GiB data files
	storageDataMarker   uint32 = 0x68626C6B    // "hblk"
)

const ( // 2 bytes, max 16 flags
	entryFlagExists = 1 << iota
	entryFlagNoLinks
	entryFlagMarked
)

type storageFileHeader struct {
	filetype uint32
	version  uint32
}

func (h *storageFileHeader) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, h.filetype)
	size += core.WriteOrPanic(w, h.version)
	return
}
func (h *storageFileHeader) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, &h.filetype)
	size += core.ReadOrPanic(r, &h.version)
	if h.version != storageVersion {
		panic(errors.New("Invalid version in dbFileHeader"))
	}
	return
}

type storageIXEntry struct { // 24 bytes data
	Flags   int16        // 2 bytes
	BlockID core.Byte128 // 16 bytes
	// File + offset is serialized as 6 bytes (48 bit) 256 TiB max data
	// 14 bit for filenumber = 16384 files
	// 34 bit for filesize = 16 GiB files
	Location [6]byte
	//File   int32 // int64(x) >> 34
	//Offset int64 // int64(x) & 0x3FFFFffff
}

func (e *storageIXEntry) SetLocation(File int32, Offset int64) {
	var l int64 = int64(File)<<34 | (Offset & 0x3ffffffff)
	e.Location[0] = byte(l >> 40)
	e.Location[1] = byte(l >> 32)
	e.Location[2] = byte(l >> 24)
	e.Location[3] = byte(l >> 16)
	e.Location[4] = byte(l >> 8)
	e.Location[5] = byte(l)
}
func (e *storageIXEntry) GetLocation() (File int32, Offset int64) {
	var l int64 = int64(e.Location[5]) | (int64(e.Location[4]) << 8) | (int64(e.Location[3]) << 16) | (int64(e.Location[2]) << 24) | (int64(e.Location[1]) << 32) | (int64(e.Location[0]) << 40)
	return int32(l >> 34), (l & 0x3ffffffff)
}

func (e *storageIXEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, e.Flags)
	size += e.BlockID.Serialize(w)
	size += core.WriteOrPanic(w, e.Location)
	return
}
func (e *storageIXEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, &e.Flags)
	size += e.BlockID.Unserialize(r)
	size += core.ReadOrPanic(r, &e.Location)
	return
}

type storageDataEntry struct {
	_datamarker uint32 // = storageDataMarker, used to find / align blocks in case of recovery
	Block       *core.HashboxBlock
}

func (e *storageDataEntry) Release() {
	if e.Block != nil {
		e.Block.Release()
	}
}

func (e *storageDataEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, storageDataMarker)
	size += e.Block.Serialize(w)
	return
}

// IMPORTANT Unserialize allocates memory that needs to be freed manually
func (e *storageDataEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, &e._datamarker)
	if e._datamarker != storageDataMarker {
		panic(errors.New(fmt.Sprintf("Incorrect Block on offset (%s/%s file corruption)", storageFileExtensionIndex, storageFileExtensionData)))
	}
	if e.Block == nil {
		e.Block = &core.HashboxBlock{}
	}
	size += e.Block.Unserialize(r)
	return
}

// calculateEntryOffset calculates a start position into the index file where the blockID could be found using the following formula:
// Use only the last 24 bits of a hash, multiply that by 24 (which is the byte size of an IXEntry)  (2^24*24 = 384MiB indexes)
func calculateEntryOffset(blockID core.Byte128) uint32 {
	return (uint32(blockID[15]) | uint32(blockID[14])<<8 | uint32(blockID[13])<<16) * 24
}

func (handler *StorageHandler) getNumberedName(fileType string, fileNumber int32) string {
	return fmt.Sprintf("%.8X%s", fileNumber, fileType)
}
func (handler *StorageHandler) getNumberedFileName(fileType string, fileNumber int32) string {
	return filepath.Join(datDirectory, handler.getNumberedName(fileType, fileNumber))
}
func (handler *StorageHandler) getNumberedFile(fileType string, fileNumber int32, create bool) *BufferedFile {
	name := handler.getNumberedName(fileType, fileNumber)
	if handler.filepool[name] == nil {
		filename := handler.getNumberedFileName(fileType, fileNumber)

		flag := 0
		if create {
			flag |= os.O_CREATE
		}

		f, err := OpenBufferedFile(filename, 4096, flag, 0666)
		if err != nil {
			if create {
				panic(err)
			} else {
				return nil
			}
		}
		serverLog("Opening file:", filename)
		handler.filepool[name] = f
	}
	return handler.filepool[name]
}

func (handler *StorageHandler) readIXEntry(blockID core.Byte128) (*storageIXEntry, int32, int64, error) {
	baseOffset := int64(calculateEntryOffset(blockID))
	ixFileNumber := int32(0)
	ixOffset := baseOffset

OuterLoop:
	for {
		ixFile := handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber, false)
		if ixFile == nil {
			Debug("ran out of index files")
			break
		}
		ixSize := ixFile.Size()
		if ixOffset > ixSize {
			Debug("%d > %d", ixOffset, ixSize)
			break
		}
		if _, err := ixFile.ReaderSeek(ixOffset, os.SEEK_SET); err != nil {
			panic(err)
		}

		var entry storageIXEntry
		for i := 0; i < storageIXEntryProbeLimit; i++ {
			if ixOffset >= ixSize {
				break OuterLoop
			}
			entry.Unserialize(ixFile.Reader)

			if entry.Flags&entryFlagExists == 0 {
				break OuterLoop
			}
			if bytes.Equal(blockID[:], entry.BlockID[:]) {
				return &entry, ixFileNumber, ixOffset, nil
			}
			ixOffset += int64(storageIXEntrySize)
		}
		ixFileNumber++
		ixOffset = baseOffset
	}
	return nil, ixFileNumber, ixOffset, errors.New(fmt.Sprintf("BlockID index entry not found for %x", blockID[:]))
}
func (handler *StorageHandler) writeIXEntry(ixFileNumber int32, ixOffset int64, entry *storageIXEntry) {
	var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber, true)
	ixFile.WriterSeek(ixOffset, os.SEEK_SET)
	entry.Serialize(ixFile.Writer)
	ixFile.Writer.Flush()
}

func (handler *StorageHandler) writeBlockFile(block *core.HashboxBlock) bool {
	_, ixFileNumber, ixOffset, err := handler.readIXEntry(block.BlockID)
	if err == nil {
		// Block already exists
		return false
	}

	dataEntry := storageDataEntry{Block: block}
	var data = new(bytes.Buffer)
	dataEntry.Serialize(data)

	var datFile *BufferedFile
	var datOffset int64
	for {
		datFile = handler.getNumberedFile(storageFileExtensionData, handler.topDatFileNumber, true)
		datOffset, _ = datFile.WriterSeek(0, os.SEEK_END)
		if datOffset+int64(data.Len()) <= storageDataFileSize {
			break
		}
		handler.topDatFileNumber++
	}

	data.WriteTo(datFile.Writer)
	datFile.Writer.Flush()

	ixEntry := storageIXEntry{Flags: entryFlagExists, BlockID: block.BlockID}
	if len(block.Links) == 0 {
		ixEntry.Flags |= entryFlagNoLinks
	}
	ixEntry.SetLocation(handler.topDatFileNumber, datOffset)
	handler.writeIXEntry(ixFileNumber, ixOffset, &ixEntry)
	return true
}

func (handler *StorageHandler) readBlockLinks(blockID core.Byte128, datFileNumber int32, datOffset int64) ([]core.Byte128, error) {
	var links []core.Byte128

	datFile := handler.getNumberedFile(storageFileExtensionData, datFileNumber, false)
	if datFile == nil {
		panic(errors.New(fmt.Sprintf("Error reading block %x, index is pointing to file %x which cannot be opened", blockID[:], datFileNumber)))
	}

	offset, err := datFile.ReaderSeek(datOffset+4, os.SEEK_SET) // 4 bytes to skip the datamarker
	if offset != datOffset+4 {
		panic(err)
	}

	var dataBlockID core.Byte128
	dataBlockID.Unserialize(datFile.Reader)
	if blockID.Compare(dataBlockID) != 0 {
		panic(errors.New(fmt.Sprintf("Error reading block %x, index is pointing to block %x", blockID[:], dataBlockID[:])))
	}
	var n uint32
	core.ReadOrPanic(datFile.Reader, &n)
	if n > 0 {
		links = make([]core.Byte128, n)
		for i := 0; i < int(n); i++ {
			links[i].Unserialize(datFile.Reader)
		}
	}
	return links, nil
}

// IMPORTANT readBlockFile allocates memory that needs to be freed manually
func (handler *StorageHandler) readBlockFile(blockID core.Byte128) (*core.HashboxBlock, error) {
	indexEntry, _, _, err := handler.readIXEntry(blockID)
	if err != nil {
		return nil, err
	}

	var dataEntry storageDataEntry

	file, offset := indexEntry.GetLocation()
	datFile := handler.getNumberedFile(storageFileExtensionData, file, false)
	if datFile == nil {
		panic(errors.New(fmt.Sprintf("Error reading block from file %x, offset %x, file does not exist", file, offset)))
	}
	datFile.ReaderSeek(offset, os.SEEK_SET)
	dataEntry.Unserialize(datFile.Reader)
	return dataEntry.Block, nil
}

func (handler *StorageHandler) CheckChain(roots []core.Byte128, Paint bool) {
	var chain []core.Byte128
	visited := make(map[core.Byte128]bool) // Keep track if we have read the links from the block already

	var progress time.Time
	chain = append(chain, roots...)
	for i := 0; len(chain) > 0; i++ {
		blockID := chain[len(chain)-1]
		chain = chain[:len(chain)-1]
		if !visited[blockID] {
			entry, _, _, err := handler.readIXEntry(blockID)
			if err != nil {
				panic(err)
			}
			if entry.Flags&entryFlagNoLinks == 0 {
				datFileNumber, datOffset := entry.GetLocation()
				links, err := handler.readBlockLinks(blockID, datFileNumber, datOffset)
				if err != nil {
					panic(err)
				}
				if len(links) > 0 && entry.Flags&entryFlagNoLinks == entryFlagNoLinks {
					//panic(errors.New(fmt.Sprintf("Index entry for block %x is marked with NoLinks but the block has %d links", blockID[:], len(links))))
				} else if len(links) == 0 && entry.Flags&entryFlagNoLinks == 0 {
					Debug("Block %x has no links but the index is missing NoLinks flag", blockID[:])
				}
				chain = append(chain, links...)
			}
			visited[blockID] = true
		}

		if Paint && time.Now().After(progress) {
			fmt.Printf("%8d links\r", len(chain))
			progress = time.Now().Add(500 * time.Millisecond)
		}
	}
}

func (handler *StorageHandler) MarkIndexes(roots []core.Byte128, Paint bool) {
	var chain []core.Byte128
	visited := make(map[core.Byte128]bool) // Keep track if we have read the links from the block already

	var progress time.Time
	chain = append(chain, roots...)
	for i := 0; len(chain) > 0; i++ {
		blockID := chain[len(chain)-1]
		chain = chain[:len(chain)-1]
		if !visited[blockID] {
			entry, ixFileNumber, ixOffset, err := handler.readIXEntry(blockID)
			if err != nil {
				panic(err)
			}
			if entry.Flags&entryFlagNoLinks == 0 {
				datFileNumber, datOffset := entry.GetLocation()
				links, err := handler.readBlockLinks(blockID, datFileNumber, datOffset)
				if err != nil {
					panic(err)
				}
				chain = append(chain, links...)
			}

			entry.Flags |= entryFlagMarked // Mark the entry
			{
				var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber, false)
				if ixFile == nil {
					panic(errors.New(fmt.Sprintf("Error marking index entry in file %x, offset %x, file does not exist", ixFileNumber, ixOffset)))
				}

				ixFile.WriterSeek(ixOffset, os.SEEK_SET)
				core.WriteOrPanic(ixFile.Writer, entry.Flags)
				ixFile.Writer.Flush()
			}
			visited[blockID] = true // Mark that we do not need to check this block again
		}

		if Paint && time.Now().After(progress) {
			fmt.Printf("%8d links\r", len(chain))
			progress = time.Now().Add(500 * time.Millisecond)
		}
	}
}
func (handler *StorageHandler) SweepIndexes(Paint bool) {
	var blankEntry storageIXEntry
	var blanks []int64

	deletedBlocks := 0
	for ixFileNumber := int32(0); ; ixFileNumber++ {
		var lastActiveEntry int64 = -1

		var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		if Paint {
			fmt.Printf("Sweeping index file #%d (%s)\n", ixFileNumber, core.HumanSize(ixSize))
		}

		ixFile.ReaderSeek(0, os.SEEK_SET)

		var lastProgress = -1
		for offset := int64(0); offset <= ixSize; offset += int64(storageIXEntrySize) {
			var entry storageIXEntry
			if offset < ixSize { // We do one extra step to be able to clean up the end of a file
				entry.Unserialize(ixFile.Reader)
			}

			if entry.Flags&entryFlagExists == entryFlagExists {
				if entry.Flags&entryFlagMarked == 0 {
					// Delete it! Well we need to compact or we will break linear probing
					deletedBlocks++
					blanks = append(blanks, offset)
					Debug("Deleted orphan Block %x IX from %x:%x", entry.BlockID[:], ixFileNumber, offset)
				} else {
					entry.Flags &^= entryFlagMarked

					// Move it down to a lower idx file?
					e, f, o, err := handler.readIXEntry(entry.BlockID)
					if f < ixFileNumber {
						Debug("Moving file for Block %x IX from %x:%x to %x:%x", entry.BlockID[:], ixFileNumber, offset, f, o)
						if e != nil || err == nil {
							panic("ASSERT, this makes no sense, the block is in a lower idx already?")
						}
						handler.writeIXEntry(f, o, &entry)
						deletedBlocks++
						blanks = append(blanks, offset)
					} else {
						// Move it inside same idx file
						newOffset := offset
						blockbase := int64(calculateEntryOffset(entry.BlockID))

						for i := range blanks {
							if blanks[i] >= blockbase {
								newOffset = blanks[i]
								blanks = append(blanks[:i], blanks[i+1:]...)
								blanks = append(blanks, offset)
								break
							}
						}

						if newOffset > lastActiveEntry {
							lastActiveEntry = newOffset
						}
						ixFile.WriterSeek(newOffset, os.SEEK_SET)
						if offset == newOffset { // no need to rewrite whole record
							core.WriteOrPanic(ixFile.Writer, entry.Flags)
						} else {
							Debug("Moving Block %x IX from %x:%x to %x:%x", entry.BlockID[:], ixFileNumber, offset, ixFileNumber, newOffset)
							entry.Serialize(ixFile.Writer)
						}
						ixFile.Writer.Flush()
					}
				}
			} else {

				// We found a hole, so we have nothing left to move
				for len(blanks) > 0 {
					Debug("Clearing %x:%x", ixFileNumber, blanks[0])
					ixFile.WriterSeek(blanks[0], os.SEEK_SET)
					blankEntry.Serialize(ixFile.Writer)
					blanks = blanks[1:]
				}
				ixFile.Writer.Flush()
			}

			if ixSize > 0 {
				p := int(offset * 100 / ixSize)
				if Paint && p > lastProgress {
					lastProgress = p
					fmt.Printf("%d%% %d\r", lastProgress, -deletedBlocks)
				}
			}
		}

		if lastActiveEntry < 0 {
			lastActiveEntry = 0
		} else {
			lastActiveEntry += int64(storageIXEntrySize)
		}
		if lastActiveEntry < ixSize {
			Debug("Truncating file %x at %x", ixFileNumber, lastActiveEntry)
			ixFile.WriteFile.Truncate(lastActiveEntry)
		}
	}
	if Paint {
		fmt.Printf("Removed %d blocks\n", deletedBlocks)
	}
}
func (handler *StorageHandler) CompactData(Paint bool) {
	var dataEntry storageDataEntry
	defer dataEntry.Release()

	var compacted int64
	for datFileNumber := int32(0); ; datFileNumber++ {
		var datFile = handler.getNumberedFile(storageFileExtensionData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}

		datSize := datFile.Size()
		if Paint {
			fmt.Printf("Compacting data file #%d (%s)\n", datFileNumber, core.HumanSize(datSize))
		}

		datFile.ReaderSeek(0, os.SEEK_SET)

		var lastProgress = -1
		offset, writeOffset := int64(0), int64(0)
		for offset < datSize {
			peek, err := datFile.Reader.Peek(12)
			if err != nil {
				panic(err)
			}
			if bytes.Equal(peek[:4], []byte("Cgap")) {
				skip := core.BytesInt64(peek[4:12])
				Debug("Cgap marker, jumping %d bytes", skip)
				for skip > 0 {
					n, _ := datFile.Reader.Discard(core.LimitInt(skip))
					offset += int64(n)
					skip -= int64(n)
				}
				continue
			}

			readOffset := offset
			entrySize := dataEntry.Unserialize(datFile.Reader)
			offset += int64(entrySize)

			Debug("Read %x:%x block %x (%d bytes)", datFileNumber, readOffset, dataEntry.Block.BlockID[:], entrySize)

			ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(dataEntry.Block.BlockID)
			if err != nil {
				Debug("Not in index: %s", err.Error())
				entrySize = 0 // Block not found, remove it
			} else {
				f, o := ixEntry.GetLocation()
				if f != datFileNumber || o != readOffset {
					Debug("Orphan block, index does not point to %x:%x but %x:%x", datFileNumber, readOffset, f, o)
					entrySize = 0 // Block found in a different location (most likely an interrupted compact)
				}
			}

			if readOffset != writeOffset && entrySize > 0 {
				newOffset := writeOffset
				if readOffset-writeOffset < int64(entrySize)+12 { // 12 bytes for the Cgap marker
					Debug("No room to move block to %x, %d < %d (+12 bytes)", writeOffset, readOffset-writeOffset, entrySize)

					var newDatFile *BufferedFile
					var newOffset int64
					var newDatFileNumber int32 = 0
					for {
						newDatFile = handler.getNumberedFile(storageFileExtensionData, newDatFileNumber, true)
						newOffset, err = newDatFile.WriterSeek(0, os.SEEK_END)
						if err != nil {
							panic(err)
						}
						if newOffset+int64(entrySize)+12 <= storageDataFileSize {
							break
						}
						newDatFileNumber++
					}
					written := int64(dataEntry.Serialize(newDatFile.Writer))
					newDatFile.Writer.Flush()
					Debug("Moved block to end of file %d, bytes %x:%x", newDatFileNumber, newOffset, newOffset+written)
					if newDatFileNumber == datFileNumber {
						Debug("File %d, increased in size from %x to %x", datFileNumber, datSize, datSize+written)
						datSize += written
					}

					Debug("Rewriting index for block %x to point to %x:%x", ixEntry.BlockID[:], newDatFileNumber, newOffset)
					ixEntry.SetLocation(newDatFileNumber, newOffset)
					handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry)

				} else {
					datFile.WriterSeek(newOffset, os.SEEK_SET)
					written := int64(dataEntry.Serialize(datFile.Writer))
					Debug("Moved block from %x to %x:%x", readOffset, writeOffset, writeOffset+written)
					writeOffset += written

					Debug("Writing Cgap skip marker to %x (skip %d)", readOffset, readOffset-writeOffset)

					core.WriteOrPanic(datFile.Writer, []byte("Cgap"))
					core.WriteOrPanic(datFile.Writer, readOffset-writeOffset)
					datFile.Writer.Flush()

					Debug("Rewriting index for block %x to point to %x:%x", ixEntry.BlockID[:], datFileNumber, newOffset)
					ixEntry.SetLocation(datFileNumber, newOffset)
					handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry)
				}
			} else {
				writeOffset += int64(entrySize)
				Debug("No need to write, moving writeOffset to %x", writeOffset)
			}

			p := int(offset * 100 / datSize)
			if Paint && p > lastProgress {
				//lastProgress =
				fmt.Printf("%d (%d%%)\r", writeOffset-offset, p)
			}
		}
		if writeOffset < offset {
			compacted += (offset - writeOffset)
			datFile.WriteFile.Truncate(writeOffset)
		} else if writeOffset > offset {
			panic(errors.New("WTF, you just broke the data file"))
		}
	}
	if Paint {
		fmt.Printf("Removed %s          \n", core.HumanSize(compacted))
	}
}

func (handler *StorageHandler) CheckIndexes() {

	for ixFileNumber := int32(0); ; ixFileNumber++ {
		var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		fmt.Printf("Checking index file #%d (%s)\n", ixFileNumber, core.HumanSize(ixSize))
		if ixSize%int64(storageIXEntrySize) > 0 {
			panic(errors.New(fmt.Sprintf("Index file %x size is not evenly divisable by the index entry size, file must be damaged", ixFileNumber)))
		}

		ixFile.ReaderSeek(0, os.SEEK_SET)

		var entry storageIXEntry

		var lastProgress = -1
		for offset := int64(0); offset < ixSize; {
			offset += int64(entry.Unserialize(ixFile.Reader))
			if entry.Flags&entryFlagExists == entryFlagExists { // In use
				o := int64(calculateEntryOffset(entry.BlockID))
				if offset < o {
					panic(errors.New(fmt.Sprintf("Block %x found on an invalid offset %d, it should be >= %d", entry.BlockID[:], offset, o)))
				}

				file, offset := entry.GetLocation()
				datFile := handler.getNumberedFile(storageFileExtensionData, file, false)
				if datFile == nil {
					panic(errors.New(fmt.Sprintf("Error reading block %x, index is pointing to data file %x which cannot be opened", entry.BlockID[:], file)))
				}
				datFile.ReaderSeek(offset+4, os.SEEK_SET) // 4 bytes to skip the datamarker
				var dataBlockID core.Byte128
				dataBlockID.Unserialize(datFile.Reader)
				if entry.BlockID.Compare(dataBlockID) != 0 {
					panic(errors.New(fmt.Sprintf("Error reading block %x, index is pointing to block %x", entry.BlockID[:], dataBlockID[:])))
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

func (handler *StorageHandler) CheckData(doRepair bool) (repaired int, critical int) {
	var dataEntry storageDataEntry
	defer dataEntry.Release()

	for datFileNumber := int32(0); ; datFileNumber++ {
		var datFile = handler.getNumberedFile(storageFileExtensionData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}

		datSize := datFile.Size()
		fmt.Printf("Checking data file #%d (%s)\n", datFileNumber, core.HumanSize(datSize))
		datFile.ReaderSeek(0, os.SEEK_SET)

		var lastProgress = -1

		for offset := int64(0); offset < datSize; {
			blockOffset := offset
			_ = "breakpoint"
			offset += int64(dataEntry.Unserialize(datFile.Reader))
			if !dataEntry.Block.VerifyBlock() {
				critical++
				// TODO... something!
				panic(errors.New(fmt.Sprintf("Unable to verify block %x at %d in data file %d", dataEntry.Block.BlockID, blockOffset, datFileNumber)))
			}

			rewriteIX := false
			ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(dataEntry.Block.BlockID)
			if err != nil {
				fmt.Printf("Orphan block %x (%s)\n", dataEntry.Block.BlockID[:], err.Error())
				ixEntry = &storageIXEntry{Flags: entryFlagExists, BlockID: dataEntry.Block.BlockID}
				ixEntry.SetLocation(datFileNumber, blockOffset)
				rewriteIX = true
			} else {
				f, o := ixEntry.GetLocation()
				if f != datFileNumber || o != blockOffset {
					fmt.Printf("Orphan block %x (%d:%d != %d:%d)\n", dataEntry.Block.BlockID[:], f, o, datFileNumber, blockOffset)
					ixEntry.SetLocation(datFileNumber, blockOffset)
					rewriteIX = true
				}
			}
			if ixEntry.Flags&entryFlagNoLinks == entryFlagNoLinks && len(dataEntry.Block.Links) > 0 {
				fmt.Printf("Block %x has %d links but the index NoLinks flag is set\n", dataEntry.Block.BlockID[:], len(dataEntry.Block.Links))
				ixEntry.Flags &^= entryFlagNoLinks
				rewriteIX = true
				critical++
			} else if ixEntry.Flags&entryFlagNoLinks == 0 && len(dataEntry.Block.Links) == 0 {
				fmt.Printf("Block %x has no links but the index NoLinks flag is not set\n", dataEntry.Block.BlockID[:])
				ixEntry.Flags |= entryFlagNoLinks
				rewriteIX = true
			}

			if doRepair && rewriteIX {
				fmt.Printf("REPAIRING index for block %x\n", dataEntry.Block.BlockID[:])
				handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry)
				repaired++
			}

			p := int(offset * 100 / datSize)
			if p > lastProgress {
				lastProgress = p
				fmt.Printf("%d%%\r", lastProgress)
			}
		}
	}
	return repaired, critical
}

//********************************************************************************//
//                                  BufferedFile                                  //
//********************************************************************************//

type BufferedFile struct {
	ReadFile  *os.File
	Reader    *bufio.Reader
	WriteFile *os.File
	Writer    *bufio.Writer
}

func OpenBufferedFile(name string, buffersize int, flag int, perm os.FileMode) (*BufferedFile, error) {
	b := new(BufferedFile)
	var err error
	if b.WriteFile, err = os.OpenFile(name, flag|os.O_WRONLY, perm); err != nil {
		return nil, err
	}
	if b.ReadFile, err = os.OpenFile(name, flag|os.O_RDONLY, perm); err != nil {
		return nil, err
	}
	b.Reader = bufio.NewReaderSize(b.ReadFile, buffersize)
	b.Writer = bufio.NewWriterSize(b.WriteFile, buffersize)
	return b, nil
}
func (b *BufferedFile) ReaderSeek(offset int64, whence int) (ret int64, err error) {
	b.Writer.Flush() // Always flush in case we want to read what we have written
	ret, err = b.ReadFile.Seek(offset, whence)
	if err == nil {
		b.Reader.Reset(b.ReadFile)
	}
	return ret, err
}
func (b *BufferedFile) WriterSeek(offset int64, whence int) (ret int64, err error) {
	b.Writer.Flush() // Always flush in case we want to read what we have written
	ret, err = b.WriteFile.Seek(offset, whence)
	if err == nil {
		b.Writer.Reset(b.WriteFile)
	}
	return ret, err
}
func (b *BufferedFile) Size() int64 {
	b.Writer.Flush() // Always flush in case we want to read what we have written
	_ = "breakpoint"
	size, err := b.ReadFile.Seek(0, os.SEEK_END)
	if err != nil {
		panic(err)
	}
	return size
}
func (b *BufferedFile) Close() (err error) {
	b.Writer.Flush() // Always flush in case we want to read what we have written
	if e := b.ReadFile.Close(); e != nil {
		err = e
	}
	if e := b.WriteFile.Close(); e != nil {
		err = e
	}
	return err
}
