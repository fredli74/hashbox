//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2018
//	+---+´

package main

import (
	"github.com/fredli74/hashbox/core"

	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

//********************************************************************************//
//                                 StorageHandler                                 //
//********************************************************************************//

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

	filepool      map[string]*core.BufferedFile
	topFileNumber []int32
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
			core.Log(core.LogError, "%v", r)
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
					abort("Unknown query in StorageHandler causing hangup: %d", q.command)
				}
			}()
		case _, ok := <-handler.signal: // Signal is closed?
			ASSERT(!ok, ok) // We should not reach this point with "ok", it means someone outside this goroutine sent a signal on the channel
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
				abort("StorageHandler panic: %v", err.Error())
			}
		default:
			switch t := r.(type) {
			case error:
				abort("StorageHandler panic: %v", t.Error())
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
		core.Log(core.LogInfo, "Closing %s", s)
		f.Close()
	}
}
func NewStorageHandler() *StorageHandler {
	handler := &StorageHandler{
		queue:         make(chan ChannelCommand, 32),
		signal:        make(chan error), // cannot be buffered
		filepool:      make(map[string]*core.BufferedFile),
		topFileNumber: []int32{-1, -1, -1},
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

const MINIMUM_IX_FREE = int64(storageIXFileSize + storageIXFileSize/20) // 105% of an IX file because we must be able to create a new one
const MINIMUM_DAT_FREE = int64(1 << 26)                                 // 64 MB minimum free space
func (handler *StorageHandler) checkFree(size int64) bool {
	if free, _ := core.FreeSpace(idxDirectory); free < MINIMUM_IX_FREE {
		core.Log(core.LogWarning, "Storage rejected because free space on index path has dropped below %d", MINIMUM_IX_FREE)
		return false
	}
	if free, _ := core.FreeSpace(datDirectory); free < size+MINIMUM_DAT_FREE {
		core.Log(core.LogWarning, "Storage rejected because free space on data path has dropped below %d", size+MINIMUM_DAT_FREE)
		return false
	}
	return true
}

//********************************************************************************//
//                                                                                //
//                      Storage database handling                                 //
//                                                                                //
//********************************************************************************//

type _storageFileTypeInfo struct {
	Type       uint32
	Extension  string
	BufferSize int
}

const (
	storageFileTypeIndex = iota
	storageFileTypeMeta
	storageFileTypeData
)

var storageFileTypeInfo []_storageFileTypeInfo = []_storageFileTypeInfo{ // datamarker, extension, buffersize
	_storageFileTypeInfo{0x48534958, ".idx", 2048},  // "HSIX" Hashbox Storage Index
	_storageFileTypeInfo{0x48534D44, ".meta", 1024}, // "HSMD" Hashbox Storage Meta
	_storageFileTypeInfo{0x48534442, ".dat", 3072},  // "HSDB" Hashbox Storage Data
}

const (
	storageVersion        uint32 = 1
	storageFileHeaderSize int64  = 16 // 16 bytes (filetype + version + deadspace)

	storageDataMarker  uint32 = 0x68626C6B    // "hblk"
	storageOffsetLimit int64  = (1 << 34) - 1 // 0x3ffffffff ~= 16 GiB data and metadata files
)

const ( // 2 bytes, max 16 flags
	entryFlagExists  = 1 << iota // index entry exists
	entryFlagNoLinks             // index entry has no links
	entryFlagMarked              // sweep marker
	entryFlagInvalid             // entry is invalid
)

type storageFileHeader struct {
	filetype  uint32
	version   uint32
	deadspace int64
}

func (h *storageFileHeader) Serialize(w io.Writer) (size int) {
	size += core.WriteUint32(w, h.filetype)
	size += core.WriteUint32(w, h.version)
	size += core.WriteInt64(w, h.deadspace)
	return
}
func (h *storageFileHeader) Unserialize(r io.Reader) (size int) {
	size += core.ReadUint32(r, &h.filetype)
	size += core.ReadUint32(r, &h.version)
	if h.version != storageVersion {
		abort("Invalid version in dbFileHeader")
	}
	size += core.ReadInt64(r, &h.deadspace)
	return
}

// sixByteLocation uses 6 bytes (48 bit) to reference a file number and file offset
// 14 bit for filenumber = 16384 files   (int64(x) >> 34)
// 34 bit for filesize = 16 GiB files    (int64(x) & 0x3ffffffff)
// total addressable storage = 256TiB
type sixByteLocation [6]byte

func (b *sixByteLocation) Set(File int32, Offset int64) {
	ASSERT(File >= 0 && File <= 0x3fff, File)
	ASSERT(Offset >= 0 && Offset <= 0x3ffffffff, Offset)

	var l int64 = int64(File)<<34 | (Offset & 0x3ffffffff)
	b[0] = byte(l >> 40)
	b[1] = byte(l >> 32)
	b[2] = byte(l >> 24)
	b[3] = byte(l >> 16)
	b[4] = byte(l >> 8)
	b[5] = byte(l)
}
func (b sixByteLocation) Get() (File int32, Offset int64) {
	var l int64 = int64(b[5]) | (int64(b[4]) << 8) | (int64(b[3]) << 16) | (int64(b[2]) << 24) | (int64(b[1]) << 32) | (int64(b[0]) << 40)
	return int32(l >> 34), (l & 0x3ffffffff)
}
func (b sixByteLocation) Serialize(w io.Writer) (size int) {
	return core.WriteBytes(w, b[:])
}
func (b *sixByteLocation) Unserialize(r io.Reader) (size int) {
	return core.ReadBytes(r, b[:])
}

//*******************************************************************************//
//                           storageIXEntry                                      //
//*******************************************************************************//

const storageIXEntrySize int64 = 24                                                  // 24 bytes
const storageIXEntryProbeLimit int = 682                                             // 682*24 bytes = 16368 < 16k
const storageIXFileSize = storageIXEntrySize * int64(1<<24+storageIXEntryProbeLimit) // last 24 bits of hash, plus max probe = 24*(2^24+682) = 384MiB indexes

type storageIXEntry struct { // 24 bytes data
	flags    uint16          // 2 bytes
	blockID  core.Byte128    // 16 bytes
	location sixByteLocation // 6 bytes
}

func (e *storageIXEntry) BlockID() core.Byte128 {
	return e.blockID
}

func (e *storageIXEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteUint16(w, e.flags)
	size += e.blockID.Serialize(w)
	size += e.location.Serialize(w)
	return
}
func (e *storageIXEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadUint16(r, &e.flags)
	size += e.blockID.Unserialize(r)
	size += e.location.Unserialize(r)
	return
}

//******************************************************************************//
//                                 storageEntry                                 //
//******************************************************************************//
type storageEntry interface {
	BlockID() core.Byte128
	ChangeLocation(handler *StorageHandler, fileNumber int32, fileOffset int64)
	VerifyLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) bool
	core.Serializer
	core.Unserializer
}

//*******************************************************************************//
//                          storageMetaEntry                                     //
//*******************************************************************************//

// storageMetaEntry used for double-storing data links, size and location (this is to speed up links and size checking)
type storageMetaEntry struct {
	datamarker uint32          // = storageDataMarker, used to find / align blocks in case of recovery
	blockID    core.Byte128    // 16 bytes
	location   sixByteLocation // 6 bytes
	dataSize   uint32          // Size of hashboxBlock Data
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
		abort("Incorrect metadata cache marker %x (should be %x)", e.datamarker, storageDataMarker)
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
func (e *storageMetaEntry) ChangeLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) {
	ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(e.blockID)
	abortOn(err)
	ixEntry.location.Set(fileNumber, fileOffset)
	// flush notice: tell writeIXEntry to flush on moves because old data might be overwritten next
	handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry, true)
}
func (e *storageMetaEntry) VerifyLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := handler.readIXEntry(e.blockID)
	abortOn(err)
	f, o := ixEntry.location.Get()
	return f == fileNumber && o == fileOffset
}

//******************************************************************************//
//                               storageDataEntry                               //
//******************************************************************************//

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
		abort("Incorrect datamarker %x (should be %x)", e.datamarker, storageDataMarker)
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
func (e *storageDataEntry) ChangeLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) {
	ixEntry, _, _, err := handler.readIXEntry(e.block.BlockID)
	abortOn(err)

	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	abortOn(err)

	metaEntry.location.Set(fileNumber, fileOffset)
	handler.writeMetaEntry(metaFileNumber, metaOffset, metaEntry)
}
func (e *storageDataEntry) VerifyLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := handler.readIXEntry(e.block.BlockID)
	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	abortOn(err)

	f, o := metaEntry.location.Get()
	return f == fileNumber && o == fileOffset
}

//********************************************************************************//
//                        storage file handling                                   //
//********************************************************************************//

func (handler *StorageHandler) getNumberedName(fileType int, fileNumber int32) string {
	return fmt.Sprintf("%.8X%s", fileNumber, storageFileTypeInfo[fileType].Extension)
}
func (handler *StorageHandler) getNumberedFileName(fileType int, fileNumber int32) string {
	var path string
	switch fileType {
	case storageFileTypeData:
		path = datDirectory
	case storageFileTypeIndex:
		path = idxDirectory
	case storageFileTypeMeta:
		path = idxDirectory
	}
	return filepath.Join(path, handler.getNumberedName(fileType, fileNumber))
}
func (handler *StorageHandler) getNumberedFile(fileType int, fileNumber int32, create bool) *core.BufferedFile {
	name := handler.getNumberedName(fileType, fileNumber)
	if handler.filepool[name] == nil {
		filename := handler.getNumberedFileName(fileType, fileNumber)

		flag := 0
		if create {
			flag |= os.O_CREATE
		}

		f, err := core.OpenBufferedFile(filename, storageFileTypeInfo[fileType].BufferSize, flag, 0666)
		if err != nil {
			if create {
				abort("%v", err)
			} else {
				return nil
			}
		}
		var header storageFileHeader
		if f.Size() == 0 { // New file, write a header
			header.filetype = storageFileTypeInfo[fileType].Type
			header.version = storageVersion
			header.Serialize(f.Writer)
		} else {
			header.Unserialize(f.Reader)
			if header.filetype != storageFileTypeInfo[fileType].Type {
				abort("Trying to read storage file %s with the wrong file type header: %x (was expecting %x)", filename, header.filetype, storageFileTypeInfo[fileType].Type)
			}
		}
		core.Log(core.LogInfo, "Opening file: %s", filename)
		handler.filepool[name] = f
	}
	return handler.filepool[name]
}

// getNumberedFileSize is used to read the dead space value from a meta or data file header
func (handler *StorageHandler) getNumberedFileSize(fileType int, fileNumber int32) (size int64, deadspace int64, err error) {
	file := handler.getNumberedFile(fileType, fileNumber, false)
	if file == nil {
		return 0, 0, errors.New(fmt.Sprintf("Trying to read free space from %.8X%s which does not exist", fileNumber, storageFileTypeInfo[fileType].Extension))
	}
	file.Reader.Seek(0, os.SEEK_SET)
	var header storageFileHeader
	header.Unserialize(file.Reader)
	return file.Size(), header.deadspace, nil
}

// setDeadSpace is used to mark the amount of dead space in a meta or data file
func (handler *StorageHandler) setDeadSpace(fileType int, fileNumber int32, size int64, add bool) {
	file := handler.getNumberedFile(fileType, fileNumber, false)
	if file == nil {
		abort("Trying to mark free space in %.8X%s which does not exist", fileNumber, storageFileTypeInfo[fileType].Extension)
	}
	file.Reader.Seek(0, os.SEEK_SET)
	var header storageFileHeader
	header.Unserialize(file.Reader)
	if add {
		header.deadspace += size
	} else {
		header.deadspace = size
	}
	file.Writer.Seek(0, os.SEEK_SET)
	header.Serialize(file.Writer)
}

// calculateIXEntryOffset calculates a start position into the index file where the blockID could be found using the following formula:
// Use only the last 24 bits of a hash, multiply that by 24 (which is the byte size of an IXEntry)  (2^24*24 = 384MiB indexes)
func calculateIXEntryOffset(blockID core.Byte128) uint32 {
	return uint32(storageFileHeaderSize) + ((uint32(blockID[15]) | uint32(blockID[14])<<8 | uint32(blockID[13])<<16) * 24)
}

// findIXOffset probes for a blockID and returns entry+file+offset if found, regardless if it is invalid or not
// if stopOnFree == true, it returns the first possible location where the block could be stored (used for compacting)
func (handler *StorageHandler) findIXOffset(blockID core.Byte128, stopOnFree bool) (*storageIXEntry, int32, int64) {
	var entry storageIXEntry

	baseOffset := int64(calculateIXEntryOffset(blockID))
	ixFileNumber, freeFileNumber := int32(0), int32(0)
	ixOffset, freeOffset := baseOffset, baseOffset
	foundFree := false

OuterLoop:
	for {
		ixFile := handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			core.Log(core.LogTrace, "ran out of index files")
			break
		}

		ixSize := ixFile.Size()
		if ixOffset > ixSize {
			core.Log(core.LogTrace, "%x > %x", ixOffset, ixSize)
			break
		}
		_, err := ixFile.Reader.Seek(ixOffset, os.SEEK_SET)
		abortOn(err)

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
	} else {
		return nil, ixFileNumber, ixOffset
	}
}

func (handler *StorageHandler) findFreeOffset(fileType int) (freeFileNum int32, freeOffset int64, freeFile *core.BufferedFile) {
	if handler.topFileNumber[fileType] < 0 {
		// Fill new data at the end
		handler.topFileNumber[fileType] = 0
		for top := int32(1); ; top++ {
			if topFile := handler.getNumberedFile(fileType, top, false); topFile == nil {
				break
			} else if topFile.Size() > storageFileHeaderSize { // this file contains data
				handler.topFileNumber[fileType] = top
			}
		}
	}

	for {
		freeFile = handler.getNumberedFile(fileType, handler.topFileNumber[fileType], true)
		freeOffset, _ = freeFile.Writer.Seek(0, os.SEEK_END)
		if freeOffset <= storageOffsetLimit {
			break
		}
		handler.topFileNumber[fileType]++
	}
	return handler.topFileNumber[fileType], freeOffset, freeFile
}

func (handler *StorageHandler) readIXEntry(blockID core.Byte128) (entry *storageIXEntry, ixFileNumber int32, ixOffset int64, err error) {
	entry, ixFileNumber, ixOffset = handler.findIXOffset(blockID, false)
	if entry == nil {
		err = errors.New(fmt.Sprintf("Block index entry not found for %x", blockID[:]))
	}
	return
}

func (handler *StorageHandler) writeIXEntry(ixFileNumber int32, ixOffset int64, entry *storageIXEntry, forceFlush bool) {
	var ixFile = handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, true)
	ixFile.Writer.Seek(ixOffset, os.SEEK_SET)
	finalFlags := entry.flags
	entry.flags |= entryFlagInvalid // Write record as invalid first
	entry.Serialize(ixFile.Writer)

	ixFile.Writer.Seek(ixOffset, os.SEEK_SET)
	core.WriteUint16(ixFile.Writer, finalFlags) // Write correct flags
	if forceFlush == true {
		ixFile.Sync()
	}
	core.Log(core.LogTrace, "writeIXEntry %x:%x", ixFileNumber, ixOffset)
}
func (handler *StorageHandler) InvalidateIXEntry(blockID core.Byte128) {
	core.Log(core.LogDebug, "InvalidateIXEntry %x", blockID[:])

	e, f, o, err := handler.readIXEntry(blockID)
	abortOn(err)

	ASSERT(e != nil, e)
	e.flags |= entryFlagInvalid
	// flush notice: no need to force flush index invalidation
	handler.writeIXEntry(f, o, e, false)
}

func (handler *StorageHandler) killMetaEntry(blockID core.Byte128, metaFileNumber int32, metaOffset int64) (size int64) {
	entry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	abortOn(err)
	if entry.blockID.Compare(blockID) == 0 {
		var data = new(bytes.Buffer)
		entry.Serialize(data)
		entrySize := data.Len()
		handler.setDeadSpace(storageFileTypeMeta, metaFileNumber, int64(entrySize), true)
		size += int64(entrySize)

		dataFileNumber, _ := entry.location.Get()
		handler.setDeadSpace(storageFileTypeData, dataFileNumber, int64(entry.dataSize), true)
		size += int64(entry.dataSize)
	} else {
		abort("Incorrect block %x (should be %x) read on metadata location %x:%x", entry.blockID[:], blockID[:], metaFileNumber, metaOffset)
	}
	return size
}
func (handler *StorageHandler) writeMetaEntry(metaFileNumber int32, metaOffset int64, entry *storageMetaEntry) (int32, int64) {
	var data = new(bytes.Buffer)
	entry.Serialize(data)

	var metaFile *core.BufferedFile
	if metaOffset == 0 { // Offset 0 does not exist as it is in the header
		metaFileNumber, metaOffset, metaFile = handler.findFreeOffset(storageFileTypeMeta)
	} else {
		metaFile = handler.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
		metaFile.Writer.Seek(metaOffset, os.SEEK_SET)
	}
	data.WriteTo(metaFile.Writer)
	// flush notice: always force a flush because index will be updated next
	metaFile.Sync()
	core.Log(core.LogTrace, "writeMetaEntry %x:%x", metaFileNumber, metaOffset)

	return metaFileNumber, metaOffset
}
func (handler *StorageHandler) readMetaEntry(metaFileNumber int32, metaOffset int64) (metaEntry *storageMetaEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
		return
	}()

	metaFile := handler.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
	if metaFile == nil {
		return nil, errors.New(fmt.Sprintf("Error reading metadata cache from file %x, file does not exist", metaFileNumber))
	}
	metaFile.Reader.Seek(metaOffset, os.SEEK_SET)

	metaEntry = new(storageMetaEntry)
	metaEntry.Unserialize(metaFile.Reader)
	return
}

func (handler *StorageHandler) writeBlockFile(block *core.HashboxBlock) bool {
	_, ixFileNumber, ixOffset, err := handler.readIXEntry(block.BlockID)
	if err == nil {
		// Block already exists
		return false
	}

	for _, r := range block.Links {
		if r.Compare(block.BlockID) == 0 {
			abort("Invalid self reference in block links")
		}
	}

	datFileNumber, datOffset, datFile := handler.findFreeOffset(storageFileTypeData)

	dataEntry := storageDataEntry{block: block}
	var data = new(bytes.Buffer)
	dataEntry.Serialize(data)
	data.WriteTo(datFile.Writer)
	// flush notice: manually flush datFile before creating the meta entry
	datFile.Sync()

	metaEntry := storageMetaEntry{blockID: block.BlockID, dataSize: uint32(block.Data.Len()), links: block.Links}
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
func (handler *StorageHandler) readBlockFile(blockID core.Byte128) (*core.HashboxBlock, error) {
	indexEntry, _, _, err := handler.readIXEntry(blockID)
	if err != nil {
		return nil, err
	}

	metaFileNumber, metaOffset := indexEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	abortOn(err)

	dataFileNumber, dataOffset := metaEntry.location.Get()
	dataFile := handler.getNumberedFile(storageFileTypeData, dataFileNumber, false)
	if dataFile == nil {
		abort("Error reading block from file %x, file does not exist", dataFileNumber)
	}
	dataFile.Reader.Seek(dataOffset, os.SEEK_SET)

	var dataEntry storageDataEntry
	dataEntry.Unserialize(dataFile.Reader)
	return dataEntry.block, nil
}

func (handler *StorageHandler) MarkIndexes(roots []core.Byte128, Paint bool) {
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
			abortOn(err)

			if entry.flags&entryFlagNoLinks == 0 {
				metaFileNumber, metaOffset := entry.location.Get()
				metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
				abortOn(err)
				chain = append(chain, metaEntry.links...)
			}

			entry.flags |= entryFlagMarked // Mark the entry
			ixFile := handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
			if ixFile == nil {
				abort("Error marking index entry in file %x, offset %x, file does not exist", ixFileNumber, ixOffset)
			}

			ixFile.Writer.Seek(ixOffset, os.SEEK_SET)
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
func (handler *StorageHandler) SweepIndexes(Paint bool) {
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
		abortOn(err)
		_, err = reader.Seek(storageFileHeaderSize, os.SEEK_SET)
		abortOn(err)

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; offset += storageIXEntrySize {
			var entry storageIXEntry
			entry.Unserialize(reader)
			core.Log(core.LogTrace, "Read %x at %x:%x (flags:%d, base:%x)", entry.blockID[:], ixFileNumber, offset, entry.flags, int64(calculateIXEntryOffset(entry.blockID)))

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
						// flush notice: force flushes on moves because next we invalidate the old record
						handler.writeIXEntry(eFileNumber, eOffset, &entry, true) // move it to an earlier file
						entry.flags |= entryFlagInvalid                          // delete old entry
						core.Log(core.LogDebug, "Moved block index %x from %x:%x to %x:%x", entry.blockID[:], ixFileNumber, offset, eFileNumber, eOffset)
					} else if eFileNumber == ixFileNumber && eOffset < offset {
						// flush notice: force flushes on moves because next we invalidate the old record
						handler.writeIXEntry(eFileNumber, eOffset, &entry, true) // move it to an earlier position
						entry.flags |= entryFlagInvalid                          // delete old entry
						core.Log(core.LogDebug, "Moved block index %x from %x:%x to %x", entry.blockID[:], ixFileNumber, offset, eOffset)
					} else {
						abort("findIXOffset for %x (%x:%x) returned an invalid offset %x:%x", entry.blockID[:], ixFileNumber, offset, eFileNumber, eOffset)
					}
				}
				ixFile.Writer.Seek(offset, os.SEEK_SET)
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

func (handler *StorageHandler) CompactIndexes(Paint bool) {
	var blankEntry storageIXEntry
	clearedBlocks := 0

	for ixFileNumber := int32(0); ; ixFileNumber++ {
		var ixFile = handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		if Paint {
			core.Log(core.LogInfo, "Compacting index file #%d (%s)", ixFileNumber, core.HumanSize(ixSize))
		}

		_, err := ixFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)
		abortOn(err)

		truncPoint := int64(storageFileHeaderSize)

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; offset += storageIXEntrySize {
			var entry storageIXEntry
			entry.Unserialize(ixFile.Reader)

			if entry.flags&entryFlagInvalid == entryFlagInvalid {
				clearedBlocks++
				ixFile.Writer.Seek(offset, os.SEEK_SET)
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
			ixFile.Writer.File.Truncate(truncPoint)
		}
	}
	if Paint {
		core.Log(core.LogInfo, "Cleared %d blocks", clearedBlocks)
	}
}

func skipDataGap(reader *core.BufferedReader) int64 {
	offset := int64(0)
	peek, err := reader.Peek(12)
	abortOn(err)
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
func forwardToDataMarker(reader *core.BufferedReader) (int64, error) {
	offset := int64(0)
	for {
		peek, err := reader.Peek(4)
		if err != nil {
			return offset, err
		}
		if uint32(peek[3])|uint32(peek[2])<<8|uint32(peek[1])<<16|uint32(peek[0])<<24 == storageDataMarker {
			return offset, nil
		}
		reader.Discard(1)
		offset++
	}
	if offset > 0 {
		core.Log(core.LogDebug, "Jumped %d bytes to next data marker in %s", offset, reader.File.Name())
	}
	return offset, nil
}

func (handler *StorageHandler) CompactFile(fileType int, fileNumber int32) int64 {
	var entry storageEntry
	switch fileType {
	case storageFileTypeMeta:
		metaEntry := new(storageMetaEntry)
		entry = metaEntry
	case storageFileTypeData:
		dataEntry := new(storageDataEntry)
		defer dataEntry.Release()
		entry = dataEntry
	}

	file := handler.getNumberedFile(fileType, fileNumber, false)
	fileSize, deadSpace, err := handler.getNumberedFileSize(fileType, fileNumber)
	abortOn(err)
	core.Log(core.LogInfo, "Compacting file %s, %s (est. dead data %s)", file.Path, core.HumanSize(fileSize), core.HumanSize(deadSpace))

	// Open a separate reader that is not moved by any other routines
	reader, err := core.OpenBufferedReader(file.Path, 32768, file.Flag)
	abortOn(err)
	_, err = reader.Seek(storageFileHeaderSize, os.SEEK_SET)
	abortOn(err)

	var lastProgress = -1
	removed, moved := int64(0), int64(0)
	offset, writeOffset := int64(storageFileHeaderSize), int64(storageFileHeaderSize)
	for offset < fileSize {
		// Check if there is a Cgap marker here and in that case, jump ahead of it
		skip := skipDataGap(reader)
		removed += skip
		offset += skip

		readOffset := offset
		entrySize := entry.Unserialize(reader)
		offset += int64(entrySize)
		entryBlockID := entry.BlockID()

		core.Log(core.LogTrace, "Read %x:%x block %x (%d bytes)", fileNumber, readOffset, entryBlockID[:], entrySize)

		if _, _, _, err = handler.readIXEntry(entryBlockID); err != nil {
			core.Log(core.LogDebug, "Removed %x:%x block %x (%s)", fileNumber, readOffset, entryBlockID[:], err.Error())
			removed += int64(entrySize)
		} else if !entry.VerifyLocation(handler, fileNumber, readOffset) {
			core.Log(core.LogDebug, "Removed %x:%x block %x (obsolete entry)", fileNumber, readOffset, entryBlockID[:])
			removed += int64(entrySize)
		} else {
			// Keep the block
			freeFileNum, freeOffset, freeFile := handler.findFreeOffset(fileType)
			if freeFileNum >= fileNumber && readOffset == writeOffset { // No space in earlier file let it be if possible
				writeOffset += int64(entrySize)
				core.Log(core.LogTrace, "No need to write, moving writeOffset to %x", writeOffset)
			} else if freeFileNum >= fileNumber && readOffset-writeOffset >= int64(entrySize)+12 { // No space in earlier file, but it can be shifted inside the same file
				newOffset := writeOffset
				file.Writer.Seek(writeOffset, os.SEEK_SET)
				written := int64(entry.Serialize(file.Writer))
				writeOffset += written
				core.Log(core.LogDebug, "Moved block %x (%d bytes) from %x:%x to %x:%x", entryBlockID[:], written, fileNumber, readOffset, fileNumber, newOffset)

				core.Log(core.LogTrace, "Creating a free space marker (%d bytes skip) at %x:%x", readOffset-writeOffset, fileNumber, writeOffset)
				core.WriteBytes(file.Writer, []byte("Cgap"))
				core.WriteInt64(file.Writer, readOffset-writeOffset)
				// flush notice: force flush before changing location
				file.Sync()
				entry.ChangeLocation(handler, fileNumber, newOffset)
			} else if free, _ := core.FreeSpace(datDirectory); free < int64(entrySize)+MINIMUM_DAT_FREE {
				core.Log(core.LogWarning, "Unable to move block %x (%d bytes) because there is not enough free space on data path", entryBlockID[:], entrySize)
				writeOffset = offset // make sure we point the writer pointer after this block so we do not overwrite it
			} else { // found space in a different file, move the block
				written := int64(entry.Serialize(freeFile.Writer))
				core.Log(core.LogDebug, "Moved block %x (%d bytes) from %x:%x to %x:%x", entryBlockID[:], written, fileNumber, readOffset, freeFileNum, freeOffset)
				moved += int64(entrySize)

				if freeFileNum == fileNumber {
					core.Log(core.LogTrace, "File %s, increased in size from %x to %x", freeFile.Path, fileSize, fileSize+written)
					fileSize += written
				}
				// flush notice: force flush before changing location
				freeFile.Sync()
				entry.ChangeLocation(handler, freeFileNum, freeOffset)
			}
		}

		p := int(offset * 100 / fileSize)
		if p > lastProgress {
			lastProgress = p
			fmt.Printf("%d (%d%%)\r", 0-removed, lastProgress)
		}
	}
	ASSERT(writeOffset <= offset, "compact made the file larger?")

	file.Writer.File.Truncate(writeOffset)
	handler.setDeadSpace(fileType, fileNumber, 0, false)

	core.Log(core.LogInfo, "Removed %s (%s moved) from file %s", core.HumanSize(offset-writeOffset), core.HumanSize(moved), file.Path)
	return removed
}
func (handler *StorageHandler) CompactAll(fileType int, threshold int) {
	handler.topFileNumber[fileType] = 0 // ignore data age-location, start at file 0
	var compacted int64
	for fileNumber := int32(0); ; fileNumber++ {
		file := handler.getNumberedFile(fileType, fileNumber, false)
		if file == nil {
			break // no more data
		}
		fileSize, deadSpace, err := handler.getNumberedFileSize(fileType, fileNumber)
		abortOn(err)
		if fileSize < storageOffsetLimit/100 || int(deadSpace*100/fileSize) >= threshold {
			compacted += handler.CompactFile(fileType, fileNumber)
			if handler.topFileNumber[fileType] > fileNumber {
				handler.topFileNumber[fileType] = fileNumber // file might have free space now
			}
		} else {
			core.Log(core.LogInfo, "Skipping compact on file %s, est. dead data %s is less than %d%%", file.Path, core.HumanSize(deadSpace), threshold)
		}
		if err != nil {
			break // no more data files
		}

	}
	core.Log(core.LogInfo, "All %s files compacted, %s released", storageFileTypeInfo[fileType].Extension, core.HumanSize(compacted))
}

func (handler *StorageHandler) checkBlockFromIXEntry(ixEntry *storageIXEntry, verifiedBlocks map[core.Byte128]bool, fullVerify bool, readOnly bool) error {
	err := (func() error {
		metaFileNumber, metaOffset := ixEntry.location.Get()
		metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
		if err != nil {
			return err
		}

		dataFileNumber, dataOffset := metaEntry.location.Get()
		dataFile := handler.getNumberedFile(storageFileTypeData, dataFileNumber, false)
		if dataFile == nil {
			return errors.New(fmt.Sprintf("Error reading block from file %x, file does not exist", dataFileNumber))
		}

		var dataEntry storageDataEntry
		defer dataEntry.Release()

		core.Log(core.LogTrace, "Read %x:%x block %x", dataFileNumber, dataOffset, ixEntry.blockID[:])
		dataFile.Reader.Seek(dataOffset, os.SEEK_SET)
		if fullVerify {
			dataEntry.Unserialize(dataFile.Reader)
			dataEntry.block.UncompressData()
			if !dataEntry.block.VerifyBlock() {
				return errors.New(fmt.Sprintf("Error reading block %x, content verification failed"))
			}
			core.Log(core.LogTrace, "Block %x location %x:%x and content verified (%s, %.0f%% compr)", ixEntry.blockID[:], dataFileNumber, dataOffset, core.HumanSize(int64(dataEntry.block.CompressedSize)), float64(100.0*float64(dataEntry.block.UncompressedSize-dataEntry.block.CompressedSize)/float64(dataEntry.block.UncompressedSize)))
		} else {
			dataEntry.UnserializeHeader(dataFile.Reader)
			if dataEntry.block == nil {
				dataEntry.block = &core.HashboxBlock{}
			}
			dataEntry.block.UnserializeHeader(dataFile.Reader)
			if dataEntry.block.BlockID.Compare(ixEntry.blockID) != 0 {
				return errors.New(fmt.Sprintf("Error reading block %x, metadata cache is pointing to block %x", ixEntry.blockID[:], dataEntry.block.BlockID[:]))
			}
			core.Log(core.LogTrace, "Block %x location %x:%x verified", ixEntry.blockID[:], dataFileNumber, dataOffset)
		}

		if len(metaEntry.links) > 0 && ixEntry.flags&entryFlagNoLinks == entryFlagNoLinks {
			return errors.New(fmt.Sprintf("Error reading block %x, index is marked having no links but the metadata cache has %d links", ixEntry.blockID[:], len(metaEntry.links)))
		}
		if len(metaEntry.links) == 0 && ixEntry.flags&entryFlagNoLinks == 0 {
			return errors.New(fmt.Sprintf("Error reading block %x, index is marked having links but the metadata cache has 0 links", ixEntry.blockID[:]))
		}
		if len(metaEntry.links) != len(dataEntry.block.Links) {
			return errors.New(fmt.Sprintf("Error reading block %x, metadata cache links mismatch", ixEntry.blockID[:]))
		}
		for i := range metaEntry.links {
			if metaEntry.links[i].Compare(dataEntry.block.Links[i]) != 0 {
				return errors.New(fmt.Sprintf("Error reading block %x, metadata cache links mismatch", ixEntry.blockID[:]))
			}
		}

		for _, r := range metaEntry.links {
			v, checked := verifiedBlocks[r]
			if !checked {
				rIX, _, _, err := handler.readIXEntry(r)
				if err != nil {
					return errors.New(fmt.Sprintf("Error in block %x, link %x does not exist", ixEntry.blockID[:], r[:]))
				}
				if rIX.flags&entryFlagInvalid == entryFlagInvalid {
					return errors.New(fmt.Sprintf("Error in block %x, link %x is invalid", ixEntry.blockID[:], r[:]))
				}
				if err := handler.checkBlockFromIXEntry(rIX, verifiedBlocks, fullVerify, readOnly); err != nil {
					return err
				}
			} else if !v {
				return errors.New(fmt.Sprintf("Error in block %x, link %x is invalid", ixEntry.blockID[:], r[:]))
			}
		}
		return nil
	})()
	if err == nil {
		verifiedBlocks[ixEntry.blockID] = true
	} else {
		verifiedBlocks[ixEntry.blockID] = false
		if readOnly {
			abort("%V", err)
		} else {
			core.Log(core.LogDebug, "%v", err)
			handler.InvalidateIXEntry(ixEntry.blockID)
		}
	}
	return err
}

func (handler *StorageHandler) CheckBlockTree(blockID core.Byte128, verifiedBlocks map[core.Byte128]bool, fullVerify bool, readOnly bool) error {
	ixEntry, _, _, err := handler.readIXEntry(blockID)
	if err != nil {
		return err
	}
	return handler.checkBlockFromIXEntry(ixEntry, verifiedBlocks, fullVerify, readOnly)
}

func (handler *StorageHandler) CheckIndexes(verifiedBlocks map[core.Byte128]bool, fullVerify bool, readOnly bool) {
	var ixEntry storageIXEntry

	for ixFileNumber := int32(0); ; ixFileNumber++ {
		ixFile := handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		core.Log(core.LogInfo, "Checking index file #%d (%s)", ixFileNumber, core.HumanSize(ixSize))
		if ixSize%storageIXEntrySize != storageFileHeaderSize {
			abort("Index file %x size is not evenly divisable by the index entry size, file must be damaged", ixFileNumber)
		}

		reader, err := core.OpenBufferedReader(ixFile.Path, 32768, ixFile.Flag)
		abortOn(err)
		_, err = reader.Seek(storageFileHeaderSize, os.SEEK_SET)
		abortOn(err)

		lastProgress := -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; offset += storageIXEntrySize {
			n := int64(ixEntry.Unserialize(reader))
			ASSERT(n == storageIXEntrySize, n) // ixEntry unserialize broken ?

			if ixEntry.flags&entryFlagInvalid == entryFlagInvalid {
				core.Log(core.LogDebug, "Skipping invalid index entry for %x found at %x:%x", ixEntry.blockID[:], ixFileNumber, offset)
			} else if ixEntry.flags&entryFlagExists == entryFlagExists { // In use
				o := int64(calculateIXEntryOffset(ixEntry.blockID))
				if offset < o {
					abort("Block %x found on an invalid offset %x, it should be >= %x", ixEntry.blockID[:], offset, o)
				}

				v, checked := verifiedBlocks[ixEntry.blockID]
				if !checked {
					if err := handler.checkBlockFromIXEntry(&ixEntry, verifiedBlocks, fullVerify, readOnly); err != nil {
						core.Log(core.LogWarning, "Block tree for %x was marked invalid: %v", ixEntry.blockID[:], err)
					}
				} else {
					ASSERT(v == true) // If this block is not valid, it should already have entryFlagInvalid in the check above
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

func (handler *StorageHandler) RemoveFiles(fileType int) {
	for fileNumber := int32(0); ; fileNumber++ {
		filename := handler.getNumberedFileName(fileType, fileNumber)
		if err := os.Rename(filename, filename+".bak"); err != nil {
			return
		}
	}
}
func (handler *StorageHandler) CheckStorageFiles() (errorCount int) {
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
				abortOn(err)
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

func (handler *StorageHandler) RecoverData(startfile int32, endfile int32) (repairCount int) {
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
		datFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)

		var lastProgress = -1
		brokenSpot := int64(0)
		for offset := int64(storageFileHeaderSize); offset < datSize; {
			// Check if there is a Cgap marker here and in that case, jump ahead of it
			offset += skipDataGap(datFile.Reader)

			blockOffset := offset

			skipToNextBlock := false
			if err := Try(func() { offset += int64(dataEntry.Unserialize(datFile.Reader)) }); err != nil {
				err := errors.New(fmt.Sprintf("Error reading dataEntry at %x:%x (%s)", datFileNumber, blockOffset, err))
				core.Log(core.LogError, "%v", err)
				skipToNextBlock = true
			} else if err := Try(func() {
				if !dataEntry.block.VerifyBlock() {
					panic(errors.New("Content verification failed"))
				}
			}); err != nil {
				err := errors.New(fmt.Sprintf("Error verifying block %x (type %d, size %d) at %x:%x (%s)", dataEntry.block.BlockID, dataEntry.block.DataType, dataEntry.block.Data.Len(), datFileNumber, blockOffset, err))
				core.Log(core.LogError, "%v", err)
				skipToNextBlock = true
			}
			if skipToNextBlock {
				if brokenSpot == 0 {
					brokenSpot = blockOffset
				}
				offset = blockOffset + 1
				datFile.Reader.Seek(offset, os.SEEK_SET)
				if o, err := forwardToDataMarker(datFile.Reader); err == nil {
					offset += o
					core.Log(core.LogInfo, "Skipped forward to next block at %x:%x", datFileNumber, offset)
					continue
				} else {
					core.Log(core.LogInfo, "Skipped forward until %v", err)
					break
				}
			}
			if blockOffset > storageOffsetLimit {
				core.Log(core.LogError, "Offset %x for block %x is beyond offset limit, forcing a move", blockOffset, dataEntry.block.BlockID[:])
				brokenSpot = blockOffset
			}
			if brokenSpot > 0 && blockOffset-brokenSpot < 12 {
				// Cannot fit a Cgap marker, so we need to move the block
				moveFileNum, moveOffset, moveFile := handler.findFreeOffset(storageFileTypeData)
				core.Log(core.LogDebug, "Rewriting block %x at (%x:%x)", dataEntry.block.BlockID[:], moveFileNum, moveOffset)
				dataEntry.Serialize(moveFile.Writer)

				core.Log(core.LogTrace, "Creating new meta for block %x", dataEntry.block.BlockID[:])
				metaEntry := storageMetaEntry{blockID: dataEntry.block.BlockID, dataSize: uint32(dataEntry.block.Data.Len()), links: dataEntry.block.Links}
				metaEntry.location.Set(moveFileNum, moveOffset)
				// flush notice: force flush before writing meta entry
				moveFile.Sync()
				metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

				core.Log(core.LogTrace, "Creating new index for block %x", dataEntry.block.BlockID[:])
				ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(dataEntry.block.BlockID)
				if err != nil {
					ixEntry = &storageIXEntry{flags: entryFlagExists, blockID: dataEntry.block.BlockID}
				}
				ixEntry.location.Set(metaFileNumber, metaOffset)
				// flush notice: no need to force flushes during recovery
				handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry, false)
				continue
			} else if brokenSpot > 0 {
				datFile.Writer.Seek(brokenSpot, os.SEEK_SET)
				core.Log(core.LogTrace, "Creating a free space marker at %x:%x (skip %d bytes)", datFileNumber, brokenSpot, blockOffset-brokenSpot)
				core.WriteBytes(datFile.Writer, []byte("Cgap"))
				core.WriteInt64(datFile.Writer, blockOffset-brokenSpot)
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
					if int(metaEntry.dataSize) != dataEntry.block.Data.Len() {
						core.Log(core.LogWarning, "Metadata cache size error for block %x (%x != %x)", dataEntry.block.BlockID[:], metaEntry.dataSize, dataEntry.block.Data.Len())
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
				metaEntry := storageMetaEntry{blockID: dataEntry.block.BlockID, dataSize: uint32(dataEntry.block.Data.Len()), links: dataEntry.block.Links}
				metaEntry.location.Set(datFileNumber, blockOffset)
				metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

				core.Log(core.LogTrace, "REPAIRING index for block %x", dataEntry.block.BlockID[:])
				ixEntry.location.Set(metaFileNumber, metaOffset)
				// flush notice: no need to force flushes during recovery
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
		// reset the amount of dead space as everything has been added back when repairing
		handler.setDeadSpace(storageFileTypeData, datFileNumber, 0, false)
	}
	return
}

func (handler *StorageHandler) ShowStorageDeadSpace() {
	for datFileNumber := int32(0); ; datFileNumber++ {
		var datFile = handler.getNumberedFile(storageFileTypeData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}
		fileSize, deadSpace, err := handler.getNumberedFileSize(storageFileTypeData, datFileNumber)
		abortOn(err)
		core.Log(core.LogInfo, "File %s, %s (est. dead data %s)", datFile.Path, core.HumanSize(fileSize), core.HumanSize(deadSpace))
	}
}
