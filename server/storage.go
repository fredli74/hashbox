//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2016
//	+---+´

package main

import (
	"github.com/fredli74/hashbox/core"

	"bufio"
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

	filepool          map[string]*core.BufferedFile
	topDatFileNumber  int32
	topMetaFileNumber int32
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
		core.Log(core.LogInfo, "Closing %s", s)
		f.Close()
	}
}
func NewStorageHandler() *StorageHandler {
	handler := &StorageHandler{
		queue:            make(chan ChannelCommand, 32),
		signal:           make(chan error), // cannot be buffered
		filepool:         make(map[string]*core.BufferedFile),
		topDatFileNumber: -1,
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
const MINIMUM_DAT_FREE = int64(2 ^ 26)                                  // 64 MB minimum free space
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
	entryFlagDefunct             // entry is defunct
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
		panic(errors.New("Invalid version in dbFileHeader"))
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

const storageIXEntrySize int64 = 24                                                 // 24 bytes
const storageIXEntryProbeLimit int = 682                                            // 682*24 bytes = 16368 < 16k
const storageIXFileSize = storageIXEntrySize * int64(2^24+storageIXEntryProbeLimit) // last 24 bits of hash, plus max probe = 24*(2^24+682) = 384MiB indexes

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
		panic(errors.New(fmt.Sprintf("Incorrect metadata cache marker %x (should be %x)", e.datamarker, storageDataMarker)))
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
	PanicOn(err)
	ixEntry.location.Set(fileNumber, fileOffset)
	handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry)
}
func (e *storageMetaEntry) VerifyLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := handler.readIXEntry(e.blockID)
	PanicOn(err)
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

// IMPORTANT Unserialize allocates memory that needs to be freed manually
func (e *storageDataEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadUint32(r, &e.datamarker)
	if e.datamarker != storageDataMarker {
		panic(errors.New(fmt.Sprintf("Incorrect datamarker %x (should be %x)", e.datamarker, storageDataMarker)))
	}
	if e.block == nil {
		e.block = &core.HashboxBlock{}
	}
	size += e.block.Unserialize(r)
	return
}
func (e *storageDataEntry) ChangeLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) {
	ixEntry, _, _, err := handler.readIXEntry(e.block.BlockID)
	PanicOn(err)

	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	PanicOn(err)

	metaEntry.location.Set(fileNumber, fileOffset)
	handler.writeMetaEntry(metaFileNumber, metaOffset, metaEntry)
}
func (e *storageDataEntry) VerifyLocation(handler *StorageHandler, fileNumber int32, fileOffset int64) bool {
	ixEntry, _, _, err := handler.readIXEntry(e.block.BlockID)
	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	PanicOn(err)

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
				panic(err)
			} else {
				return nil
			}
		}
		var header storageFileHeader
		if f.Size() == 0 { // New file, write a header
			header.filetype = storageFileTypeInfo[fileType].Type
			header.version = storageVersion
			header.Serialize(f.Writer)
			f.Writer.Flush()
		} else {
			header.Unserialize(f.Reader)
			if header.filetype != storageFileTypeInfo[fileType].Type {
				panic(errors.New(fmt.Sprintf("Trying to read storage file %s with the wrong file type header: %x (was expecting %x)", filename, header.filetype, storageFileTypeInfo[fileType].Type)))
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
		panic(errors.New(fmt.Sprintf("Trying to mark free space in %.8X%s which does not exist", fileNumber, storageFileTypeInfo[fileType].Extension)))
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
	file.Writer.Flush()
}

// calculateIXEntryOffset calculates a start position into the index file where the blockID could be found using the following formula:
// Use only the last 24 bits of a hash, multiply that by 24 (which is the byte size of an IXEntry)  (2^24*24 = 384MiB indexes)
func calculateIXEntryOffset(blockID core.Byte128) uint32 {
	return uint32(storageFileHeaderSize) + ((uint32(blockID[15]) | uint32(blockID[14])<<8 | uint32(blockID[13])<<16) * 24)
}

func (handler *StorageHandler) readIXEntry(blockID core.Byte128) (*storageIXEntry, int32, int64, error) {
	baseOffset := int64(calculateIXEntryOffset(blockID))
	ixFileNumber := int32(0)
	ixOffset := baseOffset

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
		PanicOn(err)

		var entry storageIXEntry
		for i := 0; i < storageIXEntryProbeLimit; i++ {
			if ixOffset >= ixSize {
				break OuterLoop
			}
			entry.Unserialize(ixFile.Reader)

			if entry.flags&entryFlagExists == 0 {
				break OuterLoop
			}
			if entry.flags&entryFlagDefunct == 0 && bytes.Equal(blockID[:], entry.blockID[:]) {
				return &entry, ixFileNumber, ixOffset, nil
			}
			ixOffset += storageIXEntrySize
		}
		ixFileNumber++
		ixOffset = baseOffset
	}
	return nil, ixFileNumber, ixOffset, errors.New(fmt.Sprintf("BlockID index entry not found for %x", blockID[:]))
}
func (handler *StorageHandler) writeIXEntry(ixFileNumber int32, ixOffset int64, entry *storageIXEntry) {
	var ixFile = handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, true)
	ixFile.Writer.Seek(ixOffset, os.SEEK_SET)
	finalFlags := entry.flags
	entry.flags |= entryFlagDefunct // Write record as defunct first
	entry.Serialize(ixFile.Writer)
	ixFile.Writer.Flush()

	ixFile.Writer.Seek(ixOffset, os.SEEK_SET)
	core.WriteUint16(ixFile.Writer, finalFlags) // Write correct flags
	ixFile.Writer.Flush()
}
func (handler *StorageHandler) killMetaEntry(blockID core.Byte128, metaFileNumber int32, metaOffset int64) (size int64) {
	entry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
	PanicOn(err)
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
		panic(errors.New(fmt.Sprintf("Incorrect block %x (should be %x) read on metadata location %x:%x", entry.blockID[:], blockID[:], metaFileNumber, metaOffset)))
	}
	return size
}
func (handler *StorageHandler) writeMetaEntry(metaFileNumber int32, metaOffset int64, entry *storageMetaEntry) (int32, int64) {
	var data = new(bytes.Buffer)
	entry.Serialize(data)

	var metaFile *core.BufferedFile
	if metaOffset == 0 { // Offset 0 does not exist as it is in the header
		for {
			metaFile = handler.getNumberedFile(storageFileTypeMeta, handler.topMetaFileNumber, true)
			metaOffset, _ = metaFile.Writer.Seek(0, os.SEEK_END)
			core.Log(core.LogTrace, "writeMetaEntry %x:%x", handler.topMetaFileNumber, metaOffset)
			if metaOffset <= storageOffsetLimit {
				break
			}
			handler.topMetaFileNumber++
		}
		metaFileNumber = handler.topMetaFileNumber
	} else {
		metaFile = handler.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
		metaFile.Writer.Seek(metaOffset, os.SEEK_SET)
	}
	data.WriteTo(metaFile.Writer)
	metaFile.Writer.Flush()

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

	// With new age shuffle compression, always fill new data at the end
	if handler.topDatFileNumber < 0 {
		for {
			if topFile := handler.getNumberedFile(storageFileTypeData, handler.topDatFileNumber+1, false); topFile != nil {
				handler.topDatFileNumber++
			} else {
				break
			}
		}
	}

	var datFile *core.BufferedFile
	var datOffset int64
	for {
		datFile = handler.getNumberedFile(storageFileTypeData, handler.topDatFileNumber, true)
		datOffset, _ = datFile.Writer.Seek(0, os.SEEK_END)
		if datOffset <= storageOffsetLimit {
			break
		}
		handler.topDatFileNumber++
	}

	dataEntry := storageDataEntry{block: block}
	var data = new(bytes.Buffer)
	dataEntry.Serialize(data)
	data.WriteTo(datFile.Writer)
	datFile.Writer.Flush()

	metaEntry := storageMetaEntry{blockID: block.BlockID, dataSize: uint32(block.Data.Len()), links: block.Links}
	metaEntry.location.Set(handler.topDatFileNumber, datOffset)
	metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

	ixEntry := storageIXEntry{flags: entryFlagExists, blockID: block.BlockID}
	if len(block.Links) == 0 {
		ixEntry.flags |= entryFlagNoLinks
	}
	ixEntry.location.Set(metaFileNumber, metaOffset)
	handler.writeIXEntry(ixFileNumber, ixOffset, &ixEntry)
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
	PanicOn(err)

	dataFileNumber, dataOffset := metaEntry.location.Get()
	dataFile := handler.getNumberedFile(storageFileTypeData, dataFileNumber, false)
	if dataFile == nil {
		panic(errors.New(fmt.Sprintf("Error reading block from file %x, file does not exist", dataFileNumber)))
	}
	dataFile.Reader.Seek(dataOffset, os.SEEK_SET)

	var dataEntry storageDataEntry
	dataEntry.Unserialize(dataFile.Reader)
	return dataEntry.block, nil
}

func (handler *StorageHandler) CheckChain(blockID core.Byte128, tag string, verified map[core.Byte128]bool) (critical int) {
	if !verified[blockID] {
		entry, _, _, err := handler.readIXEntry(blockID)
		if err != nil {
			critical++
			core.Log(core.LogError, "Chain %s error %v", tag, err)
			return critical
		} else if entry.flags&entryFlagNoLinks == 0 {
			metaFileNumber, metaOffset := entry.location.Get()
			metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
			PanicOn(err)
			if len(metaEntry.links) > 0 && entry.flags&entryFlagNoLinks == entryFlagNoLinks {
				//panic(errors.New(fmt.Sprintf("Index entry for block %x is marked with NoLinks but the block has %d links", blockID[:], len(metaEntry.links))))
			} else if len(metaEntry.links) == 0 && entry.flags&entryFlagNoLinks == 0 {
				core.Log(core.LogDebug, "* [%s] Block %x has no links but the index is missing NoLinks flag", tag, blockID[:])
			}
			for _, r := range metaEntry.links {
				c := handler.CheckChain(r, tag, verified)
				critical += c
			}
		}
		if critical == 0 {
			verified[blockID] = true
		} else {
			core.Log(core.LogDebug, "! [%s] Block chain for %x contains an error", tag, blockID)
		}
	}
	return critical
}

func (handler *StorageHandler) MarkIndexes(roots []core.Byte128, Paint bool, doIgnore bool) {
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
			if err != nil {
				if doIgnore {
					core.Log(core.LogError, "%v", err)
				} else {
					panic(err)
				}
			} else {
				if entry.flags&entryFlagNoLinks == 0 {
					metaFileNumber, metaOffset := entry.location.Get()
					metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
					PanicOn(err)
					chain = append(chain, metaEntry.links...)
				}

				entry.flags |= entryFlagMarked // Mark the entry
				{
					var ixFile = handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
					if ixFile == nil {
						panic(errors.New(fmt.Sprintf("Error marking index entry in file %x, offset %x, file does not exist", ixFileNumber, ixOffset)))
					}

					ixFile.Writer.Seek(ixOffset, os.SEEK_SET)
					core.WriteUint16(ixFile.Writer, entry.flags)
					ixFile.Writer.Flush()
				}
			}
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
	var blankEntry storageIXEntry
	var blanks []int64
	var sweepedSize int64

	deletedBlocks := 0
	for ixFileNumber := int32(0); ; ixFileNumber++ {
		var lastActiveEntry int64 = -1

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
		PanicOn(err)
		_, err = reader.Seek(storageFileHeaderSize, os.SEEK_SET)
		PanicOn(err)

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; offset += storageIXEntrySize {
			var entry storageIXEntry
			entry.Unserialize(reader)
			core.Log(core.LogTrace, "Read %x at %x:%x (flags:%x)", entry.blockID[:], ixFileNumber, offset, entry.flags)

			if entry.flags&entryFlagExists == entryFlagExists {
				if entry.flags&entryFlagDefunct == entryFlagDefunct {
					blanks = append(blanks, offset)
					core.Log(core.LogDebug, "Deleted defunct index at %x:%x", ixFileNumber, offset)
				} else if entry.flags&entryFlagMarked == 0 {
					// Delete it! Well we need to compact or we will break linear probing
					deletedBlocks++
					metaFileNumber, metaOffset := entry.location.Get()
					sweepedSize += handler.killMetaEntry(entry.blockID, metaFileNumber, metaOffset)

					blanks = append(blanks, offset)
					core.Log(core.LogDebug, "Deleted orphan Block %x IX from %x:%x", entry.blockID[:], ixFileNumber, offset)
				} else {
					entry.flags &^= entryFlagMarked

					// Move it down to a lower idx file?
					e, f, o, err := handler.readIXEntry(entry.blockID)
					if err == nil && e != nil && (f < ixFileNumber || (f == ixFileNumber && o < offset)) {
						core.Log(core.LogDebug, "Found an obsolete index for Block %x at %x:%x, the correct index already exists at %x:%x", entry.blockID[:], ixFileNumber, offset, f, o)
						deletedBlocks++
						blanks = append(blanks, offset)
					} else if f < ixFileNumber {
						core.Log(core.LogDebug, "Moving file for Block %x IX from %x:%x to %x:%x", entry.blockID[:], ixFileNumber, offset, f, o)
						if e != nil || err == nil {
							panic("ASSERT, this makes no sense, the block is in a lower idx already?")
						}
						handler.writeIXEntry(f, o, &entry)
						blanks = append(blanks, offset)
					} else {
						// Move it inside same idx file
						newOffset := offset
						blockbase := int64(calculateIXEntryOffset(entry.blockID))

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

						ixFile.Writer.Seek(newOffset, os.SEEK_SET)
						if offset == newOffset { // no need to rewrite whole record
							core.WriteUint16(ixFile.Writer, entry.flags)
						} else {
							core.Log(core.LogDebug, "Moving Block %x IX from %x:%x to %x:%x", entry.blockID[:], ixFileNumber, offset, ixFileNumber, newOffset)
							finalFlags := entry.flags
							entry.flags |= entryFlagDefunct // Write record as defunct first
							entry.Serialize(ixFile.Writer)
							ixFile.Writer.Flush()
							ixFile.Writer.Seek(newOffset, os.SEEK_SET)
							core.WriteUint16(ixFile.Writer, finalFlags)
						}
						ixFile.Writer.Flush()
					}
				}
			} else {
				core.Log(core.LogTrace, "Empty spot at  %x:%x (blanks = %d)", ixFileNumber, offset, len(blanks))
			}

			for len(blanks) > 0 && // while there are blank spaces
				(offset+storageIXEntrySize >= ixSize || // and we are at the end of the file
					blanks[0] < offset-storageIXEntrySize*int64(storageIXEntryProbeLimit)) { // or the first blank is out of probe limit range
				core.Log(core.LogDebug, "Clearing %x:%x", ixFileNumber, blanks[0])
				ixFile.Writer.Seek(blanks[0], os.SEEK_SET)
				blankEntry.Serialize(ixFile.Writer)
				ixFile.Writer.Flush()
				blanks = blanks[1:]
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
			lastActiveEntry += storageIXEntrySize
		}
		if lastActiveEntry < ixSize {
			core.Log(core.LogDebug, "Truncating file %x at %x", ixFileNumber, lastActiveEntry)
			ixFile.Writer.File.Truncate(lastActiveEntry)
		}
	}
	if Paint {
		core.Log(core.LogInfo, "Removed %d blocks (referencing %s data)", deletedBlocks, core.ShortHumanSize(sweepedSize))
	}
}

func SkipDataGap(reader *core.BufferedReader) int64 {
	offset := int64(0)
	peek, err := reader.Peek(12)
	PanicOn(err)
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
func ForwardToDataMarker(reader *core.BufferedReader) (int64, error) {
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

func (handler *StorageHandler) CompactFile(fileType int, fileNumber int32, lowestMove int) (compacted int64) {
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
	PanicOn(err)
	core.Log(core.LogInfo, "Compacting file %s, %s (est. dead data %s)", file.Path, core.HumanSize(fileSize), core.HumanSize(deadSpace))

	// Open a separate reader that is not moved by any other routines
	reader, err := core.OpenBufferedReader(file.Path, 32768, file.Flag)
	PanicOn(err)
	_, err = reader.Seek(storageFileHeaderSize, os.SEEK_SET)
	PanicOn(err)

	var lastProgress = -1
	offset, writeOffset := int64(storageFileHeaderSize), int64(storageFileHeaderSize)
	for offset < fileSize {
		// Check if there is a Cgap marker here and in that case, jump ahead of it
		skip := SkipDataGap(reader)
		compacted += skip
		offset += skip

		readOffset := offset
		entrySize := entry.Unserialize(reader)
		offset += int64(entrySize)
		entryBlockID := entry.BlockID()

		core.Log(core.LogTrace, "Read %x:%x block %x (%d bytes)", fileNumber, readOffset, entryBlockID[:], entrySize)

		if _, _, _, err = handler.readIXEntry(entryBlockID); err != nil {
			core.Log(core.LogDebug, "* Removed %x:%x block %x (%s)", fileNumber, readOffset, entryBlockID[:], err.Error())
			compacted += int64(entrySize)
		} else if !entry.VerifyLocation(handler, fileNumber, readOffset) {
			core.Log(core.LogDebug, "* Removed %x:%x block %x (obsolete entry)", fileNumber, readOffset, entryBlockID[:])
			compacted += int64(entrySize)
		} else {
			// Keep the block
			freeFileNum, freeOffset, freeFile := handler.FindFreeOffset(fileType, lowestMove)
			if freeFileNum >= fileNumber && readOffset == writeOffset { // No space in earlier file let it be if possible
				writeOffset += int64(entrySize)
				core.Log(core.LogTrace, "No need to write, moving writeOffset to %x", writeOffset)
			} else if freeFileNum >= fileNumber && readOffset-writeOffset >= int64(entrySize)+12 { // No space in earlier file, but it can be shifted inside the same file
				file.Writer.Seek(writeOffset, os.SEEK_SET)
				written := int64(entry.Serialize(file.Writer))
				core.Log(core.LogDebug, "Moved block %x (%d bytes) from %x:%x to %x:%x", entryBlockID[:], written, fileNumber, readOffset, fileNumber, writeOffset)
				writeOffset += written

				core.Log(core.LogTrace, "Creating a free space marker (%d bytes skip) at %x:%x", readOffset-writeOffset, fileNumber, readOffset)

				core.WriteBytes(file.Writer, []byte("Cgap"))
				core.WriteInt64(file.Writer, readOffset-writeOffset)
				file.Writer.Flush()

				entry.ChangeLocation(handler, fileNumber, writeOffset)
			} else if free, _ := core.FreeSpace(datDirectory); free < int64(entrySize)*2 {
				core.Log(core.LogWarning, "Unable to move block %x (%d bytes) because there is not enough free space on data path", entryBlockID[:], entrySize)
				writeOffset += int64(entrySize)
			} else { // found space in a different file, move the block
				entry.Serialize(freeFile.Writer)
				written := int64(entry.Serialize(freeFile.Writer))
				core.Log(core.LogDebug, "Moved block %x (%d bytes) from %x:%x to %x:%x", entryBlockID[:], written, fileNumber, readOffset, freeFileNum, freeOffset)
				freeFile.Writer.Flush()

				if freeFileNum == fileNumber {
					core.Log(core.LogTrace, "File %s, increased in size from %x to %x", freeFile.Path, fileSize, fileSize+written)
					fileSize += written
				}

				entry.ChangeLocation(handler, freeFileNum, freeOffset)
			}
		}

		p := int(offset * 100 / fileSize)
		if p > lastProgress {
			fmt.Printf("%d (%d%%)\r", 0-compacted, p)
		}
	}
	if writeOffset > offset {
		panic(" ASSERT !  compact made the file larger?")
	}

	file.Writer.File.Truncate(writeOffset)
	handler.setDeadSpace(fileType, fileNumber, 0, false)

	core.Log(core.LogInfo, "Removed %s from file %s", core.HumanSize(offset-writeOffset), file.Path)
	return compacted
}
func (handler *StorageHandler) CompactAll(fileType int, threshold int) {
	var lowestMove int
	var compacted int64
	for fileNumber := int32(0); ; fileNumber++ {
		file := handler.getNumberedFile(fileType, fileNumber, false)
		if file == nil {
			break // no more data
		}
		fileSize, deadSpace, err := handler.getNumberedFileSize(fileType, fileNumber)
		PanicOn(err)
		if int(deadSpace*100/fileSize) >= threshold {
			compacted += handler.CompactFile(fileType, fileNumber, lowestMove)
		} else {
			core.Log(core.LogInfo, "Skipping compact on file %s, est. dead data %s is less than %d%%", file.Path, core.HumanSize(deadSpace), threshold)
			lowestMove = int(fileNumber)
		}
		if err != nil {
			break // no more data files
		}

	}
	core.Log(core.LogInfo, "All %s files compacted, %s released", storageFileTypeInfo[fileType].Extension, core.HumanSize(compacted))
}

func (handler *StorageHandler) CheckMeta() {
	for metaFileNumber := int32(0); ; metaFileNumber++ {
		metaFile := handler.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
		if metaFile == nil {
			break // no more meta files
		}

		metaSize := metaFile.Size()
		metaFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)
		core.Log(core.LogInfo, "Checking metadata cache file #%d (%s)", metaFileNumber, core.HumanSize(metaSize))

		var entry storageMetaEntry

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < metaSize; {
			// Check if there is a Cgap marker here and in that case, jump ahead of it
			offset += SkipDataGap(metaFile.Reader)
			offset += int64(entry.Unserialize(metaFile.Reader))

			// TODO: CheckMeta only reads the dataBlockID from data files, but the handler.getNumberedFile returns a bufio reader with a 3kb buffer to fill
			dataFileNumber, dataOffset := entry.location.Get()
			dataFile := handler.getNumberedFile(storageFileTypeData, dataFileNumber, false)
			if dataFile == nil {
				panic(errors.New(fmt.Sprintf("Error reading block %x, metadata cache is pointing to data file %x which cannot be opened", entry.blockID[:], dataFileNumber)))
			} else {
				dataFile.Reader.Seek(dataOffset+4, os.SEEK_SET) // 4 bytes to skip the datamarker
				var dataBlockID core.Byte128
				dataBlockID.Unserialize(dataFile.Reader)

				if entry.blockID.Compare(dataBlockID) != 0 { // Double check, this is already repaired by CheckData
					panic(errors.New(fmt.Sprintf("Error reading block %x, metadata cache is pointing to block %x", entry.blockID[:], dataBlockID[:])))
				}
			}

			p := int(offset * 100 / metaSize)
			if p > lastProgress {
				lastProgress = p
				fmt.Printf("%d%%\r", lastProgress)
			}
		}
	}
	return
}

func (handler *StorageHandler) CheckIndexes() {

	for ixFileNumber := int32(0); ; ixFileNumber++ {
		var ixFile = handler.getNumberedFile(storageFileTypeIndex, ixFileNumber, false)
		if ixFile == nil {
			break // no more indexes
		}

		ixSize := ixFile.Size()
		core.Log(core.LogInfo, "Checking index file #%d (%s)", ixFileNumber, core.HumanSize(ixSize))
		if ixSize%storageIXEntrySize != storageFileHeaderSize {
			panic(errors.New(fmt.Sprintf("Index file %x size is not evenly divisable by the index entry size, file must be damaged", ixFileNumber)))
		}

		ixFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)

		var entry storageIXEntry

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; {
			offset += int64(entry.Unserialize(ixFile.Reader))
			if entry.flags&entryFlagDefunct == entryFlagDefunct {
				core.Log(core.LogDebug, "Skipping defunct index entry for %x found at %x:%x", entry.blockID[:], ixFileNumber, offset)
			} else if entry.flags&entryFlagExists == entryFlagExists { // In use
				o := int64(calculateIXEntryOffset(entry.blockID))
				if offset < o {
					panic(errors.New(fmt.Sprintf("Block %x found on an invalid offset %x, it should be >= %x", entry.blockID[:], offset, o)))
				}

				metaFileNumber, metaOffset := entry.location.Get()
				metaFile := handler.getNumberedFile(storageFileTypeMeta, metaFileNumber, false)
				if metaFile == nil {
					panic(errors.New(fmt.Sprintf("Error reading block %x, index is pointing to metadata cache file %x which cannot be opened", entry.blockID[:], metaFileNumber)))
				}
				metaFile.Reader.Seek(metaOffset, os.SEEK_SET)

				var metaEntry storageMetaEntry
				metaEntry.Unserialize(metaFile.Reader)
				if metaEntry.blockID.Compare(entry.blockID) != 0 {
					panic(errors.New(fmt.Sprintf("Error reading block %x, index is pointing to metadata cache for block %x", entry.blockID[:], metaEntry.blockID[:])))
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
func (handler *StorageHandler) CheckFiles(doRepair bool) (repaired int, critical int) {
	for fileType := 0; fileType < len(storageFileTypeInfo); fileType++ {
		for fileNumber := int32(0); ; fileNumber++ {
			filename := handler.getNumberedFileName(fileType, fileNumber)

			f, err := core.OpenBufferedFile(filename, storageFileTypeInfo[fileType].BufferSize, 0, 0666)
			if err != nil {
				break // no more files
			}

			var rewriteHeader bool
			var header storageFileHeader
			err = binary.Read(f.Reader, binary.BigEndian, &header.filetype)
			if err != nil {
				core.Log(core.LogInfo, "Unable to read file type from file header in file %s", filename)
				rewriteHeader = true
			} else if header.filetype != storageFileTypeInfo[fileType].Type {
				core.Log(core.LogInfo, "Incorrect filetype in file %s: %x (should be %x)", filename, header.filetype, storageFileTypeInfo[fileType].Type)
				rewriteHeader = true
			} else {
				err := binary.Read(f.Reader, binary.BigEndian, &header.version)
				PanicOn(err)
				if header.version != storageVersion {
					panic(errors.New(fmt.Sprintf("Cannot verify or repair storage version %d", header.version)))
				}
			}

			if rewriteHeader && !doRepair {
				critical++
			}
			if rewriteHeader && doRepair {
				core.Log(core.LogInfo, "Repairing file %s", filename)
				f.Close()
				os.Rename(filename, filename+".repair")

				write, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
				PanicOn(err)
				writer := bufio.NewWriter(write)

				header.version = storageVersion
				header.filetype = storageFileTypeInfo[fileType].Type
				header.Serialize(writer)

				read, err := os.Open(filename + ".repair")
				PanicOn(err)
				reader := bufio.NewReader(read)

				io.Copy(writer, reader)
				writer.Flush()

				write.Close()
				read.Close()

				repaired++
			} else {
				f.Close()
			}
		}
	}
	return repaired, critical
}

func (handler *StorageHandler) FindFreeOffset(fileType int, lowestNum int) (freeFileNum int32, freeOffset int64, freeFile *core.BufferedFile) {
	var err error
	freeFileNum = int32(lowestNum)
	freeOffset = int64(0)
	for {
		freeFile = handler.getNumberedFile(fileType, freeFileNum, true)
		freeOffset, err = freeFile.Writer.Seek(0, os.SEEK_END)
		PanicOn(err)
		if freeOffset <= storageOffsetLimit {
			return freeFileNum, freeOffset, freeFile
		}
		freeFileNum++
	}
}

func (handler *StorageHandler) CheckData(doRepair bool, startfile int32, endfile int32) (repaired int, critical int) {
	var dataEntry storageDataEntry
	defer dataEntry.Release()

	for datFileNumber := int32(startfile); ; datFileNumber++ {
		if endfile > 0 && datFileNumber > endfile {
			break
		}
		var datFile = handler.getNumberedFile(storageFileTypeData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}

		datSize := datFile.Size()
		core.Log(core.LogInfo, "Checking data file #%d (%s)", datFileNumber, core.HumanSize(datSize))
		datFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)

		var lastProgress = -1
		brokenSpot := int64(0)
		for offset := int64(storageFileHeaderSize); offset < datSize; {
			// Check if there is a Cgap marker here and in that case, jump ahead of it
			offset += SkipDataGap(datFile.Reader)

			blockOffset := offset

			skipToNextBlock := false
			if err := Try(func() { offset += int64(dataEntry.Unserialize(datFile.Reader)) }); err != nil {
				err := errors.New(fmt.Sprintf("Error reading dataEntry at %x:%x (%s)", datFileNumber, blockOffset, err))
				if doRepair {
					core.Log(core.LogError, "%v", err)
					skipToNextBlock = true
				} else {
					panic(err)
				}
			} else if err := Try(func() {
				if !dataEntry.block.VerifyBlock() {
					panic(errors.New("Content verification failed"))
				}
			}); err != nil {
				err := errors.New(fmt.Sprintf("Error verifying block %x (type %d, size %d) at %x:%x (%s)", dataEntry.block.BlockID, dataEntry.block.DataType, dataEntry.block.Data.Len(), datFileNumber, blockOffset, err))
				if doRepair {
					core.Log(core.LogError, "%v", err)
					skipToNextBlock = true
				} else {
					panic(err)
				}
			}
			if skipToNextBlock {
				if brokenSpot == 0 {
					brokenSpot = blockOffset
				}
				offset = blockOffset + 1
				datFile.Reader.Seek(offset, os.SEEK_SET)
				if o, err := ForwardToDataMarker(datFile.Reader); err == nil {
					offset += o
					core.Log(core.LogInfo, "Skipped forward to next block at %x:%x", datFileNumber, offset)
					continue
				} else {
					core.Log(core.LogInfo, "Skipped forward until %v", err)
					break
				}
			}
			if brokenSpot > 0 && blockOffset-brokenSpot < 12 {
				// Cannot fit a Cgap marker, so we need to move the block
				moveFileNum, moveOffset, moveFile := handler.FindFreeOffset(storageFileTypeData, 0)
				core.Log(core.LogDebug, "Rewriting block %x at (%x:%x)", dataEntry.block.BlockID[:], moveFileNum, moveOffset)
				dataEntry.Serialize(moveFile.Writer)
				moveFile.Writer.Flush()

				core.Log(core.LogTrace, "Creating new meta for block %x", dataEntry.block.BlockID[:])
				metaEntry := storageMetaEntry{blockID: dataEntry.block.BlockID, dataSize: uint32(dataEntry.block.Data.Len()), links: dataEntry.block.Links}
				metaEntry.location.Set(moveFileNum, moveOffset)
				metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

				core.Log(core.LogTrace, "Creating new index for block %x", dataEntry.block.BlockID[:])
				ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(dataEntry.block.BlockID)
				if err != nil {
					ixEntry = &storageIXEntry{flags: entryFlagExists, blockID: dataEntry.block.BlockID}
				}
				ixEntry.location.Set(metaFileNumber, metaOffset)
				handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry)
				continue
			} else if brokenSpot > 0 {
				datFile.Writer.Seek(brokenSpot, os.SEEK_SET)
				core.Log(core.LogTrace, "Creating a free space marker at %x:%x (skip %d bytes)", datFileNumber, brokenSpot, blockOffset-brokenSpot)
				core.WriteBytes(datFile.Writer, []byte("Cgap"))
				core.WriteInt64(datFile.Writer, blockOffset-brokenSpot)
				datFile.Writer.Flush()
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
					core.Log(core.LogError, "Metadata cache error for block %x (%s)", dataEntry.block.BlockID[:], err.Error())
					rewriteIX = true
				} else {
					f, o := metaEntry.location.Get()
					if f != datFileNumber || o != blockOffset {
						core.Log(core.LogError, "Metadata cache location error for block %x (%x:%x != %x:%x)", dataEntry.block.BlockID[:], f, o, datFileNumber, blockOffset)
						rewriteIX = true
					}
					if int(metaEntry.dataSize) != dataEntry.block.Data.Len() {
						core.Log(core.LogError, "Metadata cache size error for block %x (%x != %x)", dataEntry.block.BlockID[:], metaEntry.dataSize, dataEntry.block.Data.Len())
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
						core.Log(core.LogError, "Metadata cache block links mismatch for block %x", dataEntry.block.BlockID[:])
						rewriteIX = true
					}
				}
			}
			if ixEntry.flags&entryFlagNoLinks == entryFlagNoLinks && len(dataEntry.block.Links) > 0 {
				if !rewriteIX {
					core.Log(core.LogError, "Block %x has %d links but the index NoLinks flag is set", dataEntry.block.BlockID[:], len(dataEntry.block.Links))
				}
				ixEntry.flags &^= entryFlagNoLinks
				rewriteIX = true
			} else if ixEntry.flags&entryFlagNoLinks == 0 && len(dataEntry.block.Links) == 0 {
				if !rewriteIX {
					core.Log(core.LogError, "Block %x has no links but the index NoLinks flag is not set", dataEntry.block.BlockID[:])
				}
				ixEntry.flags |= entryFlagNoLinks
				rewriteIX = true
			}

			if !rewriteIX {
				core.Log(core.LogTrace, "Block %x (%x:%x) verified", dataEntry.block.BlockID[:], datFileNumber, blockOffset)
			} else {
				if doRepair {
					core.Log(core.LogTrace, "REPAIRING meta for block %x", dataEntry.block.BlockID[:])
					metaEntry := storageMetaEntry{blockID: dataEntry.block.BlockID, dataSize: uint32(dataEntry.block.Data.Len()), links: dataEntry.block.Links}
					metaEntry.location.Set(datFileNumber, blockOffset)
					metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

					core.Log(core.LogTrace, "REPAIRING index for block %x", dataEntry.block.BlockID[:])
					ixEntry.location.Set(metaFileNumber, metaOffset)
					handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry)
					repaired++
				} else {
					critical++
				}
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
		if doRepair { // reset the amount of dead space as everything has been added back when repairing
			handler.setDeadSpace(storageFileTypeData, datFileNumber, 0, false)
		}
	}
	return repaired, critical
}

func (handler *StorageHandler) ShowStorageDeadSpace() {
	for datFileNumber := int32(0); ; datFileNumber++ {
		var datFile = handler.getNumberedFile(storageFileTypeData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}
		fileSize, deadSpace, err := handler.getNumberedFileSize(storageFileTypeData, datFileNumber)
		PanicOn(err)
		core.Log(core.LogInfo, "File %s, %s (est. dead data %s)", datFile.Path, core.HumanSize(fileSize), core.HumanSize(deadSpace))
	}
}
