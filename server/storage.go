//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/hashbox/core"

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

	filepool          map[string]*BufferedFile
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

var storageFileTypeInfo []_storageFileTypeInfo = []_storageFileTypeInfo{
	_storageFileTypeInfo{0x48534958, ".idx", 2024},  // "HSIX" Hashbox Storage Index
	_storageFileTypeInfo{0x48534D44, ".meta", 2024}, // "HSMD" Hashbox Storage Index
	_storageFileTypeInfo{0x48534442, ".dat", 4096},  // "HSDB" Hashbox Storage Index
}

const (
	storageVersion uint32 = 1

	storageFileHeaderSize int64 = 16 // 16 bytes (filetype + version + deadspace)

	storageMaxFileSize int64  = (1 << 34) - 1 // 0x3ffffffff = 16 GiB data and metadata files
	storageDataMarker  uint32 = 0x68626C6B    // "hblk"
)

const ( // 2 bytes, max 16 flags
	entryFlagExists = 1 << iota
	entryFlagNoLinks
	entryFlagMarked
)

type storageFileHeader struct {
	filetype  uint32
	version   uint32
	deadspace int64
}

func (h *storageFileHeader) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, h.filetype)
	size += core.WriteOrPanic(w, h.version)
	size += core.WriteOrPanic(w, h.deadspace)
	return
}
func (h *storageFileHeader) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, &h.filetype)
	size += core.ReadOrPanic(r, &h.version)
	if h.version != storageVersion {
		panic(errors.New("Invalid version in dbFileHeader"))
	}
	size += core.ReadOrPanic(r, &h.deadspace)
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
	size += core.WriteOrPanic(w, b[:])
	return
}
func (b *sixByteLocation) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, b[:])
	return
}

//*******************************************************************************//
//                           storageIXEntry                                      //
//*******************************************************************************//

const storageIXEntrySize int64 = 24      // 24 bytes
const storageIXEntryProbeLimit int = 682 // 682*24 bytes = 16368 < 16k

type storageIXEntry struct { // 24 bytes data
	flags    int16           // 2 bytes
	blockID  core.Byte128    // 16 bytes
	location sixByteLocation // 6 bytes
}

func (e *storageIXEntry) BlockID() core.Byte128 {
	return e.blockID
}

func (e *storageIXEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, e.flags)
	size += e.blockID.Serialize(w)
	size += e.location.Serialize(w)
	return
}
func (e *storageIXEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, &e.flags)
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
	// BranchSize int64          // Size of all hashboxBlock Data linked to
	links []core.Byte128 // Array of BlockIDs
}

func (e *storageMetaEntry) BlockID() core.Byte128 {
	return e.blockID
}

func (e *storageMetaEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, storageDataMarker)
	size += e.blockID.Serialize(w)
	size += e.location.Serialize(w)
	size += core.WriteOrPanic(w, e.dataSize)
	// size += core.WriteOrPanic(w, e.BranchSize)
	size += core.WriteOrPanic(w, uint32(len(e.links)))
	for i := range e.links {
		size += e.links[i].Serialize(w)
	}
	return
}
func (e *storageMetaEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, &e.datamarker)
	if e.datamarker != storageDataMarker {
		panic(errors.New(fmt.Sprintf("Incorrect metadata cache marker %x (should be %x)", e.datamarker, storageDataMarker)))
	}
	size += e.blockID.Unserialize(r)
	size += e.location.Unserialize(r)
	size += core.ReadOrPanic(r, &e.dataSize)
	// size += core.ReadOrPanic(r, &e.BranchSize)
	var n uint32
	size += core.ReadOrPanic(r, &n)
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
	size += core.WriteOrPanic(w, storageDataMarker)
	size += e.block.Serialize(w)
	return
}

// IMPORTANT Unserialize allocates memory that needs to be freed manually
func (e *storageDataEntry) Unserialize(r io.Reader) (size int) {
	size += core.ReadOrPanic(r, &e.datamarker)
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
func (handler *StorageHandler) getNumberedFile(fileType int, fileNumber int32, create bool) *BufferedFile {
	name := handler.getNumberedName(fileType, fileNumber)
	if handler.filepool[name] == nil {
		filename := handler.getNumberedFileName(fileType, fileNumber)

		flag := 0
		if create {
			flag |= os.O_CREATE
		}

		f, err := OpenBufferedFile(filename, storageFileTypeInfo[fileType].BufferSize, flag, 0666)
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
		serverLog("Opening file:", filename)
		handler.filepool[name] = f
	}
	return handler.filepool[name]
}

// setDeadSpace is used to mark the amount of dead space in a meta or data file
func (handler *StorageHandler) setDeadSpace(fileType int, fileNumber int32, size int64, add bool) {
	file := handler.getNumberedFile(fileType, fileNumber, false)
	if file == nil {
		panic(errors.New(fmt.Sprintf("Trying to mark free space in %.8X%s which does not exist", fileNumber, storageFileTypeInfo[fileType].Extension)))
	}
	var header storageFileHeader
	file.Reader.Seek(0, os.SEEK_SET)
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
			Debug("ran out of index files")
			break
		}
		ixSize := ixFile.Size()
		if ixOffset > ixSize {
			Debug("%d > %d", ixOffset, ixSize)
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
			if bytes.Equal(blockID[:], entry.blockID[:]) {
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
	entry.Serialize(ixFile.Writer)
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

	var metaFile *BufferedFile
	if metaOffset == 0 { // Offset 0 does not exist as it is in the header
		for {
			metaFile = handler.getNumberedFile(storageFileTypeMeta, handler.topMetaFileNumber, true)
			metaOffset, _ = metaFile.Writer.Seek(0, os.SEEK_END)
			Debug("writeMetaEntry %x:%x", handler.topMetaFileNumber, metaOffset)
			if metaOffset+int64(data.Len()) <= storageMaxFileSize {
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

/*
func (handler *StorageHandler) sumBranch(links []core.Byte128) (size int64) {
	for _, l := range links {
		entry, _, _, err := handler.readIXEntry(l)
PanicOn(err)
		f, o := entry.location.Get()
		meta, err := handler.readMetaEntry(f, o)
PanicOn(err)
		size += meta.BranchSize + int64(meta.DataSize)
	}
	return size
}*/
func (handler *StorageHandler) writeBlockFile(block *core.HashboxBlock) bool {
	_, ixFileNumber, ixOffset, err := handler.readIXEntry(block.BlockID)
	if err == nil {
		// Block already exists
		return false
	}

	dataEntry := storageDataEntry{block: block}
	var data = new(bytes.Buffer)
	dataEntry.Serialize(data)

	var datFile *BufferedFile
	var datOffset int64
	for {
		datFile = handler.getNumberedFile(storageFileTypeData, handler.topDatFileNumber, true)
		datOffset, _ = datFile.Writer.Seek(0, os.SEEK_END)
		if datOffset+int64(data.Len()) <= storageMaxFileSize {
			break
		}
		handler.topDatFileNumber++
	}

	data.WriteTo(datFile.Writer)
	datFile.Writer.Flush()

	metaEntry := storageMetaEntry{blockID: block.BlockID, dataSize: uint32(block.Data.Len()), links: block.Links} // , BranchSize: handler.sumBranch(block.Links)}
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

func (handler *StorageHandler) CheckChain(roots []core.Byte128, Paint bool) {
	var chain []core.Byte128
	visited := make(map[core.Byte128]bool) // Keep track if we have read the links from the block already

	var progress time.Time
	for _, r := range roots {
		Debug("CheckChain on %x", r[:])
		chain = append(chain, r)
		for i := 0; len(chain) > 0; i++ {
			blockID := chain[len(chain)-1]
			chain = chain[:len(chain)-1]
			if !visited[blockID] {
				entry, _, _, err := handler.readIXEntry(blockID)
				PanicOn(err)
				if entry.flags&entryFlagNoLinks == 0 {
					metaFileNumber, metaOffset := entry.location.Get()
					metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
					PanicOn(err)
					if len(metaEntry.links) > 0 && entry.flags&entryFlagNoLinks == entryFlagNoLinks {
						//panic(errors.New(fmt.Sprintf("Index entry for block %x is marked with NoLinks but the block has %d links", blockID[:], len(metaEntry.links))))
					} else if len(metaEntry.links) == 0 && entry.flags&entryFlagNoLinks == 0 {
						Debug("Block %x has no links but the index is missing NoLinks flag", blockID[:])
					}
					chain = append(chain, metaEntry.links...)
				}
				visited[blockID] = true
			}

			if Paint && time.Now().After(progress) {
				fmt.Printf("%8d links\r", len(chain))
				progress = time.Now().Add(500 * time.Millisecond)
			}
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
			PanicOn(err)
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
				core.WriteOrPanic(ixFile.Writer, entry.flags)
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
			fmt.Printf("Sweeping index file #%d (%s)\n", ixFileNumber, core.HumanSize(ixSize))
		}

		ixFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset <= ixSize; offset += storageIXEntrySize {
			var entry storageIXEntry
			if offset < ixSize { // We do one extra step to be able to clean up the end of a file
				entry.Unserialize(ixFile.Reader)
			}

			if entry.flags&entryFlagExists == entryFlagExists {
				if entry.flags&entryFlagMarked == 0 {
					// Delete it! Well we need to compact or we will break linear probing
					deletedBlocks++
					metaFileNumber, metaOffset := entry.location.Get()
					sweepedSize += handler.killMetaEntry(entry.blockID, metaFileNumber, metaOffset)

					blanks = append(blanks, offset)
					Debug("Deleted orphan Block %x IX from %x:%x", entry.blockID[:], ixFileNumber, offset)
				} else {
					entry.flags &^= entryFlagMarked

					// Move it down to a lower idx file?
					e, f, o, err := handler.readIXEntry(entry.blockID)
					if f < ixFileNumber {
						Debug("Moving file for Block %x IX from %x:%x to %x:%x", entry.blockID[:], ixFileNumber, offset, f, o)
						if e != nil || err == nil {
							panic("ASSERT, this makes no sense, the block is in a lower idx already?")
						}
						handler.writeIXEntry(f, o, &entry)
						deletedBlocks++
						metaFileNumber, metaOffset := entry.location.Get()
						sweepedSize += handler.killMetaEntry(entry.blockID, metaFileNumber, metaOffset)

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
							core.WriteOrPanic(ixFile.Writer, entry.flags)
						} else {
							Debug("Moving Block %x IX from %x:%x to %x:%x", entry.blockID[:], ixFileNumber, offset, ixFileNumber, newOffset)
							entry.Serialize(ixFile.Writer)
						}
						ixFile.Writer.Flush()
					}
				}
			} else {

				// We found a hole, so we have nothing left to move
				for len(blanks) > 0 {
					Debug("Clearing %x:%x", ixFileNumber, blanks[0])
					ixFile.Writer.Seek(blanks[0], os.SEEK_SET)
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
			lastActiveEntry += storageIXEntrySize
		}
		if lastActiveEntry < ixSize {
			Debug("Truncating file %x at %x", ixFileNumber, lastActiveEntry)
			ixFile.Writer.File.Truncate(lastActiveEntry)
		}
	}
	if Paint {
		fmt.Printf("Removed %d blocks (referencing %s data)\n", deletedBlocks, core.ShortHumanSize(sweepedSize))
	}
}

func (handler *StorageHandler) CompactFile(fileType int, fileNumber int32) (compacted int64) {
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

	var deadSpace int64
	{
		file.Reader.Seek(0, os.SEEK_SET)
		var header storageFileHeader
		header.Unserialize(file.Reader)
		deadSpace = header.deadspace
	}

	fileSize := file.Size()
	serverLog(fmt.Sprintf("Compacting file %s, %s (est. dead data %s)", file.Path, core.HumanSize(fileSize), core.HumanSize(deadSpace)))

	// Open a separate reader
	reader, err := OpenBufferedReader(file.Path, file.BufferSize, file.Flag)
	PanicOn(err)
	_, err = reader.Seek(storageFileHeaderSize, os.SEEK_SET)
	PanicOn(err)

	var lastProgress = -1
	offset, writeOffset := int64(storageFileHeaderSize), int64(storageFileHeaderSize)
	for offset < fileSize {
		// Check if there is a Cgap marker here and in that case, jump ahead of it
		peek, err := reader.Peek(12)
		PanicOn(err)
		if bytes.Equal(peek[:4], []byte("Cgap")) {
			skip := core.BytesInt64(peek[4:12])
			Debug("Cgap marker, jumping %d bytes", skip)
			// Special skip logic since Discard only takes int32 and not int64
			for skip > 0 {
				n, _ := reader.Discard(core.LimitInt(skip))
				offset += int64(n)
				skip -= int64(n)
			}
			continue
		}

		readOffset := offset
		entrySize := entry.Unserialize(reader)
		offset += int64(entrySize)
		entryBlockID := entry.BlockID()

		Debug("Read %x:%x block %x (%d bytes)", fileNumber, readOffset, entryBlockID[:], entrySize)

		if _, _, _, err = handler.readIXEntry(entryBlockID); err != nil {
			Debug("Not in index: %s", err.Error())
		} else if !entry.VerifyLocation(handler, fileNumber, readOffset) {
			Debug("Not the same as in index (old invalid entry)")
		} else if readOffset != writeOffset {
			newOffset := writeOffset
			if readOffset-writeOffset < int64(entrySize)+12 { // 12 bytes for the Cgap marker
				Debug("No room to move block to %x, %d < %d (+12 bytes)", writeOffset, readOffset-writeOffset, entrySize)

				var newFile *BufferedFile
				var newOffset int64
				var newFileNumber int32 = 0
				for {
					newFile = handler.getNumberedFile(fileType, newFileNumber, true)
					newOffset, err = newFile.Writer.Seek(0, os.SEEK_END)
					PanicOn(err)
					if newOffset+int64(entrySize)+12 <= storageMaxFileSize {
						break
					}
					newFileNumber++
				}
				written := int64(entry.Serialize(newFile.Writer))
				newFile.Writer.Flush()
				Debug("Moved block to end of file %s, bytes %x-%x", newFile.Path, newOffset, newOffset+written)
				if newFileNumber == fileNumber {
					Debug("File %s, increased in size from %x to %x", newFile.Path, fileSize, fileSize+written)
					fileSize += written
				}

				entry.ChangeLocation(handler, newFileNumber, newOffset)
			} else {
				file.Writer.Seek(newOffset, os.SEEK_SET)
				written := int64(entry.Serialize(file.Writer))
				Debug("Moved block from %x to %x-%x", readOffset, writeOffset, writeOffset+written)
				writeOffset += written

				Debug("Writing Cgap skip marker to %x (skip %d)", readOffset, readOffset-writeOffset)

				core.WriteOrPanic(file.Writer, []byte("Cgap"))
				core.WriteOrPanic(file.Writer, readOffset-writeOffset)
				file.Writer.Flush()

				entry.ChangeLocation(handler, fileNumber, newOffset)
			}
		} else {
			writeOffset += int64(entrySize)
			Debug("No need to write, moving writeOffset to %x", writeOffset)
		}

		p := int(offset * 100 / fileSize)
		if p > lastProgress {
			fmt.Printf("%d (%d%%)\r", writeOffset-offset, p)
		}
	}
	if writeOffset > offset {
		panic(" ASSERT !  compact made the file larger?")
	}

	compacted = (offset - writeOffset)
	file.Writer.File.Truncate(writeOffset)
	handler.setDeadSpace(fileType, fileNumber, 0, false)

	serverLog(fmt.Sprintf("Removed %s from file %s", core.HumanSize(compacted), file.Path))
	return compacted
}
func (handler *StorageHandler) CompactAll(fileType int) {
	var compacted int64
	for fileNumber := int32(0); ; fileNumber++ {
		file := handler.getNumberedFile(fileType, fileNumber, false)
		if file == nil {
			break // no more data
		}

		compacted += handler.CompactFile(fileType, fileNumber)
	}
	serverLog(fmt.Sprintf("All %s files compacted, %s released", storageFileTypeInfo[fileType].Extension, core.HumanSize(compacted)))
}

func (handler *StorageHandler) CheckMeta(doRepair bool) (repaired int, critical int) {

	for metaFileNumber := int32(0); ; metaFileNumber++ {
		// Because BranchSize checking moves the normal reader, we open a new file handler
		filename := handler.getNumberedFileName(storageFileTypeMeta, metaFileNumber)
		metaFile, err := OpenBufferedFile(filename, 1024*1024, os.O_RDONLY, 0)
		if err != nil {
			break // no more meta files
		}

		metaSize := metaFile.Size()
		metaFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)
		fmt.Printf("Checking metadata cache file #%d (%s)\n", metaFileNumber, core.HumanSize(metaSize))

		var entry storageMetaEntry

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < metaSize; {
			metaOffset := offset
			offset += int64(entry.Unserialize(metaFile.Reader))

			dataFileNumber, dataOffset := entry.location.Get()
			dataFile := handler.getNumberedFile(storageFileTypeData, dataFileNumber, false)
			if dataFile == nil {
				panic(errors.New(fmt.Sprintf("Error reading block %x, metadata cache is pointing to data file %x which cannot be opened", entry.blockID[:], dataFileNumber)))
			}
			dataFile.Reader.Seek(dataOffset+4, os.SEEK_SET) // 4 bytes to skip the datamarker
			var dataBlockID core.Byte128
			dataBlockID.Unserialize(dataFile.Reader)

			if entry.blockID.Compare(dataBlockID) != 0 { // Double check, this is already repaired by CheckData
				panic(errors.New(fmt.Sprintf("Error reading block %x, metadata cache is pointing to block %x", entry.blockID[:], dataBlockID[:])))
			}

			var rewriteMeta bool
			{ // Check DataSize
				var n uint32
				core.ReadOrPanic(dataFile.Reader, &n)
				if n > 0 {
					dataFile.Reader.Discard(int(n) * 16)
				}
				dataFile.Reader.Discard(1) // 1 byte for datatype
				core.ReadOrPanic(dataFile.Reader, &n)
				if n != entry.dataSize {
					fmt.Printf("Incorrect metadata for block %x, DataSize %d (should be %d)\n", entry.blockID[:], entry.dataSize, n)
					entry.dataSize = n
					rewriteMeta = true
				}
			}
			/*
								{ // Do a full branch check
									var links []core.Byte128
									var n int64
									links = append(links, entry.Links...)
									for links := entry.Links; len(links) > 0; {
										var l core.Byte128
										l, links = links[len(links)-1], links[:len(links)-1]

										ixEntry, _, _, err := handler.readIXEntry(l)
				PanicOn(err)
										mf, mo := ixEntry.location.Get()
										metaEntry, err := handler.readMetaEntry(mf, mo)
				PanicOn(err)
										n += int64(metaEntry.dataSize)
										links = append(links, metaEntry.links...)
									}
									if n != entry.BranchSize {
										fmt.Printf("Incorrect metadata for block %x, BranchSize %d (should be %d)\n", entry.blockID[:], entry.BranchSize, n)
										entry.BranchSize = n
										rewriteMeta = true
									}
								}*/
			if rewriteMeta && doRepair {
				fmt.Printf("REPAIRING metadata for block %x at %x:%x\n", entry.blockID[:], metaFileNumber, metaOffset)
				_, err := metaFile.Writer.Seek(metaOffset, os.SEEK_SET)
				PanicOn(err)
				entry.Serialize(metaFile.Writer)
				metaFile.Writer.Flush()
				repaired++
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
		fmt.Printf("Checking index file #%d (%s)\n", ixFileNumber, core.HumanSize(ixSize))
		if ixSize%storageIXEntrySize != storageFileHeaderSize {
			panic(errors.New(fmt.Sprintf("Index file %x size is not evenly divisable by the index entry size, file must be damaged", ixFileNumber)))
		}

		ixFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)

		var entry storageIXEntry

		var lastProgress = -1
		for offset := int64(storageFileHeaderSize); offset < ixSize; {
			offset += int64(entry.Unserialize(ixFile.Reader))
			if entry.flags&entryFlagExists == entryFlagExists { // In use
				o := int64(calculateIXEntryOffset(entry.blockID))
				if offset < o {
					panic(errors.New(fmt.Sprintf("Block %x found on an invalid offset %d, it should be >= %d", entry.blockID[:], offset, o)))
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

func (handler *StorageHandler) CheckFiles(doRepair bool) (repaired int, critical int) {
	for fileType := 0; fileType < len(storageFileTypeInfo); fileType++ {
		for fileNumber := int32(0); ; fileNumber++ {
			filename := handler.getNumberedFileName(fileType, fileNumber)

			f, err := OpenBufferedFile(filename, storageFileTypeInfo[fileType].BufferSize, 0, 0666)
			if err != nil {
				break // no more files
			}

			var rewriteHeader bool
			var header storageFileHeader
			err = binary.Read(f.Reader, binary.BigEndian, &header.filetype)
			if err != nil {
				fmt.Printf("Unable to read file type from file header in file %s\n", filename)
				critical++
				rewriteHeader = true
			} else if header.filetype != storageFileTypeInfo[fileType].Type {
				fmt.Printf("Incorrect filetype in file %s: %x (should be %x)\n", filename, header.filetype, storageFileTypeInfo[fileType].Type)
				critical++
				rewriteHeader = true
			} else {
				err := binary.Read(f.Reader, binary.BigEndian, &header.version)
				PanicOn(err)
				if header.version != storageVersion {
					panic(errors.New(fmt.Sprintf("Cannot verify or repair storage version %d", header.version)))
				}
			}

			if rewriteHeader && doRepair {
				fmt.Println("Repairing file", filename)
				f.Close()
				os.Rename(filename, filename+".repair")

				write, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
				PanicOn(err)
				writer := bufio.NewWriter(write)

				header.version = storageVersion
				header.filetype = storageFileTypeInfo[fileType].Type
				header.Serialize(writer)

				read, err := os.OpenFile(filename+".repair", os.O_RDONLY, 0666)
				PanicOn(err)
				reader := bufio.NewReader(read)

				io.Copy(writer, reader)
				writer.Flush()

				write.Close()
				read.Close()
			} else {
				f.Close()
			}
		}
	}
	return repaired, critical
}

func (handler *StorageHandler) CheckData(doRepair bool) (repaired int, critical int) {
	var dataEntry storageDataEntry
	defer dataEntry.Release()

	for datFileNumber := int32(0); ; datFileNumber++ {
		var datFile = handler.getNumberedFile(storageFileTypeData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}

		datSize := datFile.Size()
		fmt.Printf("Checking data file #%d (%s)\n", datFileNumber, core.HumanSize(datSize))
		datFile.Reader.Seek(storageFileHeaderSize, os.SEEK_SET)

		var lastProgress = -1
		var lastRepair = 0
		for offset := int64(storageFileHeaderSize); offset < datSize; {
			blockOffset := offset
			offset += int64(dataEntry.Unserialize(datFile.Reader))
			if !dataEntry.block.VerifyBlock() {
				critical++
				// TODO... something!
				panic(errors.New(fmt.Sprintf("Unable to verify block %x in datafile at %x:%x", dataEntry.block.BlockID, datFileNumber, blockOffset)))
			}

			rewriteIX := false
			ixEntry, ixFileNumber, ixOffset, err := handler.readIXEntry(dataEntry.block.BlockID)
			if err != nil {
				fmt.Printf("Orphan block at %x:%x (%s)\n", datFileNumber, blockOffset, err.Error())
				ixEntry = &storageIXEntry{flags: entryFlagExists, blockID: dataEntry.block.BlockID}
				ixEntry.location.Set(datFileNumber, blockOffset)
				rewriteIX = true
			} else {
				metaFileNumber, metaOffset := ixEntry.location.Get()
				metaEntry, err := handler.readMetaEntry(metaFileNumber, metaOffset)
				if err != nil {
					fmt.Printf("Metadata cache error for block %x (%s)\n", dataEntry.block.BlockID[:], err.Error())
					rewriteIX = true
					critical++
				} else {
					f, o := metaEntry.location.Get()
					if f != datFileNumber || o != blockOffset {
						fmt.Printf("Metadata cache error for block %x (%d:%d != %d:%d)\n", dataEntry.block.BlockID[:], f, o, datFileNumber, blockOffset)
						rewriteIX = true
						critical++
					}
				}
			}
			if ixEntry.flags&entryFlagNoLinks == entryFlagNoLinks && len(dataEntry.block.Links) > 0 {
				if !rewriteIX {
					fmt.Printf("Block %x has %d links but the index NoLinks flag is set\n", dataEntry.block.BlockID[:], len(dataEntry.block.Links))
				}
				ixEntry.flags &^= entryFlagNoLinks
				rewriteIX = true
				critical++
			} else if ixEntry.flags&entryFlagNoLinks == 0 && len(dataEntry.block.Links) == 0 {
				if !rewriteIX {
					fmt.Printf("Block %x has no links but the index NoLinks flag is not set\n", dataEntry.block.BlockID[:])
				}
				ixEntry.flags |= entryFlagNoLinks
				rewriteIX = true
			}

			if doRepair && rewriteIX {
				fmt.Printf("REPAIRING meta for block %x\n", dataEntry.block.BlockID[:])
				metaEntry := storageMetaEntry{blockID: dataEntry.block.BlockID, dataSize: uint32(dataEntry.block.Data.Len()), links: dataEntry.block.Links}
				metaEntry.location.Set(datFileNumber, blockOffset)
				metaFileNumber, metaOffset := handler.writeMetaEntry(0, 0, &metaEntry)

				fmt.Printf("REPAIRING index for block %x\n", dataEntry.block.BlockID[:])
				ixEntry.location.Set(metaFileNumber, metaOffset)
				handler.writeIXEntry(ixFileNumber, ixOffset, ixEntry)
				repaired++
			}

			p := int(offset * 100 / datSize)
			if p > lastProgress {
				lastProgress = p
				fmt.Printf("%d%%\r", lastProgress)
				if doRepair && repaired != lastRepair {
					fmt.Println()
					lastRepair = repaired
				}
			}
		}
		if doRepair { // reset the amount of dead space as everything has been added back when repairing
			handler.setDeadSpace(storageFileTypeData, datFileNumber, 0, false)
		}
	}
	return repaired, critical
}
