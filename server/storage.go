//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/hashbox/core"
	"bufio"
	"bytes"
	_ "encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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

	filepool         map[string]*os.File
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
		filepool: make(map[string]*os.File),
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

type storageFileHeader struct {
	filetype uint32
	version  uint32
}

func (h *storageFileHeader) Serialize(w io.Writer) {
	core.WriteOrPanic(w, h.filetype)
	core.WriteOrPanic(w, h.version)
}
func (h *storageFileHeader) Unserialize(r io.Reader) {
	core.ReadOrPanic(r, &h.filetype)
	core.ReadOrPanic(r, &h.version)
	if h.version != storageVersion {
		panic(errors.New("Invalid version in dbFileHeader"))
	}
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

func (e *storageIXEntry) Serialize(w io.Writer) {
	core.WriteOrPanic(w, e.Flags)
	e.BlockID.Serialize(w)
	core.WriteOrPanic(w, e.Location)
}
func (e *storageIXEntry) Unserialize(r io.Reader) {
	core.ReadOrPanic(r, &e.Flags)
	e.BlockID.Unserialize(r)
	core.ReadOrPanic(r, &e.Location)
}

type storageDataEntry struct {
	_datamarker uint32 // = storageDataMarker, used to find / align blocks in case of recovery
	Block       *core.HashboxBlock
}

func (e *storageDataEntry) Serialize(w io.Writer) {
	core.WriteOrPanic(w, storageDataMarker)
	e.Block.Serialize(w)
}
func (e *storageDataEntry) Unserialize(r io.Reader) {
	core.ReadOrPanic(r, &e._datamarker)
	if e._datamarker != storageDataMarker {
		panic(errors.New(fmt.Sprintf("Incorrect Block on offset (%s/%s file corruption)", storageFileExtensionIndex, storageFileExtensionData)))
	}
	e.Block = &core.HashboxBlock{}
	e.Block.Unserialize(r)
	testhash := e.Block.HashData()
	if !bytes.Equal(testhash[:], e.Block.BlockID[:]) {
		panic(errors.New(fmt.Sprintf("Corrupted Block read from disk (hash check incorrect, %x != %x", e.Block.HashData(), e.Block.BlockID)))
	}
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
func (handler *StorageHandler) getNumberedFile(fileType string, fileNumber int32) *os.File {
	name := handler.getNumberedName(fileType, fileNumber)
	if handler.filepool[name] == nil {
		filename := handler.getNumberedFileName(fileType, fileNumber)

		serverLog("Opening file:", filename)
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		handler.filepool[name] = f
	}
	return handler.filepool[name]
}

func (handler *StorageHandler) readIXEntry(blockID core.Byte128) (*storageIXEntry, int32, int64, error) {
	baseOffset := int64(calculateEntryOffset(blockID))
	ixOffset := baseOffset
	ixFileNumber := int32(0)
	for {
		info, err := os.Stat(handler.getNumberedFileName(storageFileExtensionIndex, ixFileNumber))
		if err != nil || baseOffset >= int64(info.Size()) {
			break
		}
		var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber)

		var entry storageIXEntry
		ixFile.Seek(ixOffset, 0)

		r := bufio.NewReaderSize(ixFile, storageIXEntrySize*storageIXEntryProbeLimit)

		for i := 0; i < storageIXEntryProbeLimit; i++ {
			entry.Unserialize(r)
			//			err = binary.Read(r, binary.BigEndian, &entry)
			if /*err != nil || */ entry.Flags&1 == 0 {
				return nil, ixFileNumber, ixOffset, errors.New("BlockID entry not found")
			}
			if bytes.Equal(blockID[:], entry.BlockID[:]) {
				return &entry, ixFileNumber, ixOffset, nil
			}
			ixOffset += int64(storageIXEntrySize)
		}
		ixFileNumber++
		ixOffset = baseOffset
	}
	return nil, ixFileNumber, ixOffset, errors.New("BlockID entry not found")
}
func (handler *StorageHandler) writeIXEntry(ixFileNumber int32, ixOffset int64, entry *storageIXEntry) {
	var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber)
	ixFile.Seek(ixOffset, 0)
	w := bufio.NewWriter(ixFile)
	entry.Serialize(w) // (ixFile)
	w.Flush()
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

	var datFile *os.File
	var datOffset int64
	for {
		datFile = handler.getNumberedFile(storageFileExtensionData, handler.topDatFileNumber)
		fi, _ := datFile.Stat()
		datOffset = fi.Size()
		if datOffset+int64(data.Len()) <= storageDataFileSize {
			break
		}
		handler.topDatFileNumber++
	}

	datFile.Seek(datOffset, 0)
	data.WriteTo(datFile)

	ixEntry := storageIXEntry{Flags: 0x01, BlockID: block.BlockID}
	ixEntry.SetLocation(handler.topDatFileNumber, datOffset)
	handler.writeIXEntry(ixFileNumber, ixOffset, &ixEntry)
	return true
}

func (handler *StorageHandler) readBlockFile(blockID core.Byte128) (*core.HashboxBlock, error) {
	_ = "breakpoint"
	var dataEntry storageDataEntry

	indexEntry, _, _, err := handler.readIXEntry(blockID)
	if err != nil {
		return nil, err
	}

	file, offset := indexEntry.GetLocation()
	datFile := handler.getNumberedFile(storageFileExtensionData, file)
	datFile.Seek(offset, 0)
	dataEntry.Unserialize(datFile)
	return dataEntry.Block, nil
}
