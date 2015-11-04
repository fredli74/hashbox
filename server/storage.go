//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/hashbox/core"
	"bytes"
	"encoding/binary"
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

	storageDataFileSize int64  = 4 << 32    // 16 GB data files
	storageDataMarker   uint32 = 0x68626C6B // "hblk"
)

type storageFileHeader struct {
	filetype uint32
	version  uint32
}

func (h storageFileHeader) Serialize(w io.Writer) {
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

type storageIXEntry struct {
	Flags   uint32       // 4 bytes
	BlockID core.Byte128 // 16 bytes
	File    int32        // 4 bytes
	Offset  int64        // 8 bytes
}

func (e storageIXEntry) Serialize(w io.Writer) {
	core.WriteOrPanic(w, e.Flags)
	e.BlockID.Serialize(w)
	core.WriteOrPanic(w, e.File)
	core.WriteOrPanic(w, e.Offset)
}
func (e *storageIXEntry) Unserialize(r io.Reader) {
	core.ReadOrPanic(r, &e.Flags)
	e.BlockID.Unserialize(r)
	core.ReadOrPanic(r, &e.File)
	core.ReadOrPanic(r, &e.Offset)
}

type storageDataEntry struct {
	_datamarker uint32 // = storageDataMarker, used to find / align blocks in case of recovery
	Block       *core.HashboxBlock
}

func (e storageDataEntry) Serialize(w io.Writer) {
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

// calculateEntryOffset calculates a position into the index file where the blockID should be found using the following formula:
// Each IXEntry is 32 bytes, so to align the offset to 32 bytes we do not use the first 5 bits in the offset value
// We also limit the offset to the max size of an indexfile (24 bits = 512MB indexes)
func calculateEntryOffset(blockID core.Byte128) uint32 {
	// Put first 24 bits of the hash in AND with 24 bits and mask out the first 5 bits
	return (uint32(blockID[15]) | uint32(blockID[14])<<8 | uint32(blockID[13])<<16) & 0xffffe0
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
	ixOffset := int64(calculateEntryOffset(blockID))
	ixFileNumber := int32(0)

	for {
		info, err := os.Stat(handler.getNumberedFileName(storageFileExtensionIndex, ixFileNumber))
		if err != nil || ixOffset >= int64(info.Size()) {
			break
		}
		var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber)

		var entry storageIXEntry
		ixFile.Seek(ixOffset, 0)

		err = binary.Read(ixFile, binary.BigEndian, &entry)
		if err != nil || entry.Flags&1 == 0 {
			break
		}
		if bytes.Equal(blockID[:], entry.BlockID[:]) {
			return &entry, ixFileNumber, ixOffset, nil
		}
		ixFileNumber++
	}
	return nil, ixFileNumber, ixOffset, errors.New("BlockID entry not found")
}
func (handler *StorageHandler) writeIXEntry(ixFileNumber int32, ixOffset int64, entry *storageIXEntry) {
	var ixFile = handler.getNumberedFile(storageFileExtensionIndex, ixFileNumber)
	ixFile.Seek(ixOffset, 0)
	entry.Serialize(ixFile)
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

	handler.writeIXEntry(ixFileNumber, ixOffset, &storageIXEntry{Flags: 0x01, BlockID: block.BlockID, File: handler.topDatFileNumber, Offset: datOffset})
	return true
}

func (handler *StorageHandler) readBlockFile(blockID core.Byte128) (*core.HashboxBlock, error) {
	var dataEntry storageDataEntry

	indexEntry, _, _, err := handler.readIXEntry(blockID)
	if err != nil {
		return nil, err
	}

	datFile := handler.getNumberedFile(storageFileExtensionData, indexEntry.File)
	datFile.Seek(indexEntry.Offset, 0)
	dataEntry.Unserialize(datFile)
	return dataEntry.Block, nil
}
