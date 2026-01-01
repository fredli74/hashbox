//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package main

import (
	"sync"

	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/hashbox/pkg/storagedb"
)

//***********************************************************************//
//                         StorageHandler                                //
//***********************************************************************//

type ChannelCommand struct {
	command int
	data    interface{}
	result  chan interface{}
}

type StorageHandler struct {
	signal  chan error // goroutine signal channel, returns raised errors and stops goroutine when closed
	queue   chan ChannelCommand
	wg      sync.WaitGroup
	closing bool

	store *storagedb.Store
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
					q.result <- handler.store.DoesBlockExist(blockID)
				case storagehandler_readBlock:
					blockID := q.data.(core.Byte128)
					q.result <- handler.store.ReadBlock(blockID)
				case storagehandler_writeBlock:
					block := q.data.(*core.HashboxBlock)
					q.result <- handler.store.WriteBlock(block)
				default:
					core.Abort("Unknown query in StorageHandler causing hangup: %d", q.command)
				}
			}()
		case _, ok := <-handler.signal: // Signal is closed?
			core.ASSERT(!ok, ok) // We should not reach this point with "ok", it means someone outside this goroutine sent a signal on the channel
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
				core.Abort("StorageHandler panic: %v", err.Error())
			}
		default:
			switch t := r.(type) {
			case error:
				core.Abort("StorageHandler panic: %v", t.Error())
			}
		}
	}()
	handler.queue <- q
	r := <-q.result
	return r
}

func (handler *StorageHandler) SyncAll() {
	handler.store.SyncAll()
}

func (handler *StorageHandler) Close() {
	handler.closing = true
	close(handler.signal)
	handler.wg.Wait()
	handler.store.Close()
}

func NewStorageHandler() *StorageHandler {
	handler := &StorageHandler{
		queue:  make(chan ChannelCommand, 32),
		signal: make(chan error),
		store:  storagedb.NewStore(datDirectory, idxDirectory),
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
	}
	return false
}
func (handler *StorageHandler) readBlock(BlockID core.Byte128) *core.HashboxBlock {
	q := ChannelCommand{storagehandler_readBlock, BlockID, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(*core.HashboxBlock)
	}
	return nil
}
func (handler *StorageHandler) writeBlock(Block *core.HashboxBlock) bool {
	q := ChannelCommand{storagehandler_writeBlock, Block, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(bool)
	}
	return false
}
