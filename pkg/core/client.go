//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"io"
	"runtime/debug"

	"github.com/fredli74/bytearray"

	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const DEFAULT_QUEUE_SIZE int64 = 32 * 1024 * 1024 // 32 MiB max memory
const DEFAULT_CONNECTION_TIMEOUT time.Duration = 5 * time.Minute

type messageDispatch struct {
	msg           *ProtocolMessage
	returnChannel chan interface{}
}

type Client struct {
	// variables used in atomic operations (declared first to make sure they are 32 / 64 bit aligned)
	msgNum              uint32 // protocol message number, 32-bit because there are no 16-bit atomic functions
	sendworkers         int32  // number of active send workers
	transmittedBlocks   int32  // number of transmitted blocks
	skippedBlocks       int32  // number of skipped blocks
	WriteData           int64  // total data written
	WriteDataCompressed int64  // total compressed data written

	Session
	AccessKey Byte128 // = hmac^20000( AccountName "*ACCESS*KEY*PAD*", md5( password ))

	ServerAddress string
	connection    *TimeoutConn

	wg          sync.WaitGroup
	EnablePaint bool

	QueueMax   int64         // max size of the outgoing block queue (in bytes)
	ThreadMax  int32         // maximum number of goroutines started by send queue (defaults to runtime.NumCPU)
	Retry      bool          // retry on network errors/EOF
	RetryMax   int           // max retry attempts; -1 means retry forever, 0 disables retries
	RetryWait  time.Duration // interval between retries
	retryCount int           // total retries this session

	sendMutex sync.Mutex // protects from two threads sending at the same time

	// mutex protected
	dispatchMutex  sync.Mutex
	closing        bool
	blockQueueSize int64 // queue size in bytes
	blockQueueMap  map[Byte128]*blockQueueEntry
	blockQueueList []*blockQueueEntry

	dispatchChannel   chan *messageDispatch // input to goroutine; buffered list of outgoing messages to dispatch
	blockWriteChannel chan *messageDispatch // input to goroutine; next block in queue to store
	errorChannel      chan error            // output from goroutine; ioHandler encountered an error
	lastError         error                 // last error encountered
}

func NewClient(address string, account string, accesskey Byte128) *Client {

	client := &Client{
		ServerAddress: address,
		AccessKey:     accesskey,
		Session: Session{
			AccountNameH: Hash([]byte(account)),
		},
		blockQueueMap: make(map[Byte128]*blockQueueEntry),

		QueueMax:  DEFAULT_QUEUE_SIZE,
		ThreadMax: int32(runtime.NumCPU() / 2),
		Retry:     true,
		RetryMax:  -1,
		RetryWait: 15 * time.Second,

		dispatchChannel:   make(chan *messageDispatch, 1024),
		blockWriteChannel: make(chan *messageDispatch, 1),
	}

	if address != "" {
		client.Connect()
	}
	client.errorChannel = make(chan error, 1)
	client.wg.Add(1)
	go client.ioHandler()

	return client
}

var lastPaint string = "\n"

func (c *Client) Paint(what string) {
	if c.EnablePaint && (what != "\n" || what != lastPaint) {
		fmt.Print(what)
		lastPaint = what
	}
}

func (c *Client) Close(polite bool) {
	c.dispatchMutex.Lock()
	if !c.closing {
		if polite {
			c.dispatchMutex.Unlock()
			func() {
				defer func() {
					r := recover()
					if r != nil {
						Log(LogDebug, "Error sending Goodbye message to server (%v)", r)
					}
				}()
				c.dispatchAndWait(MsgTypeGoodbye, nil)
			}()
			c.dispatchMutex.Lock()
		}
		c.closing = true
		close(c.dispatchChannel)
		close(c.blockWriteChannel)
	}
	c.dispatchMutex.Unlock()

	if connection := c.connection; connection != nil {
		connection.Close() // This will cancel a blocking IO-read if we have one
	}
	c.wg.Wait()
}

const (
	blockStateNew int32 = iota
	blockStateRequested
	blockStateProcessing
	blockStateProcessed
	blockStateQueued
	blockStateSending
	blockStateCompleted
)

type blockQueueEntry struct {
	state int32
	block *HashboxBlock
}

func (c *Client) queueBlock(block *HashboxBlock) bool {
	c.dispatchMutex.Lock()
	if c.blockQueueMap[block.BlockID] != nil {
		c.dispatchMutex.Unlock()
		Abort("ASSERT! Block %x already in queue", block.BlockID)
	}

	var size int64
	if block.Compressed {
		size = bytearray.ChunkQuantize(int64(block.CompressedSize))
	} else {
		size = bytearray.ChunkQuantize(int64(block.UncompressedSize))
	}
	if c.blockQueueSize+size > c.QueueMax {
		c.dispatchMutex.Unlock()
		return false // Queue full, retry later
	}

	entry := &blockQueueEntry{state: int32(blockStateNew), block: block}
	c.blockQueueMap[block.BlockID] = entry
	c.blockQueueList = append(c.blockQueueList, entry)
	c.blockQueueSize += size

	Log(LogTrace, "Queue block %x (queue=%d)", block.BlockID, len(c.blockQueueList))

	if c.sendworkers < 1 || c.sendworkers < c.ThreadMax {
		atomic.AddInt32(&c.sendworkers, 1)
		go func() {
			defer func() { // a panic was raised inside the goroutine (most likely the channel was closed)
				if r := recover(); !c.closing && r != nil {
					if c.lastError != nil {
						panic(c.lastError)
					}
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}
					c.lastError = err
					select {
					case c.errorChannel <- err:
					default:
					}
					panic(r)
				}
			}()

			for done := false; !done; {
				var workItem *blockQueueEntry

				// Process split into two, first queue traversal under lock, then processing without lock
				c.dispatchMutex.Lock()
				for i := 0; workItem == nil && i < len(c.blockQueueList); i++ {
					entry := c.blockQueueList[i]
					switch entry.state {
					case blockStateNew: // ignore for now, waiting for singleExchange allocate response
					case blockStateRequested:
						workItem = entry
					case blockStateProcessing: // skip, being compressed
					case blockStateProcessed:
						if i == 0 {
							workItem = entry
						}
					case blockStateQueued: // skip, waiting to be sent
					case blockStateSending: // skip, waiting for ACK
					case blockStateCompleted:
						if entry.block.Compressed {
							c.blockQueueSize -= bytearray.ChunkQuantize(int64(entry.block.CompressedSize))
						} else {
							c.blockQueueSize -= bytearray.ChunkQuantize(int64(entry.block.UncompressedSize))
						}
						entry.block.Release()
						delete(c.blockQueueMap, entry.block.BlockID)
						c.blockQueueList = append(c.blockQueueList[:i], c.blockQueueList[i+1:]...)
						i-- // adjust index
					default:
						Abort("ASSERT! Invalid state %d for blockQueue entry", entry.state)
					}
				}
				if workItem != nil {
					// Transition to next state, so next worker does not pick it up
					atomic.AddInt32(&workItem.state, 1)
				} else if len(c.blockQueueList) == 0 {
					done = true
					atomic.AddInt32(&c.sendworkers, -1)
					//						fmt.Println("worker stopping")
				}
				c.dispatchMutex.Unlock()

				// Process work item outside lock
				if workItem != nil {
					switch workItem.state {
					case blockStateNew:
						panic("ASSERT!")
					case blockStateRequested:
						panic("ASSERT!")
					case blockStateProcessing:
						if !workItem.block.Compressed {
							workItem.block.CompressData()
							diff := bytearray.ChunkQuantize(int64(workItem.block.UncompressedSize)) - bytearray.ChunkQuantize(int64(workItem.block.CompressedSize))
							atomic.AddInt64(&c.blockQueueSize, -diff)
						}
						atomic.AddInt32(&workItem.state, 1)
					case blockStateProcessed:
						panic("ASSERT!")
					case blockStateQueued:
						atomic.AddInt64(&c.WriteData, int64(workItem.block.UncompressedSize))
						atomic.AddInt64(&c.WriteDataCompressed, int64(workItem.block.CompressedSize))
						atomic.AddInt32(&c.transmittedBlocks, 1) //	c.transmittedBlocks++
						c.Paint("*")
						Log(LogTrace, "Upload block %x (links=%d, size=%d, compressed=%d)", workItem.block.BlockID, len(workItem.block.Links), workItem.block.UncompressedSize, workItem.block.CompressedSize)
						msg := &ProtocolMessage{Type: MsgTypeWriteBlock, Data: &MsgClientWriteBlock{Block: workItem.block}}
						c.blockWriteChannel <- &messageDispatch{msg: msg}
						atomic.AddInt32(&workItem.state, 1)
					default:
						Abort("ASSERT! Invalid state %d for work item", workItem.state)
					}
				} else {
					time.Sleep(25 * time.Millisecond)
				}
			}
		}()
	}

	c.dispatchMutex.Unlock()
	c.dispatchMessage(MsgTypeAllocateBlock, &MsgClientAllocateBlock{BlockID: block.BlockID}, nil)
	return true
}

func (c *Client) handshake(connection *TimeoutConn) {
	data := c.singleExchange(connection, &messageDispatch{msg: &ProtocolMessage{Type: MsgTypeGreeting, Data: &MsgClientGreeting{Version: ProtocolVersion}}}).Data
	if data == nil {
		panic(errors.New("Server did not respond correctly to handshake"))
	}
	r := data.(*MsgServerGreeting)
	clientTime := uint64(time.Now().Unix())
	serverTime := binary.BigEndian.Uint64(r.SessionNonce[:]) / 1000000000
	if clientTime < serverTime-600 || clientTime > serverTime+600 {
		panic(errors.New("Connection refused, system time difference between client and server is more than 10 minutes"))
	}

	c.SessionNonce = r.SessionNonce
}

func (c *Client) authenticate(connection *TimeoutConn) {
	c.GenerateSessionKey(c.AccessKey)
	c.singleExchange(connection, &messageDispatch{msg: &ProtocolMessage{Type: MsgTypeAuthenticate, Data: &MsgClientAuthenticate{
		AccountNameH:    c.AccountNameH,
		AuthenticationH: DeepHmac(1, c.AccountNameH[:], c.SessionKey),
	}}})
}

func (c *Client) Handshake() {
	c.handshake(c.connection)
}

func (c *Client) Authorize() {
	c.handshake(c.connection)
	c.authenticate(c.connection)
}

// Dial opens a new socket connection and wraps it in a TimeoutConn.
func (c *Client) Dial() {
	c.connection = nil // Drop previous connection object

	conn, err := net.Dial("tcp", c.ServerAddress)
	AbortOn(err)
	c.connection = NewTimeoutConn(conn, DEFAULT_CONNECTION_TIMEOUT)
}

func (c *Client) Connect() {
	c.Dial()
	c.Authorize()
}
func (c *Client) singleExchange(connection *TimeoutConn, outgoing *messageDispatch) *ProtocolMessage {
	// Send an outgoing message
	outgoing.msg.Num = uint16(atomic.AddUint32(&c.msgNum, 1) - 1)
	WriteMessage(connection, outgoing.msg)

	// Wait for the reply
	incoming := ReadMessage(connection)
	if incoming.Num != outgoing.msg.Num {
		panic(errors.New("ASSERT! This should never happen unless the server is coded wrong"))
	}
	if outgoing.returnChannel != nil {
		outgoing.returnChannel <- incoming
	}

	// Handle block queue
	switch d := incoming.Data.(type) {
	case *MsgServerError:
		panic(errors.New("Received error from server: " + string(d.ErrorMessage)))
	case *MsgServerReadBlock:
		c.dispatchMutex.Lock()
		q := c.blockQueueMap[d.BlockID]
		if q != nil && q.state == blockStateNew {
			q.state = blockStateRequested
		}
		c.dispatchMutex.Unlock()
	case *MsgServerAcknowledgeBlock:
		var skipped bool = false

		c.dispatchMutex.Lock()
		q := c.blockQueueMap[d.BlockID]
		if q != nil {
			if q.state == blockStateNew {
				// Block was not sent
				skipped = true
			}
			q.state = blockStateCompleted
		}
		c.dispatchMutex.Unlock()

		if skipped {
			atomic.AddInt32(&c.skippedBlocks, 1) //c.skippedBlocks++
			c.Paint("-")
		}
	}
	return incoming
}

func (c *Client) retryingExchange(outgoing *messageDispatch) (r *ProtocolMessage) {
	if c.ServerAddress == "" {
		// Only used in unit testing
		return c.singleExchange(c.connection, outgoing)
	}

	for ever := true; ever && !c.closing; {
		ever = func() (retry bool) {
			retry = outgoing.msg.Type != MsgTypeGreeting && outgoing.msg.Type != MsgTypeGoodbye && outgoing.msg.Type != MsgTypeAuthenticate &&
				(c.RetryMax < 0 || c.retryCount < c.RetryMax)

			defer func() {
				if retry {
					err := recover()
					if err == nil {
						return
					}

					c.connection = nil
					switch e := err.(type) {
					case net.Error:
						Log(LogError, e.Error())
						Log(LogInfo, "Stacktrace from panic: %s", debug.Stack())
						return // Network error, retry and retry again
					case error:
						if e == io.EOF {
							Log(LogError, "Lost connection with server (%v)", e.Error())
							Log(LogInfo, "Stacktrace from panic: %s", debug.Stack())
							return // Network stream closed, non fatal
						}
						Log(LogError, "%v", e)
					default:
						Log(LogError, "Unknown error in client communication")
						Log(LogError, fmt.Sprint(e))
					}
					// Any other error is fatal
					panic(err)
				}
			}()

			if c.connection == nil {
				if retry {
					c.retryCount++
					Log(LogInfo, "Retrying connection in %s", c.RetryWait)
					time.Sleep(c.RetryWait)
					Log(LogInfo, "Reconnecting to server")
					c.Connect()
				}
			} else {
				r = c.singleExchange(c.connection, outgoing)
				retry = false
			}
			return
		}()
	}
	return r
}

func (c *Client) ioHandler() {
	defer func() {
		if r := recover(); !c.closing && r != nil { // a panic was raised inside the goroutine
			c.lastError = r.(error)
			c.errorChannel <- r.(error)
			close(c.errorChannel) // close it to trigger everyone to stop
		}
		c.wg.Done()
	}()

	for {
		select {
		case outgoing, ok := <-c.dispatchChannel:
			if !ok {
				// Channel is closed
				return
			}
			c.retryingExchange(outgoing)
		default:
			select {
			case outgoing, ok := <-c.dispatchChannel:
				if !ok {
					// Channel is closed
					return
				}
				c.retryingExchange(outgoing)
			case outgoing, ok := <-c.blockWriteChannel:
				if !ok {
					// Channel is closed
					return
				}
				c.retryingExchange(outgoing)
			}
		}
	}
}

// dispatchMessage returns a result channel if a returnChannel was specified, otherwise it just returns nil
func (c *Client) dispatchMessage(msgType uint32, msgData interface{}, returnChannel chan interface{}) {
	defer func() {
		if r := recover(); !c.closing && r != nil { // a panic was raised (most likely the channel was closed)
			if c.lastError != nil {
				panic(c.lastError)
			}
			panic(r)
		}
	}()

	if !c.closing {
		select {
		case c.dispatchChannel <- &messageDispatch{msg: &ProtocolMessage{Type: msgType, Data: msgData}, returnChannel: returnChannel}:
			return
		case err, _ := <-c.errorChannel:
			if c.lastError != nil {
				panic(c.lastError)
			}
			AbortOn(err)
			panic("Why did we end up here?")
		}
	} else if returnChannel != nil {
		close(returnChannel)
	}
}

// dispatchAndWait will always return the response you were waiting for or throw a panic, so there is no need to check return values
func (c *Client) dispatchAndWait(msgType uint32, msgData interface{}) interface{} {
	waiter := make(chan interface{}, 1)
	defer close(waiter)
	c.dispatchMessage(msgType, msgData, waiter)
	select {
	case R, ok := <-waiter:
		if !ok {
			panic(errors.New("Server disconnected while waiting for a response"))
		}
		switch t := R.(type) {
		case *ProtocolMessage:
			switch dt := t.Data.(type) {
			case *MsgServerError:
				panic(errors.New("Received error from server: " + string(dt.ErrorMessage)))
			default:
				return t.Data
			}
		}
	case err := <-c.errorChannel:
		if c.lastError != nil {
			panic(c.lastError)
		}
		AbortOn(err)
		panic(errors.New("Connection was closed while waiting for a response"))
	}
	panic(errors.New("ASSERT! We should not reach this point"))
}

func (c *Client) GetAccountInfo() *MsgServerAccountInfo {
	r := c.dispatchAndWait(MsgTypeAccountInfo, &MsgClientAccountInfo{AccountNameH: c.AccountNameH}).(*MsgServerAccountInfo)
	return r
}
func (c *Client) ListDataset(datasetName string) *MsgServerListDataset {
	r := c.dispatchAndWait(MsgTypeListDataset, &MsgClientListDataset{AccountNameH: c.AccountNameH, DatasetName: String(datasetName)}).(*MsgServerListDataset)
	return r
}
func (c *Client) AddDatasetState(datasetName string, state DatasetState) {
	c.dispatchAndWait(MsgTypeAddDatasetState, &MsgClientAddDatasetState{AccountNameH: c.AccountNameH, DatasetName: String(datasetName), State: state})
}
func (c *Client) RemoveDatasetState(datasetName string, stateID Byte128) {
	c.dispatchAndWait(MsgTypeRemoveDatasetState, &MsgClientRemoveDatasetState{AccountNameH: c.AccountNameH, DatasetName: String(datasetName), StateID: stateID})
}

func (c *Client) VerifyBlock(blockID Byte128) bool {
	r := c.dispatchAndWait(MsgTypeAllocateBlock, &MsgClientAllocateBlock{BlockID: blockID})
	switch r.(type) {
	case *MsgServerAcknowledgeBlock:
		return true
	case *MsgServerReadBlock:
		return false
	default:
		panic(errors.New("Unknown response from server"))
	}
}

func (c *Client) StoreData(dataType byte, data bytearray.ByteArray, links []Byte128) Byte128 {
	// Create a block
	block := NewHashboxBlock(dataType, data, links)
	return c.StoreBlock(block)
}

// StoreBlock is blocking if the block queue is full
func (c *Client) StoreBlock(block *HashboxBlock) Byte128 {
	// Add the block to the io queue
	for queued := false; !queued; {
		c.dispatchMutex.Lock()
		if c.closing {
			c.dispatchMutex.Unlock()
			panic(errors.New("Connection closed"))
		}
		if c.blockQueueMap[block.BlockID] != nil {
			c.dispatchMutex.Unlock()
			Log(LogTrace, "Block %x already in queue", block.BlockID)
			block.Release()
			return block.BlockID
		}
		c.dispatchMutex.Unlock()
		// Try to queue the block
		queued = c.queueBlock(block)
		if !queued {
			time.Sleep(25 * time.Millisecond)
		}
	}
	return block.BlockID
}
func (c *Client) ReadBlock(blockID Byte128) *HashboxBlock {
	b := c.dispatchAndWait(MsgTypeReadBlock, &MsgClientReadBlock{BlockID: blockID}).(*MsgServerWriteBlock)
	b.Block.UncompressData()
	return b.Block
}
func (c *Client) Commit() {
	for done := false; !done; time.Sleep(100 * time.Millisecond) {
		func() {
			done = c.Done()
		}()
	}
}
func (c *Client) Done() bool {
	if c.lastError != nil {
		panic(c.lastError)
	}
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	return c.closing || len(c.blockQueueMap) == 0
}

const hashPadding_accesskey = "*ACCESS*KEY*PAD*" // TODO: move to client source

// binary.BigEndian.Get and binary.BigEndian.Put  much faster than
// binary.Read and binary.Write

func (c *Client) GetStats() (tranismitted int32, skipped int32, queued int32, queuesize int64) {
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	return c.transmittedBlocks, c.skippedBlocks, int32(len(c.blockQueueMap)), c.blockQueueSize
}
