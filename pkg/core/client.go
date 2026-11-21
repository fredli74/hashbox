//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Package core provides Hashbox core primitives.
package core

import (
	"bufio"
	"io"
	"runtime/debug"
	"slices"

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
	returnChannel chan *ProtocolMessage
	entry         *blockQueueEntry // the queue's own allocate or write, only its reply moves the entry's state
}

type Client struct {
	// variables used in atomic operations (declared first to make sure they are 32 / 64 bit aligned)
	msgNum              uint32 // protocol message number, 32-bit because there are no 16-bit atomic functions
	sendworkers         int32  // number of active send workers
	transmittedBlocks   int32  // number of transmitted blocks
	skippedBlocks       int32  // number of skipped blocks
	blockQueueLen       int32  // number of items in block queue
	blockQueueSize      int64  // queue size in bytes
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
	RetryMax   int           // max retry attempts; -1 means retry forever, 0 disables retries
	RetryWait  time.Duration // interval between retries
	retryCount int           // total retries this session

	// mutex protected
	dispatchMutex  sync.Mutex
	closing        bool
	blockQueueMap  map[Byte128]*blockQueueEntry
	blockQueueList []*blockQueueEntry

	dispatchChannel   chan *messageDispatch // input to goroutine; buffered list of outgoing messages to dispatch
	blockWriteChannel chan *messageDispatch // input to goroutine; next block in queue to store
	errorChannel      chan error            // output from goroutine; ioHandler encountered an error
	lastError         error                 // last error encountered

	inflightMutex sync.Mutex
	inflight      []*messageDispatch // written and waiting for a reply, in send order, resent after a reconnect
}

// RemoteError marks a failure of the client or the server, as opposed to damage
// found in the data itself. Nothing can be read after one is raised, so a caller
// recovering block reads must not treat it as a bad block.
type RemoteError struct{ error }

func (e RemoteError) Unwrap() error { return e.error }

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
		RetryMax:  -1,
		RetryWait: 15 * time.Second,

		dispatchChannel:   make(chan *messageDispatch, 1024),
		blockWriteChannel: make(chan *messageDispatch, maxInFlight),
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
		_ = connection.Close() // wakes a blocking read, the connection may already be closed after an error
	}
	c.wg.Wait()
}

const (
	blockStateNew int32 = iota
	blockStateRequested
	blockStateProcessing
	blockStateProcessed
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
	if atomic.LoadInt64(&c.blockQueueSize)+size > c.QueueMax {
		c.dispatchMutex.Unlock()
		return false // Queue full, retry later
	}

	entry := &blockQueueEntry{state: int32(blockStateNew), block: block}
	c.blockQueueMap[block.BlockID] = entry
	c.blockQueueList = append(c.blockQueueList, entry)
	atomic.AddInt64(&c.blockQueueSize, size)
	atomic.AddInt32(&c.blockQueueLen, 1)

	Log(LogTrace, "Queue block %x (queue=%d)", block.BlockID, len(c.blockQueueList))

	if c.sendworkers < 1 || c.sendworkers < c.ThreadMax {
		c.sendworkers++
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
					c.lastError = RemoteError{err}
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
				// Blocks are handed to the writer under the lock and in queue order, so a parent never reaches
				// the server before its children. Later blocks may be written while earlier ones wait for their ACK.
				inOrder := !c.closing // the write channel is closed under this lock
				for i := 0; workItem == nil && i < len(c.blockQueueList); i++ {
					entry := c.blockQueueList[i]
					switch entry.state {
					case blockStateNew: // ignore for now, waiting for the allocate response
						inOrder = false
					case blockStateRequested:
						workItem = entry
						entry.state = blockStateProcessing
					case blockStateProcessing: // skip, being compressed
						inOrder = false
					case blockStateProcessed:
						if inOrder {
							select {
							case c.blockWriteChannel <- &messageDispatch{msg: &ProtocolMessage{Type: MsgTypeWriteBlock, Data: &MsgClientWriteBlock{Block: entry.block}}, entry: entry}:
								entry.state = blockStateSending
								atomic.AddInt64(&c.WriteData, int64(entry.block.UncompressedSize))
								atomic.AddInt64(&c.WriteDataCompressed, int64(entry.block.CompressedSize))
								atomic.AddInt32(&c.transmittedBlocks, 1)
								c.Paint("*")
								Log(LogTrace, "Upload block %x (links=%d, size=%d, compressed=%d)", entry.block.BlockID, len(entry.block.Links), entry.block.UncompressedSize, entry.block.CompressedSize)
							default: // the writer is full, try again later
								inOrder = false
							}
						}
					case blockStateSending: // skip, waiting for ACK
					case blockStateCompleted:
						if entry.block.Compressed {
							atomic.AddInt64(&c.blockQueueSize, -bytearray.ChunkQuantize(int64(entry.block.CompressedSize)))
						} else {
							atomic.AddInt64(&c.blockQueueSize, -bytearray.ChunkQuantize(int64(entry.block.UncompressedSize)))
						}
						entry.block.Release()
						delete(c.blockQueueMap, entry.block.BlockID)
						c.blockQueueList = append(c.blockQueueList[:i], c.blockQueueList[i+1:]...)
						atomic.AddInt32(&c.blockQueueLen, -1)
						i-- // adjust index
					default:
						Abort("ASSERT! Invalid state %d for blockQueue entry", entry.state)
					}
				}
				if workItem == nil && len(c.blockQueueList) == 0 {
					done = true
					c.sendworkers--
					//						fmt.Println("worker stopping")
				}
				c.dispatchMutex.Unlock()

				// Compress the work item outside the lock
				if workItem != nil {
					if !workItem.block.Compressed {
						workItem.block.CompressData()
						diff := bytearray.ChunkQuantize(int64(workItem.block.UncompressedSize)) - bytearray.ChunkQuantize(int64(workItem.block.CompressedSize))
						atomic.AddInt64(&c.blockQueueSize, -diff)
					}
					c.dispatchMutex.Lock()
					ASSERT(workItem.state == blockStateProcessing, "ASSERT! Invalid state %d for blockQueue entry", workItem.state)
					workItem.state = blockStateProcessed
					c.dispatchMutex.Unlock()
				} else {
					time.Sleep(25 * time.Millisecond)
				}
			}
		}()
	}

	c.dispatchMutex.Unlock()
	c.dispatchMessage(MsgTypeAllocateBlock, &MsgClientAllocateBlock{BlockID: block.BlockID}, nil, entry)
	return true
}

func (c *Client) handshake(connection *TimeoutConn) {
	data := c.singleExchange(connection, &messageDispatch{msg: &ProtocolMessage{Type: MsgTypeGreeting, Data: &MsgClientGreeting{Version: ProtocolVersion}}}).Data
	if data == nil {
		panic(errors.New("server did not respond correctly to handshake"))
	}
	r := data.(*MsgServerGreeting)
	clientTime := uint64(time.Now().Unix())
	serverTime := binary.BigEndian.Uint64(r.SessionNonce[:]) / 1000000000
	if clientTime < serverTime-600 || clientTime > serverTime+600 {
		panic(errors.New("connection refused, system time difference between client and server is more than 10 minutes"))
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
	AbortOnError(err)
	c.connection = NewTimeoutConn(conn, DEFAULT_CONNECTION_TIMEOUT)
}

func (c *Client) Connect() {
	c.Dial()
	c.Authorize()
}

// singleExchange sends one message and waits for its reply, used for the handshake before streaming starts.
func (c *Client) singleExchange(connection *TimeoutConn, outgoing *messageDispatch) *ProtocolMessage {
	outgoing.msg.Num = uint16(atomic.AddUint32(&c.msgNum, 1) - 1)
	WriteMessage(connection, outgoing.msg)
	incoming := ReadMessage(connection)
	c.handleReply(incoming, outgoing)
	return incoming
}

func (c *Client) handleReply(incoming *ProtocolMessage, outgoing *messageDispatch) {
	if incoming.Num != outgoing.msg.Num {
		panic(errors.New("ASSERT! This should never happen unless the server is coded wrong"))
	}
	if outgoing.returnChannel != nil {
		outgoing.returnChannel <- incoming
	}

	if e, isError := incoming.Data.(*MsgServerError); isError {
		panic(errors.New("Received error from server: " + string(e.ErrorMessage)))
	}
	q := outgoing.entry
	if q == nil {
		return
	}
	// the queue's own allocate is answered before its write is sent
	c.dispatchMutex.Lock()
	switch incoming.Data.(type) {
	case *MsgServerReadBlock:
		ASSERT(q.state == blockStateNew, "ASSERT! Invalid state %d for blockQueue entry", q.state)
		q.state = blockStateRequested
	case *MsgServerAcknowledgeBlock:
		ASSERT(q.state == blockStateNew || q.state == blockStateSending, "ASSERT! Invalid state %d for blockQueue entry", q.state)
		if q.state == blockStateNew { // the server already has it, it was never sent
			atomic.AddInt32(&c.skippedBlocks, 1)
			c.Paint("-")
		}
		q.state = blockStateCompleted
	}
	c.dispatchMutex.Unlock()
}

// maxInFlight is how many messages are written ahead of their replies.
const maxInFlight = 1024

// ioHandler streams outgoing messages: they are written without waiting for replies, and a reader
// matches the replies, which the server sends in order. After a reconnect the messages still waiting
// for a reply are resent in order; every client message is safe to repeat.
func (c *Client) ioHandler() {
	defer func() {
		if r := recover(); !c.closing && r != nil { // a panic was raised inside the goroutine
			c.lastError = RemoteError{r.(error)}
			c.errorChannel <- r.(error)
			close(c.errorChannel) // close it to trigger everyone to stop
		}
		c.wg.Done()
	}()

	// waits for the first message, the connection may be set up after NewClient
	if _, ok := c.nextOutgoing(nil); !ok {
		return
	}
	for !c.streamConnection() {
	}
}

// nextOutgoing takes the next message, other messages before block writes, numbers it and adds it to the in-flight list.
func (c *Client) nextOutgoing(failed chan any) (*messageDispatch, bool) {
	var outgoing *messageDispatch
	var ok bool
	select {
	case outgoing, ok = <-c.dispatchChannel:
	default:
		select {
		case outgoing, ok = <-c.dispatchChannel:
		case outgoing, ok = <-c.blockWriteChannel:
		case r := <-failed:
			panic(r)
		}
	}
	if ok {
		outgoing.msg.Num = uint16(atomic.AddUint32(&c.msgNum, 1) - 1)
		c.inflightMutex.Lock()
		c.inflight = append(c.inflight, outgoing)
		c.inflightMutex.Unlock()
	}
	return outgoing, ok
}

// streamConnection streams on one connection, reconnecting first if needed.
// Returns true when the client is closed, false to reconnect; a fatal error panics.
func (c *Client) streamConnection() (closed bool) {
	defer func() {
		if r := recover(); r != nil {
			closed = c.closing
			if !closed {
				c.retryOrPanic(r)
				c.connection = nil // a handshake that failed during reconnect
			}
		}
	}()
	if c.connection == nil {
		c.retryCount++
		Log(LogInfo, "Retrying connection in %s", c.RetryWait)
		time.Sleep(c.RetryWait)
		Log(LogInfo, "Reconnecting to server")
		c.Connect()
	}
	connection := c.connection
	pending := make(chan struct{}, maxInFlight) // one token per message waiting for its reply
	failed := make(chan any, 1)
	exited := make(chan struct{})
	c.wg.Add(1)
	go c.readReplies(connection, pending, failed, exited)
	defer func() {
		close(pending)
		if !closed { // on close, Close() closes the connection
			_ = connection.Close() // wakes the reader if it is reading
		}
		<-exited
	}()
	// a token is handed to the reader after each write, a full channel holds the sender back
	send := func(outgoing *messageDispatch) {
		WriteMessage(connection, outgoing.msg)
		select {
		case pending <- struct{}{}:
		case r := <-failed:
			panic(r)
		}
	}

	c.inflightMutex.Lock()
	resend := slices.Clone(c.inflight)
	c.inflightMutex.Unlock()
	for _, outgoing := range resend {
		send(outgoing)
	}
	for {
		outgoing, ok := c.nextOutgoing(failed)
		if !ok {
			return true
		}
		send(outgoing)
	}
}

// readReplies matches replies to the messages in flight, reading only while one is waiting, so an idle connection never times out.
func (c *Client) readReplies(connection *TimeoutConn, pending chan struct{}, failed chan any, exited chan struct{}) {
	defer c.wg.Done()
	defer close(exited)
	defer func() {
		if r := recover(); r != nil {
			failed <- r
		}
	}()
	reader := bufio.NewReader(connection) // one read per buffer, not per field
	for range pending {
		incoming := ReadMessage(reader)
		c.inflightMutex.Lock()
		outgoing := c.inflight[0]
		c.inflight[0] = nil // the answered message, often a block write, is not kept alive by the slice
		c.inflight = c.inflight[1:]
		c.inflightMutex.Unlock()
		c.handleReply(incoming, outgoing)
	}
}

// retryOrPanic returns if the error lost the connection and a reconnect is allowed, otherwise it panics with it.
func (c *Client) retryOrPanic(err any) {
	c.inflightMutex.Lock()
	goodbye := slices.ContainsFunc(c.inflight, func(outgoing *messageDispatch) bool { return outgoing.msg.Type == MsgTypeGoodbye })
	c.inflightMutex.Unlock()
	if c.ServerAddress == "" || goodbye || (c.RetryMax >= 0 && c.retryCount >= c.RetryMax) {
		panic(err)
	}
	switch e := err.(type) {
	case net.Error:
		Log(LogError, e.Error())
		Log(LogInfo, "Stacktrace from panic: %s", debug.Stack())
	case error:
		if e != io.EOF {
			Log(LogError, "%v", e)
			panic(err)
		}
		Log(LogError, "Lost connection with server (%v)", e.Error())
		Log(LogInfo, "Stacktrace from panic: %s", debug.Stack())
	default:
		Log(LogError, "Unknown error in client communication")
		Log(LogError, fmt.Sprint(e))
		panic(err)
	}
}

// dispatchMessage queues a message, returnChannel (if set) receives the reply
func (c *Client) dispatchMessage(msgType uint32, msgData interface{}, returnChannel chan *ProtocolMessage, entry *blockQueueEntry) {
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
		case c.dispatchChannel <- &messageDispatch{msg: &ProtocolMessage{Type: msgType, Data: msgData}, returnChannel: returnChannel, entry: entry}:
			return
		case err := <-c.errorChannel:
			if c.lastError != nil {
				panic(c.lastError)
			}
			AbortOnError(err)
			panic("Why did we end up here?")
		}
	} else if returnChannel != nil {
		close(returnChannel)
	}
}

// dispatchAndWait will always return the response you were waiting for or throw a panic, so there is no need to check return values
func (c *Client) dispatchAndWait(msgType uint32, msgData interface{}) interface{} {
	// Never closed here: dispatchMessage closes it when the client is closing,
	// a second close would replace the RemoteError below with a runtime panic
	waiter := make(chan *ProtocolMessage, 1)
	c.dispatchMessage(msgType, msgData, waiter, nil)
	return c.waitReply(waiter)
}

// waitReply waits for the reply of a dispatched message, see dispatchAndWait.
func (c *Client) waitReply(waiter chan *ProtocolMessage) interface{} {
	select {
	case reply, ok := <-waiter:
		if !ok {
			panic(RemoteError{errors.New("server disconnected while waiting for a response")})
		}
		if e, isError := reply.Data.(*MsgServerError); isError {
			panic(RemoteError{errors.New("Received error from server: " + string(e.ErrorMessage))})
		}
		return reply.Data
	case err := <-c.errorChannel:
		if c.lastError != nil {
			panic(c.lastError)
		}
		AbortOnError(err)
		panic(RemoteError{errors.New("connection was closed while waiting for a response")})
	}
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

// MissingBlocks returns the blocks in blockIDs that the server does not have.
// All checks are sent before the first reply is read, so they cost about one round trip together.
func (c *Client) MissingBlocks(blockIDs []Byte128) (missing []Byte128) {
	waiters := make([]chan *ProtocolMessage, len(blockIDs))
	for i, blockID := range blockIDs {
		waiters[i] = make(chan *ProtocolMessage, 1)
		c.dispatchMessage(MsgTypeAllocateBlock, &MsgClientAllocateBlock{BlockID: blockID}, waiters[i], nil)
	}
	for i, blockID := range blockIDs {
		if _, ok := c.waitReply(waiters[i]).(*MsgServerReadBlock); ok {
			missing = append(missing, blockID)
		}
	}
	return missing
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
			panic(errors.New("connection closed"))
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
	defer releaseOnPanic(b.Block) // corrupt data, the block is ours to free
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

// binary.BigEndian.Get and binary.BigEndian.Put  much faster than
// binary.Read and binary.Write

func (c *Client) GetStats() (tranismitted int32, skipped int32, queued int32, queuesize int64) {
	return atomic.LoadInt32(&c.transmittedBlocks), atomic.LoadInt32(&c.skippedBlocks), atomic.LoadInt32(&c.blockQueueLen), atomic.LoadInt64(&c.blockQueueSize)
}
