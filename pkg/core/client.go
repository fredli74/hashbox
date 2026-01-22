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

	QueueMax  int64 // max size of the outgoing block queue (in bytes)
	ThreadMax int32 // maximum number of goroutines started by send queue (defaults to runtime.NumCPU)
	Retry     bool          // retry on network errors/EOF
	RetryMax  int           // max retry attempts; -1 means retry forever, 0 disables retries
	RetryWait time.Duration // interval between retries
	retryCount int          // total retries this session

	sendMutex sync.Mutex // protects from two threads sending at the same time

	// mutex protected
	dispatchMutex  sync.Mutex
	closing        bool
	blockbuffer    map[Byte128]*HashboxBlock
	blockqueuesize int64 // queue size in bytes

	sendqueueMap map[Byte128]bool
	sendqueue    []*sendQueueEntry

	dispatchChannel chan *messageDispatch // input to goroutine; buffered list of outgoing messages to dispatch
	storeChannel    chan *messageDispatch // input to goroutine; next block in queue to store
	errorChannel    chan error            // output from goroutine; ioHandler encountered an error
	lastError       error                 // last error encountered
}

func NewClient(address string, account string, accesskey Byte128) *Client {

	client := &Client{
		ServerAddress: address,
		AccessKey:     accesskey,
		Session: Session{
			AccountNameH: Hash([]byte(account)),
		},
		blockbuffer:  make(map[Byte128]*HashboxBlock),
		sendqueueMap: make(map[Byte128]bool),

		QueueMax:  DEFAULT_QUEUE_SIZE,
		ThreadMax: int32(runtime.NumCPU() / 2),
		Retry:     true,
		RetryMax:  -1,
		RetryWait: 15 * time.Second,

		dispatchChannel: make(chan *messageDispatch, 1024),
		storeChannel:    make(chan *messageDispatch, 1),
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
		close(c.storeChannel)
	}
	c.dispatchMutex.Unlock()

	if connection := c.connection; connection != nil {
		connection.Close() // This will cancel a blocking IO-read if we have one
	}
	c.wg.Wait()
}

type sendQueueEntry struct {
	state int32 // (atomic alignment) 0 - new, 1 - compressing, 2 - compressed, 3 - sending, 4 - sent
	block *HashboxBlock
}

func (c *Client) sendQueue(what Byte128) {
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	block := c.blockbuffer[what]
	if block != nil && !c.sendqueueMap[what] {
		c.sendqueueMap[what] = true
		c.sendqueue = append(c.sendqueue, &sendQueueEntry{state: 0, block: block})
		//		fmt.Printf("+q=%d;", len(c.sendqueue))

		if c.sendworkers < 1 || c.sendworkers < c.ThreadMax {
			atomic.AddInt32(&c.sendworkers, 1)
			go func() {
				defer func() { // a panic was raised inside the goroutine (most likely the channel was closed)
					if r := recover(); !c.closing && r != nil {
						if c.lastError != nil {
							panic(c.lastError)
						}
						err, _ := <-c.errorChannel
						AbortOn(err)
						panic(r)
					}
				}()

				for done := false; !done; {
					var workItem *sendQueueEntry

					c.dispatchMutex.Lock()
					if len(c.sendqueue) > 0 {
						if c.sendqueue[0].state == 2 { // compressed
							workItem = c.sendqueue[0] // send it
						} else if c.sendqueue[0].state == 4 { // sent
							if c.sendqueue[0].block != nil {
								delete(c.sendqueueMap, c.sendqueue[0].block.BlockID)
							}
							c.sendqueue = c.sendqueue[1:] // remove it
							//							fmt.Printf("-q=%d;", len(c.sendqueue))
						} else {
							for i := 0; i < len(c.sendqueue); i++ {
								if c.sendqueue[i].state == 0 { // new
									workItem = c.sendqueue[i] // compress it
									break
								}
							}
						}
						if workItem != nil {
							atomic.AddInt32(&workItem.state, 1)
						}
					} else {
						done = true
						atomic.AddInt32(&c.sendworkers, -1)
						//						fmt.Println("worker stopping")
					}
					c.dispatchMutex.Unlock()

					if workItem != nil {
						switch workItem.state {
						case 0:
							panic("ASSERT!")
						case 1:
							if !workItem.block.Compressed {
								workItem.block.CompressData()
								diff := bytearray.ChunkQuantize(int64(workItem.block.UncompressedSize)) - bytearray.ChunkQuantize(int64(workItem.block.CompressedSize))
								atomic.AddInt64(&c.blockqueuesize, -diff)
							}
							atomic.AddInt32(&workItem.state, 1)
						case 2:
							panic("ASSERT!")
							case 3:
								atomic.AddInt64(&c.WriteData, int64(workItem.block.UncompressedSize))
								atomic.AddInt64(&c.WriteDataCompressed, int64(workItem.block.CompressedSize))
								atomic.AddInt32(&c.transmittedBlocks, 1) //	c.transmittedBlocks++
								c.Paint("*")
								Log(LogTrace, "Upload block %x (links=%d, size=%d, compressed=%d)", workItem.block.BlockID, len(workItem.block.Links), workItem.block.UncompressedSize, workItem.block.CompressedSize)
								msg := &ProtocolMessage{Type: MsgTypeWriteBlock, Data: &MsgClientWriteBlock{Block: workItem.block}}
								c.storeChannel <- &messageDispatch{msg: msg}
								atomic.AddInt32(&workItem.state, 1)
						default:
							panic("ASSERT!")
						}
					} else {
						time.Sleep(25 * time.Millisecond)
					}
				}
			}()
		}
	}
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
		close(outgoing.returnChannel)
	}

	// Handle block queue
	switch d := incoming.Data.(type) {
	case *MsgServerError:
		panic(errors.New("Received error from server: " + string(d.ErrorMessage)))
	case *MsgServerAcknowledgeBlock:
		var skipped bool = false

		c.dispatchMutex.Lock()
		block := c.blockbuffer[d.BlockID]
		if block != nil {
			if block.Compressed {
				c.blockqueuesize -= bytearray.ChunkQuantize(int64(block.CompressedSize))
			} else {
				// not compressed = never sent
				skipped = true
				c.blockqueuesize -= bytearray.ChunkQuantize(int64(block.UncompressedSize))
			}
			block.Release()
			delete(c.blockbuffer, d.BlockID)
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
				if (retry) {
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
						Log(LogError, e.Error())
						Log(LogError, fmt.Sprint(e))
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
			case outgoing, ok := <-c.storeChannel:
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
	} else {
		if returnChannel != nil {
			close(returnChannel)
		}
	}
}

// dispatchAndWait will always return the response you were waiting for or throw a panic, so there is no need to check return values
func (c *Client) dispatchAndWait(msgType uint32, msgData interface{}) interface{} {
	waiter := make(chan interface{}, 1)
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

// StoreBlock is blocking if the blockbuffer is full
func (c *Client) StoreBlock(block *HashboxBlock) Byte128 {
	// Add the block to the io queue
	for full := true; full; { //
		c.dispatchMutex.Lock()
		if c.closing {
			c.dispatchMutex.Unlock()
			panic(errors.New("Connection closed"))
		} else if c.blockbuffer[block.BlockID] != nil {
			block.Release()
			c.dispatchMutex.Unlock()
			return block.BlockID
		} else {
			var size int64
			if block.Compressed {
				size = bytearray.ChunkQuantize(int64(block.CompressedSize))
			} else {
				size = bytearray.ChunkQuantize(int64(block.UncompressedSize))
			}
			if c.blockqueuesize == 0 || c.blockqueuesize+size*2 < c.QueueMax {
				c.blockbuffer[block.BlockID] = block
				c.blockqueuesize += size
				full = false
			}
		}
		c.dispatchMutex.Unlock()

		if full {
			time.Sleep(25 * time.Millisecond)
		}
	}

	// Put an allocate block on the line
	waiter := make(chan interface{}, 1)
	c.dispatchMessage(MsgTypeAllocateBlock, &MsgClientAllocateBlock{BlockID: block.BlockID}, waiter)
	go func(id Byte128, rc <-chan interface{}) {
		if r, ok := <-rc; ok {
			if t, ok := r.(*ProtocolMessage); ok {
				if _, ok := t.Data.(*MsgServerReadBlock); ok {
					c.sendQueue(id)
				}
			}
		}
	}(block.BlockID, waiter)
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
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	return c.closing || len(c.blockbuffer) == 0
}

const hashPadding_accesskey = "*ACCESS*KEY*PAD*" // TODO: move to client source

// binary.BigEndian.Get and binary.BigEndian.Put  much faster than
// binary.Read and binary.Write

func (c *Client) GetStats() (tranismitted int32, skipped int32, queued int32, queuesize int64) {
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	return c.transmittedBlocks, c.skippedBlocks, int32(len(c.blockbuffer)), c.blockqueuesize
}
