//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const DEFAULT_MAX_QUEUE_MEMORY int = 32 * 1024 * 1024 // 32 MB max memory

type messageDispatch struct {
	msg           *ProtocolMessage
	returnChannel chan interface{}
}

type Client struct {
	Session
	AccessKey Byte128 // = hmac^20000( AccountName "*ACCESS*KEY*PAD*", md5( password ))

	conn  io.ReadWriteCloser
	wg    sync.WaitGroup
	Paint bool

	QueueMax int // max size of the outgoing block queue (in bytes) (limited to 32bit = 4GB)

	sendMutex sync.Mutex // protects from two threads sending at the same time

	// mutex protected
	dispatchMutex     sync.Mutex
	msgNum            uint32 // 32-bit because there are no 16-bit atomic functions
	closing           bool
	blockbuffer       map[Byte128]*HashboxBlock
	blockqueuesize    int // queue size in bytes
	transmittedBlocks int32
	skippedBlocks     int32

	sendqueue   []*HashboxBlock
	sendworkers int32

	WriteData           int64
	WriteDataCompressed int64

	handlerSignal chan error

	dispatchChannel chan *messageDispatch
	storeChannel    chan *messageDispatch
}

func NewClient(conn io.ReadWriteCloser, account string, accesskey Byte128) *Client {

	client := &Client{
		conn:      conn,
		AccessKey: accesskey,
		Session: Session{
			AccountNameH: Hash([]byte(account)),
		},
		blockbuffer: make(map[Byte128]*HashboxBlock),

		QueueMax: DEFAULT_MAX_QUEUE_MEMORY,

		dispatchChannel: make(chan *messageDispatch, 80000),
		storeChannel:    make(chan *messageDispatch, 1),
	}
	client.wg.Add(1)
	client.handlerSignal = make(chan error, 1)
	go client.ioHandler()

	{ // Say hello
		r := client.dispatchAndWait(MsgTypeGreeting, nil).(*MsgServerGreeting)
		client.SessionNonce = r.SessionNonce
		client.GenerateSessionKey(client.AccessKey)
	}

	{ // Authenticate
		client.dispatchAndWait(MsgTypeAuthenticate, &MsgClientAuthenticate{
			AccountNameH:    client.AccountNameH,
			AuthenticationH: DeepHmac(1, client.AccountNameH[:], client.SessionKey),
		})
	}

	return client
}

func (c *Client) Close() {
	c.dispatchAndWait(MsgTypeGoodbye, nil)

	c.dispatchMutex.Lock()
	if !c.closing {
		c.closing = true
		close(c.dispatchChannel)
		close(c.storeChannel)
	}
	c.dispatchMutex.Unlock()

	c.conn.Close() // This will cancel a blocking IO-read if we have one
	c.wg.Wait()
}

func (c *Client) sendQueue(what Byte128) {
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	block := c.blockbuffer[what]
	if block != nil {
		c.sendqueue = append(c.sendqueue, block)

		if c.sendworkers < int32(runtime.NumCPU()) {
			atomic.AddInt32(&c.sendworkers, 1)
			go func() {
				for {
					var reply *HashboxBlock

					c.dispatchMutex.Lock()
					if len(c.sendqueue) > 0 {
						reply, c.sendqueue = c.sendqueue[0], c.sendqueue[1:]
					}
					c.dispatchMutex.Unlock()

					if reply != nil {
						reply.EncodeData()
						atomic.AddInt64(&c.WriteData, reply.DecodedSize())
						atomic.AddInt64(&c.WriteDataCompressed, reply.EncodedSize())
						atomic.AddInt32(&c.transmittedBlocks, 1) //	c.transmittedBlocks++
						if c.Paint {
							fmt.Print("*")
						}

						msg := &ProtocolMessage{Num: uint16(atomic.AddUint32(&c.msgNum, 1) - 1), Type: MsgTypeWriteBlock, Data: &MsgClientWriteBlock{Block: reply}}
						c.storeChannel <- &messageDispatch{msg: msg}
					} else {
						break
					}
				}
				atomic.AddInt32(&c.sendworkers, -1)
			}()
		}
	}
}

func (c *Client) singleExchange(outgoing *messageDispatch) *ProtocolMessage {
	// Send an outgoing message
	WriteMessage(c.conn, outgoing.msg)

	// Wait for the reply
	incoming := ReadMessage(c.conn)
	if incoming.Num != outgoing.msg.Num {
		panic(errors.New("ASSERT! Jag kan inte programmera"))
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
		if c.blockbuffer[d.BlockID] != nil {
			if c.blockbuffer[d.BlockID].encodedData == nil { // no encoded data = never sent
				skipped = true
			}
			c.blockqueuesize -= len(c.blockbuffer[d.BlockID].Data)
			delete(c.blockbuffer, d.BlockID)
		}
		c.dispatchMutex.Unlock()
		runtime.GC()

		if skipped {
			atomic.AddInt32(&c.skippedBlocks, 1) //c.skippedBlocks++
			if c.Paint {
				fmt.Print("-")
			}
		}
	case *MsgServerReadBlock:
		c.sendQueue(d.BlockID)
	}
	return incoming
}

func (c *Client) ioHandler() {
	defer func() {
		if r := recover(); !c.closing && r != nil { // a panic was raised inside the goroutine
			fmt.Println("ioHandler", r)
			c.handlerSignal <- r.(error)
			close(c.handlerSignal)
		}

		c.dispatchMutex.Lock()
		if !c.closing {
			c.closing = true
			close(c.dispatchChannel)
			close(c.storeChannel)
		}
		c.dispatchMutex.Unlock()

		c.wg.Done()
	}()

	for {
		select {
		case outgoing, ok := <-c.storeChannel:
			if !ok {
				return
			}
			c.singleExchange(outgoing)
		default:
			select {
			case outgoing, ok := <-c.dispatchChannel:
				if !ok {
					return
				}
				c.singleExchange(outgoing)
			default:
				continue
			}
		}
	}
}

// dispatchMessage returns a result channel if a returnChannel was specified, otherwise it just returns nil
func (c *Client) dispatchMessage(msgType uint32, msgData interface{}, returnChannel chan interface{}) {
	defer func() {
		if r := recover(); !c.closing && r != nil { // a panic was raised inside the goroutine
			err, _ := <-c.handlerSignal
			if err != nil {
				panic(err)
			} else {
				panic(r)
			}
		}
	}()

	if !c.closing {
		msg := &ProtocolMessage{Num: uint16(atomic.AddUint32(&c.msgNum, 1) - 1), Type: msgType, Data: msgData}
		c.dispatchChannel <- &messageDispatch{msg: msg, returnChannel: returnChannel}
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
		fmt.Println("här", R)
	case err := <-c.handlerSignal:
		if err != nil {
			panic(err)
		}
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

// StoreBlock is blocking if the blockbuffer is full
func (c *Client) StoreBlock(dataType byte, data []byte, links []Byte128) Byte128 {
	// Calculate the BlockID
	block := NewHashboxBlock(dataType, data, links)

	// Add the block to the io queue
	for full := true; full; { //
		func() {
			c.dispatchMutex.Lock()
			defer c.dispatchMutex.Unlock() // I do like this because if I have a failure on anything, it will still release the mutex
			if c.closing {
				panic(errors.New("Connection closed"))
			} else if c.blockqueuesize < c.QueueMax {
				if c.blockbuffer[block.BlockID] == nil {
					c.blockbuffer[block.BlockID] = block
					c.blockqueuesize += len(block.Data)
				}
				full = false
			}
		}()

		if full {
			time.Sleep(25 * time.Millisecond)
		}
	}

	// Put an allocate block on the line
	c.dispatchMessage(MsgTypeAllocateBlock, &MsgClientAllocateBlock{BlockID: block.BlockID}, nil)
	return block.BlockID
}
func (c *Client) ReadBlock(blockID Byte128) *HashboxBlock {
	b := c.dispatchAndWait(MsgTypeReadBlock, &MsgClientReadBlock{BlockID: blockID}).(*MsgServerWriteBlock)
	b.Block.DecodeData()
	return b.Block
}
func (c *Client) Commit() {
	for empty := false; !empty; time.Sleep(100 * time.Millisecond) {
		func() {
			c.dispatchMutex.Lock()
			empty = c.closing || len(c.blockbuffer) == 0
			c.dispatchMutex.Unlock()
		}()
	}
}

const hashPadding_accesskey = "*ACCESS*KEY*PAD*" // TODO: move to client source

// binary.BigEndian.Get and binary.BigEndian.Put  much faster than
// binary.Read and binary.Write

func (c *Client) GetStats() (tranismitted int32, skipped int32, queued int32, queuesize int32) {
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	return c.transmittedBlocks, c.skippedBlocks, int32(len(c.blockbuffer)), int32(c.blockqueuesize)
}
