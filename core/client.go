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
	_ "runtime"
	"sync"
	"time"
)

type Client struct {
	Session
	AccessKey Byte128 // = hmac^20000( AccountName "*ACCESS*KEY*PAD*", md5( password ))

	conn  io.ReadWriteCloser
	wg    sync.WaitGroup
	Paint bool

	sendMutex sync.Mutex // protects from two threads sending at the same time

	// mutex protected
	dispatchMutex     sync.Mutex
	msgNum            uint16 // 16-bit because it does not matter if it wraps
	waitlist          map[uint16]chan interface{}
	closing           bool
	blockqueue        map[Byte128]*HashboxBlock
	transmittedBlocks int
	skippedBlocks     int
}

func (c *Client) GetStats() (tranismitted int, skipped int, queued int) {
	c.dispatchMutex.Lock()
	defer c.dispatchMutex.Unlock()
	return c.transmittedBlocks, c.skippedBlocks, len(c.blockqueue)
}

// The ioHandler is started as a goroutine for the blocking IO reads
func (c *Client) ioHandler() {
	defer func() {
		c.dispatchMutex.Lock()
		if r := recover(); !c.closing && r != nil { // a panic was raised inside the goroutine
			for _, w := range c.waitlist {
				w <- r
			}
		}
		for _, w := range c.waitlist {
			close(w)
		}
		c.closing = true
		c.dispatchMutex.Unlock()
		c.wg.Done()
	}()

	for {
		msg := ReadMessage(c.conn) // Blocking read protocol message

		c.dispatchMutex.Lock()
		waiter := c.waitlist[msg.Num]
		delete(c.waitlist, msg.Num)
		c.dispatchMutex.Unlock()

		if waiter != nil {
			waiter <- msg
		}

		// Handle block queue
		switch d := msg.Data.(type) {
		case *MsgServerAcknowledgeBlock:
			func() {
				c.dispatchMutex.Lock()
				defer c.dispatchMutex.Unlock()
				if c.blockqueue[d.BlockID] == nil {
					c.skippedBlocks++
					if c.Paint {
						fmt.Print("-")
					}
				} else {
					delete(c.blockqueue, d.BlockID)
				}
			}()
		case *MsgServerReadBlock:
			if c.Paint {
				fmt.Print("*")
			}
			var reply *HashboxBlock
			func() {
				c.dispatchMutex.Lock()
				defer c.dispatchMutex.Unlock()
				c.transmittedBlocks++
				reply = c.blockqueue[d.BlockID]
				delete(c.blockqueue, d.BlockID)
			}()
			if reply != nil {
				c.dispatchMessage(MsgTypeWriteBlock, &MsgClientWriteBlock{Block: reply}, nil)
			}
		}
	}
}

// dispatchMessage returns a result channel if a waitChannel was specified, otherwise it just returns nil
func (c *Client) dispatchMessage(msgType uint32, msgData interface{}, waitChannel chan interface{}) {
	c.sendMutex.Lock()
	defer c.sendMutex.Unlock()

	var msg *ProtocolMessage
	func() {
		c.dispatchMutex.Lock()
		defer c.dispatchMutex.Unlock()
		if !c.closing {
			msg = &ProtocolMessage{Num: c.msgNum, Type: msgType, Data: msgData}
			c.msgNum++
			if waitChannel != nil {
				c.waitlist[msg.Num] = waitChannel
			}
		} else if waitChannel != nil {
			close(waitChannel)
		}
	}()
	if msg != nil {
		WriteMessage(c.conn, msg)
	}
}

// dispatchAndWait will always return the response you were waiting for or throw a panic, so there is no need to check return values
func (c *Client) dispatchAndWait(msgType uint32, msgData interface{}) interface{} {
	waiter := make(chan interface{}, 1)
	c.dispatchMessage(msgType, msgData, waiter)
	R := <-waiter

	switch t := R.(type) {
	case *ProtocolMessage:
		switch dt := t.Data.(type) {
		case *MsgServerError:
			panic(errors.New("Received error from server: " + string(dt.ErrorMessage)))
		default:
			return t.Data
		}
	case error:
		panic(t) // errors.New(t)"Server experienced an error and closed the connection"))
	case nil:
		panic(errors.New("Connection was closed while waiting for a response"))
	default:
		panic("ASSERT! We should not reach this point")
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

// StoreBlock is blocking if the blockqueue is full
func (c *Client) StoreBlock(data []byte, links []Byte128) Byte128 {
	// Calculate the BlockID
	block := HashboxBlock{Links: links, Data: data}
	block.BlockID = block.HashData()

	// Add the block to the io queue
	for full := true; full; { //
		func() {
			c.dispatchMutex.Lock()
			defer c.dispatchMutex.Unlock() // I do like this because if I have a failure on anything, it will still release the mutex
			if c.closing {
				panic(errors.New("Connection closed"))
			} else if len(c.blockqueue) < MAX_BLOCKS_IN_PIPE {
				if c.blockqueue[block.BlockID] == nil {
					c.blockqueue[block.BlockID] = &block
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
	return b.Block
}
func (c *Client) Commit() {
	for empty := false; !empty; time.Sleep(100 * time.Millisecond) {
		func() {
			c.dispatchMutex.Lock()
			empty = c.closing || len(c.blockqueue) == 0
			c.dispatchMutex.Unlock()
		}()
	}
}

func (c *Client) Close() {
	c.dispatchAndWait(MsgTypeGoodbye, nil)
	c.dispatchMutex.Lock()
	c.closing = true
	c.dispatchMutex.Unlock()
	c.conn.Close()
	c.wg.Wait()
}

func NewClient(conn io.ReadWriteCloser, account string, accesskey Byte128) *Client {
	client := &Client{
		conn:      conn,
		AccessKey: accesskey,
		Session: Session{
			AccountNameH: Hash([]byte(account)),
		},
		waitlist:   make(map[uint16]chan interface{}),
		blockqueue: make(map[Byte128]*HashboxBlock),
	}
	client.wg.Add(1)
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

const hashPadding_accesskey = "*ACCESS*KEY*PAD*" // TODO: move to client source

// binary.BigEndian.Get and binary.BigEndian.Put  much faster than
// binary.Read and binary.Write
