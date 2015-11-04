//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	cmd "bitbucket.org/fredli74/cmdparser"
	"bitbucket.org/fredli74/hashbox/core"

	"bitbucket.org/kardianos/osext"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"
)

var datDirectory string
var accountHandler *AccountHandler
var storageHandler *StorageHandler

var done chan bool

var logLock sync.Mutex

func clientLog(v ...interface{}) {
	logLock.Lock()
	fmt.Print("C: ")
	fmt.Println(v...)
	logLock.Unlock()
}
func serverLog(v ...interface{}) {
	logLock.Lock()
	fmt.Print("S: ")
	fmt.Println(v...)
	logLock.Unlock()
}

type HardError string

func (e HardError) Error() string { return string(e) }

func panicHard(s string) {
	var err error = HardError(s)
	panic(err)
}

func handleConnection(conn net.Conn) {
	remoteID := conn.RemoteAddr().String()
	clientLog(remoteID, "= Connection established")

	defer func() {
		if err := recover(); err != nil {
			clientLog(remoteID, "! ", err)
		}

		clientLog(remoteID, "= Connection closed")
		conn.Close()
	}()

	var clientSession core.Session
	var sessionAuthenticated = false

	for keepAlive := true; keepAlive; {
		conn.SetReadDeadline(time.Now().Add(10 * time.Minute)) // max size 512kb over 10 minutes is slower than 9600bps so we should be safe
		// TODO: testing non-idle of 10s between commands

		unauthorized := false

		incoming := core.ReadMessage(conn)
		clientLog(remoteID, "< "+incoming.String()+" "+incoming.Details())
		reply := core.ProtocolMessage{Num: incoming.Num, Type: incoming.Type & core.MsgTypeServerMask}

		switch incoming.Type {
		case core.MsgTypeGreeting:
			// Create server nonce using  64-bit time and 64-bit random
			binary.BigEndian.PutUint64(clientSession.SessionNonce[0:], uint64(time.Now().UnixNano()))
			binary.BigEndian.PutUint32(clientSession.SessionNonce[8:], rand.Uint32())
			binary.BigEndian.PutUint32(clientSession.SessionNonce[12:], rand.Uint32())
			reply.Data = &core.MsgServerGreeting{clientSession.SessionNonce}
		case core.MsgTypeAuthenticate:
			c := incoming.Data.(*core.MsgClientAuthenticate)
			clientSession.AccountNameH = c.AccountNameH
			account := accountHandler.GetInfo(clientSession.AccountNameH)
			if account != nil {
				clientSession.GenerateSessionKey(account.AccessKey)
				myAuthenticationH := core.DeepHmac(1, c.AccountNameH[:], clientSession.SessionKey)
				sessionAuthenticated = bytes.Equal(myAuthenticationH[:], c.AuthenticationH[:])
			}
			unauthorized = !sessionAuthenticated
			// No need to set any data in reply
		case core.MsgTypeAddDatasetState:
			c := incoming.Data.(*core.MsgClientAddDatasetState)
			unauthorized = !sessionAuthenticated || c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
			if !unauthorized {
				// TODO: Need to check that the root BlockID in state exists
				accountHandler.AddDatasetState(c.AccountNameH, c.DatasetName, c.State)
				// No need to set any data in reply
			}
		case core.MsgTypeRemoveDatasetState:
			c := incoming.Data.(*core.MsgClientRemoveDatasetState)
			unauthorized = !sessionAuthenticated || c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
			if !unauthorized {
				accountHandler.RemoveDatasetState(c.AccountNameH, c.DatasetName, c.StateID)
				// No need to set any data in reply
			}
		case core.MsgTypeListDataset:
			c := incoming.Data.(*core.MsgClientListDataset)
			unauthorized = !sessionAuthenticated || c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
			if !unauthorized {
				list := accountHandler.ListDataset(c.AccountNameH, c.DatasetName)
				if list == nil {
					list = new(dbStateCollection)
				}
				reply.Data = &core.MsgServerListDataset{States: list.States, ListH: list.ListH}
				//				} else {
				//					reply.Type = core.MsgTypeError & core.MsgTypeServerMask
				//					reply.Data = &core.MsgServerError{"Cannot list dataset named " + c.DatasetName}

			}
		case core.MsgTypeAccountInfo:
			c := incoming.Data.(*core.MsgClientAccountInfo)
			unauthorized = !sessionAuthenticated || c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
			if !unauthorized {
				account := accountHandler.GetInfo(c.AccountNameH)
				if account != nil {
					reply.Data = &core.MsgServerAccountInfo{DatasetList: account.Datasets}
				} else {
					reply.Type = core.MsgTypeError & core.MsgTypeServerMask
					reply.Data = &core.MsgServerError{"Cannot find account information"}
				}
			}
		case core.MsgTypeAllocateBlock:
			c := incoming.Data.(*core.MsgClientAllocateBlock)
			if storageHandler.doesBlockExist(c.BlockID) {
				reply.Type = core.MsgTypeAcknowledgeBlock & core.MsgTypeServerMask
				reply.Data = &core.MsgServerAcknowledgeBlock{BlockID: c.BlockID}
			} else {
				reply.Type = core.MsgTypeReadBlock & core.MsgTypeServerMask
				reply.Data = &core.MsgServerReadBlock{BlockID: c.BlockID}
			}
		case core.MsgTypeReadBlock:
			c := incoming.Data.(*core.MsgClientReadBlock)
			block := storageHandler.readBlock(c.BlockID)
			if block == nil {
				reply.Type = core.MsgTypeError & core.MsgTypeServerMask
				reply.Data = &core.MsgServerError{"Invalid blockID"}
				keepAlive = false
			} else {
				reply.Type = core.MsgTypeWriteBlock & core.MsgTypeServerMask
				reply.Data = &core.MsgServerWriteBlock{Block: block}
			}
		case core.MsgTypeWriteBlock:
			c := incoming.Data.(*core.MsgClientWriteBlock)
			storageHandler.writeBlock(c.Block)
			reply.Type = core.MsgTypeAcknowledgeBlock & core.MsgTypeServerMask
			reply.Data = &core.MsgServerAcknowledgeBlock{BlockID: c.Block.BlockID}
		case core.MsgTypeGoodbye:
			keepAlive = false

		default:
			panic(errors.New("ASSERT: Well if we reach this point, we have not implemented everything correctly (missing " + incoming.String() + ")"))
		}

		if unauthorized {
			// Invalid authentication
			// TODO: Add to server auth.log
			reply.Type = core.MsgTypeError & core.MsgTypeServerMask
			reply.Data = &core.MsgServerError{"Invalid authentication"}
			keepAlive = false
		}
		clientLog(remoteID, "> "+reply.String()+" "+reply.Details())
		core.WriteMessage(conn, &reply)
	}
}

func connectionListener(listener *net.TCPListener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			serverLog("Closed network connection")
			done <- true
			return
		}
		go handleConnection(conn)
	}
}

func main() {
	defer func() {
		// Panic error handling
		if r := recover(); r != nil {
			fmt.Println(r)
			os.Exit(1)
		}
	}()

	var err error

	exeRoot, _ := osext.ExecutableFolder()

	datDirectory = *flag.String("db", filepath.Join(exeRoot, "data"), "Full path to database files")

	cmd.Title = "Hashbox Server 0.1-go"
	cmd.AddOption("port", "", "Server listening port", int64(core.DEFAULT_SERVER_IP_PORT), cmd.Standard)
	cmd.AddOption("db", "", "Full path to database files", filepath.Join(exeRoot, "data"), cmd.Standard)

	// Please note that datPath has not been set until we have parsed arguments, that is ok because neither of the handlers
	// start opening files on their own
	// TODO: remove datPath global and send them into handlers on creation instead
	accountHandler = NewAccountHandler()
	defer accountHandler.Close()
	storageHandler = NewStorageHandler()
	defer storageHandler.Close()

	cmd.AddCommand("", "", func() {
		datDirectory = cmd.Option["db"].String()

		// Default

		serverAddr := net.TCPAddr{nil, int(cmd.Option["port"].Value.(int64)), ""}

		var listener *net.TCPListener
		if listener, err = net.ListenTCP("tcp", &serverAddr); err != nil {
			fmt.Println("Error listening: ", err.Error())
			os.Exit(1)
		}
		serverLog(cmd.Title, "is listening on", listener.Addr().String())

		done = make(chan bool)
		defer close(done)

		signalchan := make(chan os.Signal)
		defer close(signalchan)
		signal.Notify(signalchan, os.Interrupt)
		signal.Notify(signalchan, os.Kill)
		go func() {
			for s := range signalchan {
				serverLog("Received OS signal:", s)
				listener.Close()
				// done <- true
				return
			}
		}()

		go connectionListener(listener)

		// blocking channel read
		select {
		case <-done:
			fmt.Println("debug, server quit")
		case a, ok := <-accountHandler.signal:
			fmt.Println(a)
			fmt.Println(ok)
			fmt.Println("accounthandler quit")
		case a, ok := <-storageHandler.signal:
			fmt.Println(a)
			fmt.Println(ok)
			fmt.Println("storagehandler quit")
		}

		serverLog("Hashbox Server terminating")
	})

	// TODO: This is a temporary hack to allow creation of hashback users on the server side
	// It should be an interface to an adminsitrative tool instead
	cmd.AddCommand("adduser", "<username> <password>", func() {
		datDirectory = cmd.Option["db"].String()

		if len(cmd.Args) < 4 {
			panic(errors.New("Missing argument to adduser command"))
		}

		_ = "breakpoint"

		if (!accountHandler.SetInfo(AccountInfo{AccountName: core.String(cmd.Args[2]), AccessKey: core.GenerateAccessKey(cmd.Args[2], cmd.Args[3])})) {
			panic(errors.New("Error creating account"))
		}
		accountNameH := core.Hash([]byte(cmd.Args[2]))
		dataEncryptionKey := core.GenerateDataEncryptionKey()
		fmt.Printf("DEBUG: DataEncryptionKey is: %x\n", dataEncryptionKey)
		core.EncryptDataInPlace(dataEncryptionKey[:], core.GenerateBackupKey(cmd.Args[2], cmd.Args[3]))
		block := core.HashboxBlock{Data: dataEncryptionKey[:]}
		block.BlockID = block.HashData()
		if !storageHandler.writeBlock(&block) {
			panic(errors.New("Error writing key block"))
		}
		if err := accountHandler.AddDatasetState(accountNameH, core.String("\x07HASHBACK_DEK"), core.DatasetState{BlockID: block.BlockID}); err != nil {
			panic(err)
		}
		fmt.Println("User added")
	})

	if err := cmd.Parse(); err != nil {
		panic(err)
	}

	os.Exit(0)
}
