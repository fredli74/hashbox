//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	//"github.com/davecheney/profile"

	"bitbucket.org/fredli74/bytearray"
	cmd "bitbucket.org/fredli74/cmdparser"
	"bitbucket.org/fredli74/hashbox/core"
	"bitbucket.org/fredli74/lockfile"

	"github.com/kardianos/osext"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"
)

var Version = "(dev-build)"

const DEFAULT_SERVER_IP_PORT int = 7411

var datDirectory string
var idxDirectory string
var accountHandler *AccountHandler
var storageHandler *StorageHandler

var done chan bool

var logLock sync.Mutex

var DEBUG bool = false

func PanicOn(err error) {
	if err != nil {
		panic(err)
	}
}

func Debug(format string, a ...interface{}) {
	if DEBUG {
		fmt.Print("DEBUG: ")
		fmt.Printf(format, a...)
		fmt.Println()
	}
}
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

		func() {
			incoming := core.ReadMessage(conn)
			defer incoming.Release()
			clientLog(remoteID, "< "+incoming.String()+" "+incoming.Details())
			reply := &core.ProtocolMessage{Num: incoming.Num, Type: incoming.Type & core.MsgTypeServerMask}
			defer reply.Release()

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
			case core.MsgTypeGoodbye:
				keepAlive = false
			default:
				unauthorized = !sessionAuthenticated
				if !unauthorized {
					switch incoming.Type {
					case core.MsgTypeAddDatasetState:
						c := incoming.Data.(*core.MsgClientAddDatasetState)
						unauthorized = c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
						if !unauthorized {
							if !storageHandler.doesBlockExist(c.State.BlockID) {
								reply.Type = core.MsgTypeError & core.MsgTypeServerMask
								reply.Data = &core.MsgServerError{"Dataset pointing to a non existent block"}
							} else {
								accountHandler.AddDatasetState(c.AccountNameH, c.DatasetName, c.State)
								// No need to set any data in reply
							}
						}
					case core.MsgTypeRemoveDatasetState:
						c := incoming.Data.(*core.MsgClientRemoveDatasetState)
						unauthorized = c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
						if !unauthorized {
							accountHandler.RemoveDatasetState(c.AccountNameH, c.DatasetName, c.StateID)
							// No need to set any data in reply
						}
					case core.MsgTypeListDataset:
						c := incoming.Data.(*core.MsgClientListDataset)
						unauthorized = c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
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
						unauthorized = c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
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
						if c.Block.VerifyBlock() {
							for _, l := range c.Block.Links {
								if !storageHandler.doesBlockExist(l) {
									reply.Type = core.MsgTypeError & core.MsgTypeServerMask
									reply.Data = &core.MsgServerError{"Linked to non existant block"}
									break
								}
							}
							if reply.Data == nil {
								storageHandler.writeBlock(c.Block)
								reply.Type = core.MsgTypeAcknowledgeBlock & core.MsgTypeServerMask
								reply.Data = &core.MsgServerAcknowledgeBlock{BlockID: c.Block.BlockID}
							}
						} else {
							reply.Type = core.MsgTypeError & core.MsgTypeServerMask
							reply.Data = &core.MsgServerError{"Unable to verify blockID"}
						}
					default:
						panic(errors.New("ASSERT: Well if we reach this point, we have not implemented everything correctly (missing " + incoming.String() + ")"))
					}
				}
			}

			if unauthorized {
				// Invalid authentication
				// TODO: Add to server auth.log
				reply.Type = core.MsgTypeError & core.MsgTypeServerMask
				reply.Data = &core.MsgServerError{"Invalid authentication"}
				keepAlive = false
			}
			clientLog(remoteID, "> "+reply.String()+" "+reply.Details())
			core.WriteMessage(conn, reply)
		}()
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
	bytearray.EnableAutoGC(60, 74)

	//defer profile.Start(&profile.Config{CPUProfile: false, MemProfile: true, ProfilePath: ".", NoShutdownHook: true}).Stop()

	/*defer func() {
		// Panic error handling
		if r := recover(); r != nil {
			fmt.Println(r)
			os.Exit(1)
		}
	}()*/

	var err error

	exeRoot, _ := osext.ExecutableFolder()

	var serverPort int64 = int64(DEFAULT_SERVER_IP_PORT)
	datDirectory = filepath.Join(exeRoot, "data")
	idxDirectory = datDirectory

	cmd.Title = fmt.Sprintf("Hashbox Server %s", Version)
	cmd.IntOption("port", "", "<port>", "Server listening port", &serverPort, cmd.Standard)
	cmd.StringOption("data", "", "<path>", "Full path to data files", &datDirectory, cmd.Standard).OnChange(func() {
		idxDirectory = datDirectory
	})
	idxSpecial := ""
	cmd.StringOption("idx", "", "<path>", "Full path to idx files (default same as data path)", &idxSpecial, cmd.Standard).OnChange(func() {
		idxDirectory = idxSpecial
	})
	cmd.BoolOption("debug", "", "Debug output", &DEBUG, cmd.Hidden)

	// Please note that datPath has not been set until we have parsed arguments, that is ok because neither of the handlers
	// start opening files on their own
	// TODO: remove datPath global and send them into handlers on creation instead
	accountHandler = NewAccountHandler()
	defer accountHandler.Close()
	storageHandler = NewStorageHandler()
	defer storageHandler.Close()

	cmd.Command("", "", func() { // Default
		serverAddr := net.TCPAddr{nil, int(serverPort), ""}

		if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
			panic(err)
		} else {
			defer lock.Unlock()
		}

		var listener *net.TCPListener
		if listener, err = net.ListenTCP("tcp", &serverAddr); err != nil {
			fmt.Println("Error listening: ", err.Error())
			os.Exit(1)
		}
		serverLog(cmd.Title, "is listening on", listener.Addr().String())

		done = make(chan bool)
		defer close(done)

		signalchan := make(chan os.Signal, 1)
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

		go func() {
			var lastStats string
			for { // ever
				time.Sleep(10 * time.Second)
				s := core.MemoryStats()
				if s != lastStats {
					fmt.Println(s)
					lastStats = s
				}
			}
		}()

		// blocking channel read
		select {
		case <-done:
		case <-accountHandler.signal:
		case <-storageHandler.signal:
		}

		serverLog("Hashbox Server terminating")
	})

	// TODO: This is a temporary hack to allow creation of hashback users on the server side
	// It should be an interface to an adminsitrative tool instead
	cmd.Command("adduser", "<username> <password>", func() {
		if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
			panic(err)
		} else {
			defer lock.Unlock()
		}

		if len(cmd.Args) < 4 {
			panic(errors.New("Missing argument to adduser command"))
		}

		if (!accountHandler.SetInfo(AccountInfo{AccountName: core.String(cmd.Args[2]), AccessKey: core.GenerateAccessKey(cmd.Args[2], cmd.Args[3])})) {
			panic(errors.New("Error creating account"))
		}
		accountNameH := core.Hash([]byte(cmd.Args[2]))
		dataEncryptionKey := core.GenerateDataEncryptionKey()
		Debug("DataEncryptionKey is: %x", dataEncryptionKey)
		core.EncryptDataInPlace(dataEncryptionKey[:], core.GenerateBackupKey(cmd.Args[2], cmd.Args[3]))

		var blockData bytearray.ByteArray
		blockData.Write(dataEncryptionKey[:])
		block := core.NewHashboxBlock(core.BlockDataTypeRaw, blockData, nil)
		if !storageHandler.writeBlock(block) {
			panic(errors.New("Error writing key block"))
		}
		err := accountHandler.AddDatasetState(accountNameH, core.String("\x07HASHBACK_DEK"), core.DatasetState{BlockID: block.BlockID})
		PanicOn(err)

		block.Release()
		fmt.Println("User added")
	})

	var doRepair bool
	var skipData, skipMeta, skipIndex bool
	cmd.BoolOption("repair", "check-storage", "Try to repair non-fatal errors", &doRepair, cmd.Standard)
	cmd.BoolOption("skipdata", "check-storage", "Skip checking data files", &skipData, cmd.Standard)
	cmd.BoolOption("skipmeta", "check-storage", "Skip checking meta files", &skipMeta, cmd.Standard)
	cmd.BoolOption("skipindex", "check-storage", "Skip checking index files", &skipIndex, cmd.Standard)
	cmd.Command("check-storage", "", func() {
		if doRepair {
			if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
				panic(err)
			} else {
				defer lock.Unlock()
			}
		}

		start := time.Now()

		fmt.Println("Checking all storage files")
		repaired, critical := storageHandler.CheckFiles(doRepair)
		if !skipData && repaired == 0 && critical == 0 {
			fmt.Println("Checking data files")
			repaired, critical = storageHandler.CheckData(doRepair)
		}
		if !skipMeta && repaired == 0 && critical == 0 {
			fmt.Println("Checking meta files")
			repaired, critical = storageHandler.CheckMeta(doRepair)
		}

		if !skipIndex && repaired == 0 && critical == 0 {
			fmt.Println("Checking index files")
			storageHandler.CheckIndexes()
		}

		if repaired == 0 && critical == 0 {
			fmt.Println("Checking dataset transactions")
			roots := accountHandler.CollectAllRootBlocks()
			fmt.Println("Checking block chain integrity")
			storageHandler.CheckChain(roots, true)
		}

		if critical > 0 {
			fmt.Printf("Detected %d critical errors, DO NOT start the server unless everything is repaired\n", critical)
		}
		if repaired > 0 {
			fmt.Printf("Performed %d repairs, please run again to verify repairs\n", repaired)
		}
		if critical == 0 && repaired == 0 {
			fmt.Printf("All checks completed successfully in %.1f minutes\n\n", time.Since(start).Minutes())
		}
	})

	var indexOnly bool
	cmd.BoolOption("index", "gc", "Mark and sweep index only", &indexOnly, cmd.Standard)
	cmd.Command("gc", "", func() {
		if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
			panic(err)
		} else {
			defer lock.Unlock()
		}

		start := time.Now()
		fmt.Println("Marking index entries")
		roots := accountHandler.CollectAllRootBlocks()
		storageHandler.MarkIndexes(roots, true)
		storageHandler.SweepIndexes(true)
		fmt.Printf("Mark and sweep duration %.1f minutes\n", time.Since(start).Minutes())
		storageHandler.ShowStorageDeadSpace()
		if !indexOnly {
			storageHandler.CompactAll(storageFileTypeData)
			storageHandler.CompactAll(storageFileTypeMeta)
		}
		fmt.Printf("Garbage collection completed in %.1f minutes\n\n", time.Since(start).Minutes())
	})

	err = cmd.Parse()
	PanicOn(err)

	fmt.Println(core.MemoryStats())

	os.Exit(0)
}
