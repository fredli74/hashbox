//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2016
//	+---+´

package main

import (
	//"github.com/davecheney/profile"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/fredli74/bytearray"
	cmd "github.com/fredli74/cmdparser"
	"github.com/fredli74/hashbox/core"
	"github.com/fredli74/lockfile"

	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kardianos/osext"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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

func PanicOn(err error) {
	if err != nil {
		panic(err)
	}
}
func Try(f func()) (err interface{}) {
	defer func() {
		err = recover()
	}()
	f()
	return nil
}

type HardError string

func (e HardError) Error() string { return string(e) }

func panicHard(s string) {
	var err error = HardError(s)
	panic(err)
}

func handleConnection(conn net.Conn) {
	remoteID := conn.RemoteAddr().String()
	core.Log(core.LogInfo, "%s - Connection established", remoteID)

	defer func() {
		if err := recover(); err != nil {
			core.Log(core.LogError, "%s - %v", remoteID, err)
		}

		core.Log(core.LogInfo, "%s - Connection closed", remoteID)
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
			core.Log(core.LogInfo, "%s < %s %s", remoteID, incoming.String(), incoming.Details())
			reply := &core.ProtocolMessage{Num: incoming.Num, Type: incoming.Type & core.MsgTypeServerMask}
			defer reply.Release()

			switch incoming.Type {
			case core.MsgTypeOldGreeting:
				reply.Type = core.MsgTypeError & core.MsgTypeServerMask
				reply.Data = &core.MsgServerError{"Client uses an outdated protocol version"}
			case core.MsgTypeGreeting:
				c := incoming.Data.(*core.MsgClientGreeting)
				if c.Version < core.ProtocolVersion {
					reply.Type = core.MsgTypeError & core.MsgTypeServerMask
					reply.Data = &core.MsgServerError{"Client uses an outdated protocol version"}
				} else if c.Version > core.ProtocolVersion {
					reply.Type = core.MsgTypeError & core.MsgTypeServerMask
					reply.Data = &core.MsgServerError{"Server uses an outdated protocol version"}
				} else {
					// Create server nonce using  64-bit time and 64-bit random
					binary.BigEndian.PutUint64(clientSession.SessionNonce[0:], uint64(time.Now().UnixNano()))
					binary.BigEndian.PutUint32(clientSession.SessionNonce[8:], rand.Uint32())
					binary.BigEndian.PutUint32(clientSession.SessionNonce[12:], rand.Uint32())
					reply.Data = &core.MsgServerGreeting{clientSession.SessionNonce}
				}
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
							if !storageHandler.checkFree(int64(c.Block.Data.Len())) {
								reply.Type = core.MsgTypeError & core.MsgTypeServerMask
								reply.Data = &core.MsgServerError{"Write permission is denied because the server is out of space"}
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
			core.Log(core.LogInfo, "%s > %s %s", remoteID, reply.String(), reply.Details())
			core.WriteMessage(conn, reply)
		}()
	}
}

func connectionListener(listener *net.TCPListener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			core.Log(core.LogInfo, "Closed network connection")
			done <- true
			return
		}
		go handleConnection(conn)
	}
}

func run() int {
	bytearray.EnableAutoGC(60, 74)

	runtime.SetBlockProfileRate(1000)
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	//defer profile.Start(&profile.Config{CPUProfile: false, MemProfile: true, ProfilePath: ".", NoShutdownHook: true}).Stop()

	/*defer func() {
		// Panic error handling
		if r := recover(); r != nil {
			fmt.Println(r)
			return 1
		}
	}()*/

	var err error

	exeRoot, _ := osext.ExecutableFolder()

	var serverPort int64 = int64(DEFAULT_SERVER_IP_PORT)
	datDirectory = filepath.Join(exeRoot, "data")
	idxDirectory = filepath.Join(exeRoot, "index")

	cmd.Title = fmt.Sprintf("Hashbox Server %s", Version)
	cmd.IntOption("port", "", "<port>", "Server listening port", &serverPort, cmd.Standard)
	cmd.StringOption("data", "", "<path>", "Full path to dat files", &datDirectory, cmd.Standard)
	cmd.StringOption("index", "", "<path>", "Full path to idx and meta files", &idxDirectory, cmd.Standard)
	var loglvl int64 = int64(core.LogInfo)
	cmd.IntOption("loglevel", "", "<level>", "Set log level (0=errors, 1=warnings, 2=info, 3=debug, 4=trace", &loglvl, cmd.Hidden).OnChange(func() {
		core.LogLevel = int(loglvl)
	})

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
			panic(errors.New(fmt.Sprintf("Error listening: %v", err.Error())))
		}
		core.Log(core.LogInfo, "%s is listening on %s", cmd.Title, listener.Addr().String())

		done = make(chan bool)
		defer close(done)

		signalchan := make(chan os.Signal, 1)
		defer close(signalchan)
		signal.Notify(signalchan, os.Interrupt)
		signal.Notify(signalchan, os.Kill)
		go func() {
			for s := range signalchan {
				core.Log(core.LogInfo, "Received OS signal: %v", s)
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

		core.Log(core.LogInfo, "Hashbox Server terminating")
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
		core.Log(core.LogDebug, "DataEncryptionKey is: %x", dataEncryptionKey)
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
		core.Log(core.LogInfo, "User added")
	})

	var doRepair bool
	var doRebuild bool
	var skipData, skipMeta, skipIndex bool
	cmd.BoolOption("repair", "check-storage", "Repair non-fatal errors", &doRepair, cmd.Standard)
	cmd.BoolOption("rebuild", "check-storage", "Rebuild index and meta files from data", &doRebuild, cmd.Standard)
	cmd.BoolOption("skipdata", "check-storage", "Skip checking data files", &skipData, cmd.Standard)
	cmd.BoolOption("skipmeta", "check-storage", "Skip checking meta files", &skipMeta, cmd.Standard)
	cmd.BoolOption("skipindex", "check-storage", "Skip checking index files", &skipIndex, cmd.Standard)
	cmd.Command("check-storage", "", func() {
		if doRebuild {
			if len(cmd.Args) > 2 {
				panic("Start and end file arguments are not valid in combination with rebuild")
			}
			doRepair = true
		}

		startfile := int32(0)
		endfile := int32(-1)
		if len(cmd.Args) > 2 {
			i, err := strconv.ParseInt(cmd.Args[2], 0, 32)
			if err != nil {
				panic(err)
			}
			startfile = int32(i)
			core.Log(core.LogInfo, "Starting from file #%d (%04x)", startfile, startfile)
		}
		if len(cmd.Args) > 3 {
			i, err := strconv.ParseInt(cmd.Args[3], 0, 32)
			if err != nil {
				panic(err)
			}
			endfile = int32(i)
			core.Log(core.LogInfo, "Stopping after file #%d (%04x)", endfile, endfile)
		}

		if doRepair {
			if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
				panic(err)
			} else {
				defer lock.Unlock()
			}
		}

		start := time.Now()

		if doRebuild {
			core.Log(core.LogInfo, "Removing index files")
			storageHandler.RemoveFiles(storageFileTypeIndex)
			core.Log(core.LogInfo, "Removing meta files")
			storageHandler.RemoveFiles(storageFileTypeMeta)
		}

		core.Log(core.LogInfo, "Checking all storage files")
		repaired, critical := storageHandler.CheckFiles(doRepair)
		if !skipData && (doRepair || critical == 0) {
			core.Log(core.LogInfo, "Checking data files")
			r, c := storageHandler.CheckData(doRepair, startfile, endfile)
			repaired += r
			critical += c
		}
		if !skipMeta && (doRepair || critical == 0) {
			core.Log(core.LogInfo, "Checking meta files")
			storageHandler.CheckMeta()
		}

		if !skipIndex && (doRepair || critical == 0) {
			core.Log(core.LogInfo, "Checking index files")
			storageHandler.CheckIndexes(doRepair)
		}

		if doRepair || critical == 0 {
			core.Log(core.LogInfo, "Checking dataset transactions")
			rootlist := accountHandler.RebuildAccountFiles()

			core.Log(core.LogInfo, "Checking block chain integrity")
			verified := make(map[core.Byte128]bool) // Keep track of verified blocks
			for i, r := range rootlist {
				tag := fmt.Sprintf("%s.%s.%x", r.AccountName, r.DatasetName, r.StateID[:])
				core.Log(core.LogDebug, "CheckChain on %s", tag)
				c := storageHandler.CheckChain(r.BlockID, tag, verified)
				if c > 0 {
					accountHandler.InvalidateDatasetState(r.AccountNameH, r.DatasetName, r.StateID)
				}
				critical += c

				p := int(i * 100 / len(rootlist))
				fmt.Printf("%d%%\r", p)
			}
		}

		if critical > 0 {
			core.Log(core.LogError, "Detected %d critical errors, DO NOT start the server unless everything is repaired", critical)
		}
		if repaired > 0 {
			core.Log(core.LogWarning, "Performed %d repairs, please run again to verify repairs", repaired)
		}
		if critical == 0 && repaired == 0 {
			core.Log(core.LogInfo, "All checks completed successfully in %.1f minutes", time.Since(start).Minutes())
		}
	})

	var doCompact bool
	var deadSkip int64 = 5
	var skipSweep bool
	var doForce bool
	cmd.BoolOption("compact", "gc", "Compact data files to free space", &doCompact, cmd.Standard)
	cmd.BoolOption("skipsweep", "gc", "Skip sweeping indexes", &skipSweep, cmd.Standard)
	cmd.BoolOption("force", "gc", "Ignore broken datasets and force a garbage collect", &doForce, cmd.Standard)
	cmd.IntOption("threshold", "gc", "<percentage>", "Compact minimum dead space threshold", &deadSkip, cmd.Standard)
	cmd.Command("gc", "", func() {
		if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
			panic(err)
		} else {
			defer lock.Unlock()
		}

		start := time.Now()
		if !skipSweep {
			core.Log(core.LogInfo, "Marking index entries")
			var roots []core.Byte128
			for _, r := range accountHandler.CollectAllRootBlocks(doForce) {
				roots = append(roots, r.BlockID)
			}
			storageHandler.MarkIndexes(roots, true)
			storageHandler.SweepIndexes(true)
			core.Log(core.LogInfo, "Mark and sweep duration %.1f minutes", time.Since(start).Minutes())
			storageHandler.ShowStorageDeadSpace()
		}
		if doCompact {
			storageHandler.CompactIndexes(true)
			storageHandler.CompactAll(storageFileTypeMeta, int(deadSkip))
			storageHandler.CompactAll(storageFileTypeData, int(deadSkip))
		}
		core.Log(core.LogInfo, "Garbage collection completed in %.1f minutes", time.Since(start).Minutes())
	})

	err = cmd.Parse()
	PanicOn(err)

	fmt.Println(core.MemoryStats())

	return 0
}

func main() {
	os.Exit(run())
}
