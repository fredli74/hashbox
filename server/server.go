//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package main

import (
	//"github.com/davecheney/profile"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"

	"github.com/fredli74/bytearray"
	cmd "github.com/fredli74/cmdparser"
	"github.com/fredli74/hashbox/pkg/accountdb"
	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/hashbox/pkg/storagedb"
	"github.com/fredli74/lockfile"
)

var Version = "(dev-build)"

const DEFAULT_SERVER_IP_PORT int = core.DefaultServerPort

var datDirectory string
var idxDirectory string
var accountHandler *AccountHandler
var storageHandler *StorageHandler

var done chan bool

func handleConnection(conn net.Conn) {
	remoteID := conn.RemoteAddr().String()
	core.Log(core.LogInfo, "%s - Connection established", remoteID)

	defer func() {
		if err := recover(); err != nil {
			core.Log(core.LogError, "%s - %v", remoteID, err)
			core.Log(core.LogInfo, "%s - Stacktrace from panic: %s", remoteID, debug.Stack())
		}

		core.Log(core.LogInfo, "%s - Connection closed", remoteID)
		if err := conn.Close(); err != nil {
			core.Log(core.LogWarning, "%s - Error closing connection: %v", remoteID, err)
		}
	}()

	var clientSession core.Session
	var sessionAuthenticated = false

	for keepAlive := true; keepAlive; {
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Minute)); err != nil {
			core.AbortOnError(err)
		} // max size 512kb over 10 minutes is slower than 9600bps so we should be safe
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
				reply.Data = &core.MsgServerError{ErrorMessage: "Client uses an outdated protocol version"}
			case core.MsgTypeGreeting:
				c := incoming.Data.(*core.MsgClientGreeting)
				if c.Version < core.ProtocolVersion {
					reply.Type = core.MsgTypeError & core.MsgTypeServerMask
					reply.Data = &core.MsgServerError{ErrorMessage: "Client uses an outdated protocol version"}
				} else if c.Version > core.ProtocolVersion {
					reply.Type = core.MsgTypeError & core.MsgTypeServerMask
					reply.Data = &core.MsgServerError{ErrorMessage: "Server uses an outdated protocol version"}
				} else {
					// Create server nonce using  64-bit time and 64-bit random
					binary.BigEndian.PutUint64(clientSession.SessionNonce[0:], uint64(time.Now().UnixNano()))
					_, err := rand.Read(clientSession.SessionNonce[8:])
					core.AbortOnError(err)
					reply.Data = &core.MsgServerGreeting{SessionNonce: clientSession.SessionNonce}
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
								reply.Data = &core.MsgServerError{ErrorMessage: "Dataset pointing to a nonexistent block"}
							} else {
								core.AbortOnError(accountHandler.AddDatasetState(c.AccountNameH, c.DatasetName, c.State))
								// No need to set any data in reply
							}
						}
					case core.MsgTypeRemoveDatasetState:
						c := incoming.Data.(*core.MsgClientRemoveDatasetState)
						unauthorized = c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
						if !unauthorized {
							core.AbortOnError(accountHandler.RemoveDatasetState(c.AccountNameH, c.DatasetName, c.StateID))
							// No need to set any data in reply
						}
					case core.MsgTypeListDataset:
						c := incoming.Data.(*core.MsgClientListDataset)
						unauthorized = c.AccountNameH != clientSession.AccountNameH // TODO: admin support for other accounts hashes?
						if !unauthorized {
							list := accountHandler.ListDataset(c.AccountNameH, c.DatasetName)
							if list == nil {
								list = new(accountdb.DBStateCollection)
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
								reply.Data = &core.MsgServerError{ErrorMessage: "Cannot find account information"}
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
							reply.Data = &core.MsgServerError{ErrorMessage: "Invalid blockID"}
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
									reply.Data = &core.MsgServerError{ErrorMessage: core.String(fmt.Sprintf("Linked to nonexistent block: block=%x link=%x", c.Block.BlockID, l))}
									break
								}
							}
							if !storageHandler.store.CheckFree(int64(c.Block.Data.Len())) {
								reply.Type = core.MsgTypeError & core.MsgTypeServerMask
								reply.Data = &core.MsgServerError{ErrorMessage: "Write permission is denied because the server is out of space"}
							}
							if reply.Data == nil {
								storageHandler.writeBlock(c.Block)
								reply.Type = core.MsgTypeAcknowledgeBlock & core.MsgTypeServerMask
								reply.Data = &core.MsgServerAcknowledgeBlock{BlockID: c.Block.BlockID}
							}
						} else {
							reply.Type = core.MsgTypeError & core.MsgTypeServerMask
							reply.Data = &core.MsgServerError{ErrorMessage: "Unable to verify blockID"}
						}
					default:
						core.ASSERT(false, incoming.String()) // Well if we reach this point, we have not implemented everything correctly
					}
				}
			}

			if unauthorized {
				// Invalid authentication
				// TODO: Add to server auth.log
				reply.Type = core.MsgTypeError & core.MsgTypeServerMask
				reply.Data = &core.MsgServerError{ErrorMessage: "Invalid authentication"}
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

func run() (returnValue int) {
	bytearray.EnableAutoGC(60, 74)

	var err error

	core.ApplyEnvUMASK()

	var serverPort int64 = int64(DEFAULT_SERVER_IP_PORT)
	datDirectory, err = filepath.Abs("data")
	core.AbortOnError(err, "abs data dir: %v", err)
	idxDirectory, err = filepath.Abs("index")
	core.AbortOnError(err, "abs index dir: %v", err)

	cmd.Title = fmt.Sprintf("Hashbox Server %s", Version)
	cmd.ShowCurrentDefaults = true
	cmd.IntOption("port", "", "<port>", "Server listening port", &serverPort, cmd.Standard)
	cmd.StringOption("data", "", "<path>", "Full path to dat files", &datDirectory, cmd.Standard)
	cmd.StringOption("index", "", "<path>", "Full path to idx and meta files", &idxDirectory, cmd.Standard)
	var optLogLevel int64 = int64(core.LogInfo)
	cmd.IntOption("loglevel", "", "<level>", "Set log level (0=errors, 1=warnings, 2=info, 3=debug, 4=trace", &optLogLevel, cmd.Standard).OnChange(func() {
		core.LogLevel = int(optLogLevel)

		if optLogLevel >= 3 {
			runtime.SetBlockProfileRate(1000)
			go func() {
				core.Log(core.LogInfo, "%v", http.ListenAndServe(":6060", nil))
			}()
		}
	})

	cmd.Command("", "", func() { // Default
		serverAddr := net.TCPAddr{IP: nil, Port: int(serverPort), Zone: ""}

		if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
			core.Abort("%v", err)
		} else {
			defer lock.Unlock()
		}

		accountHandler = NewAccountHandler()
		defer accountHandler.Close()
		storageHandler = NewStorageHandler()
		defer storageHandler.Close()

		var listener *net.TCPListener
		if listener, err = net.ListenTCP("tcp", &serverAddr); err != nil {
			core.Abort("Error listening: %v", err.Error())
		}
		core.Log(core.LogInfo, "%s is listening on %s", cmd.Title, listener.Addr().String())

		done = make(chan bool)
		defer close(done)

		signalchan := make(chan os.Signal, 1)
		defer close(signalchan)
		signal.Notify(signalchan, os.Interrupt, syscall.SIGTERM)
		go func() {
			for s := range signalchan {
				core.Log(core.LogInfo, "Received OS signal: %v", s)
				core.AbortOnError(listener.Close())
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
			core.Abort("%v", err)
		} else {
			defer lock.Unlock()
		}

		accountHandler = NewAccountHandler()
		defer accountHandler.Close()
		storageHandler = NewStorageHandler()
		defer storageHandler.Close()

		if len(cmd.Args) < 4 {
			core.Abort("Missing argument to adduser command")
		}

		if !accountHandler.SetInfo(accountdb.AccountInfo{AccountName: core.String(cmd.Args[2]), AccessKey: core.GenerateAccessKey(cmd.Args[2], cmd.Args[3])}) {
			core.Abort("Error creating account")
		}
		accountNameH := core.Hash([]byte(cmd.Args[2]))
		dataEncryptionKey := core.GenerateDataEncryptionKey()
		core.Log(core.LogDebug, "DataEncryptionKey is: %x", dataEncryptionKey)
		core.EncryptDataInPlace(dataEncryptionKey[:], core.GenerateBackupKey(cmd.Args[2], cmd.Args[3]))

		var blockData bytearray.ByteArray
		if _, err := blockData.Write(dataEncryptionKey[:]); err != nil {
			core.AbortOnError(err)
		}
		block := core.NewHashboxBlock(core.BlockDataTypeRaw, blockData, nil)
		if !storageHandler.writeBlock(block) {
			core.Abort("Error writing key block")
		}
		core.AbortOnError(accountHandler.AddDatasetState(accountNameH, core.String("\x07HASHBACK_DEK"), core.DatasetState{BlockID: block.BlockID}))

		block.Release()
		core.Log(core.LogInfo, "User added")
	})

	var optGcCompact bool
	var optGcCompactOnly bool
	var optGcDeadSkip int64 = 5
	var optGcForce bool
	cmd.BoolOption("compact", "gc", "Compact data files to free space", &optGcCompact, cmd.Standard)
	cmd.BoolOption("compact-only", "gc", "Compact without sweeping indexes first", &optGcCompactOnly, cmd.Standard)
	cmd.BoolOption("force", "gc", "Ignore invalid datasets and force a garbage collect", &optGcForce, cmd.Standard)
	cmd.IntOption("threshold", "gc", "<percentage>", "Compact minimum dead space threshold", &optGcDeadSkip, cmd.Standard)
	cmd.Command("gc", "", func() {
		if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
			core.Abort("%v", err)
		} else {
			defer lock.Unlock()
		}

		accountHandler = NewAccountHandler()
		defer accountHandler.Close()
		storageHandler = NewStorageHandler()
		defer storageHandler.Close()

		start := time.Now()
		if !optGcCompactOnly {
			core.Log(core.LogInfo, "Marking index entries")
			var roots []core.Byte128
			for _, r := range accountHandler.CollectAllRootBlocks(optGcForce) {
				roots = append(roots, r.BlockID)
			}
			storageHandler.store.MarkIndexes(roots, true)
			storageHandler.store.SweepIndexes(true)
			core.Log(core.LogInfo, "Mark and sweep duration %.1f minutes", time.Since(start).Minutes())
			storageHandler.store.ShowStorageDeadSpace()
		}
		if optGcCompact || optGcCompactOnly {
			storageHandler.store.CompactIndexes(true)
			storageHandler.store.CompactAll(storagedb.StorageFileTypeMeta, int(optGcDeadSkip))
			storageHandler.store.CompactAll(storagedb.StorageFileTypeData, int(optGcDeadSkip))
		}
		core.Log(core.LogInfo, "Garbage collection completed in %.1f minutes", time.Since(start).Minutes())
	})

	cmd.Command("recover", "[start-file-number] [end-file-number]", func() {
		startfile := int32(0)
		endfile := int32(-1)
		if len(cmd.Args) > 2 {
			i, err := strconv.ParseInt(cmd.Args[2], 0, 32)
			core.AbortOnError(err)
			startfile = int32(i)
			core.Log(core.LogInfo, "Starting from file #%d (%08x)", startfile, startfile)
		}
		if len(cmd.Args) > 3 {
			i, err := strconv.ParseInt(cmd.Args[3], 0, 32)
			core.AbortOnError(err)
			endfile = int32(i)
			core.Log(core.LogInfo, "Stopping after file #%d (%08x)", endfile, endfile)
		}

		if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
			core.Abort("%v", err)
		} else {
			defer lock.Unlock()
		}

		accountHandler = NewAccountHandler()
		defer accountHandler.Close()
		storageHandler = NewStorageHandler()
		defer storageHandler.Close()

		start := time.Now()

		core.Log(core.LogInfo, "Checking storage file headers")
		if storageHandler.store.CheckStorageFiles() > 0 {
			core.Log(core.LogError, "Recovery will not continue because errors where raised.")
			returnValue = 1
			return
		}

		core.Log(core.LogInfo, "Scanning data files")
		repairCount := storageHandler.store.RecoverData(startfile, endfile)

		if repairCount > 0 {
			core.Log(core.LogInfo, "Performed %d repairs. Please run a verify.", repairCount)
		}
		core.Log(core.LogInfo, "Recover completed in %.1f minutes.", time.Since(start).Minutes())
	})

	optVerifyContent := false
	optReadOnly := true
	cmd.BoolOption("content", "verify", "Uncompress and verifying block content", &optVerifyContent, cmd.Standard)
	cmd.BoolOption("readonly", "verify", "Do not invalidate broken block trees", &optReadOnly, cmd.Standard)
	cmd.Command("verify", "", func() {
		if !optReadOnly {
			if lock, err := lockfile.Lock(filepath.Join(datDirectory, "hashbox.lck")); err != nil {
				core.Abort("%v", err)
			} else {
				defer lock.Unlock()
			}
		}

		accountHandler = NewAccountHandler()
		defer accountHandler.Close()
		storageHandler = NewStorageHandler()
		defer storageHandler.Close()

		errorCount := 0
		start := time.Now()

		core.Log(core.LogInfo, "Verifying dataset storage")

		verifiedBlocks := make(map[core.Byte128]bool) // Keep track of verifiedBlocks blocks
		if !optReadOnly {
			accountHandler.RebuildAccountFiles()
		}
		rootlist := accountHandler.CollectAllRootBlocks(false)

		for i, r := range rootlist {
			tag := fmt.Sprintf("%s.%s.%x", r.AccountName, r.DatasetName, r.StateID[:])
			core.Log(core.LogDebug, "Verify data referenced by %s", tag)
			if err := storageHandler.store.CheckBlockTree(r.BlockID, verifiedBlocks, optVerifyContent, optReadOnly); err != nil {
				if optReadOnly {
					core.Abort("%v", err)
				} else {
					core.Log(core.LogWarning, "Dataset %s is marked as invalid: %v", tag, err)
					accountHandler.InvalidateDatasetState(r.AccountNameH, r.DatasetName, r.StateID)
					errorCount++
				}
			}

			p := int(i * 100 / len(rootlist))
			fmt.Printf("%d%%\r", p)
		}

		core.Log(core.LogInfo, "Verifying unreferenced index entries")
		storageHandler.store.CheckIndexes(verifiedBlocks, optVerifyContent, optReadOnly)

		core.Log(core.LogInfo, "Verify completed in %.1f minutes", time.Since(start).Minutes())
		if errorCount > 0 {
			core.Log(core.LogWarning, "Detected errors in %d datasets, please run a full recover or remove the datasets", errorCount)
		}
	})

	core.AbortOnError(cmd.Parse())

	fmt.Println(core.MemoryStats())

	return returnValue
}

func main() {
	os.Exit(run())
}
