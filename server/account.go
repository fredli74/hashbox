//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2018
//	+---+´

package main

import (
	"crypto/md5"
	"encoding/base64"
	"github.com/fredli74/hashbox/core"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type AccountInfo struct {
	AccountName core.String
	AccessKey   core.Byte128
	Datasets    core.DatasetArray
}
type queryListDataset struct {
	AccountNameH core.Byte128
	DatasetName  core.String
}
type queryAddDatasetState struct {
	AccountNameH core.Byte128
	DatasetName  core.String
	State        core.DatasetState
}
type queryRemoveDatasetState struct {
	AccountNameH core.Byte128
	DatasetName  core.String
	StateID      core.Byte128
}

type ChannelQuery struct {
	query  int
	data   interface{}
	result chan interface{}
}

type AccountHandler struct {
	signal chan error // goroutine signal channel, returns raised errors and stops goroutine when closed
	query  chan ChannelQuery
	wg     sync.WaitGroup
}

const (
	accounthandler_setinfo = iota
	accounthandler_getinfo
	accounthandler_listset
	accounthandler_addset
	accounthandler_removeset
)

func (handler *AccountHandler) dispatcher() {
	defer func() {
		// query cleanup
		close(handler.query)
		for q := range handler.query {
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
		case q := <-handler.query:
			func() {
				defer close(q.result) // Always close the result channel after returning
				switch q.query {
				case accounthandler_getinfo:
					accountNameH := q.data.(core.Byte128)
					q.result <- readInfoFile(accountNameH)

				case accounthandler_setinfo:
					accountInfo := q.data.(AccountInfo)
					accountNameH := core.Hash([]byte(accountInfo.AccountName))

					writeInfoFile(accountNameH, accountInfo)
					q.result <- true

				case accounthandler_listset:
					list := q.data.(queryListDataset)
					q.result <- readDBFile(list.AccountNameH, list.DatasetName)

				case accounthandler_addset:
					add := q.data.(queryAddDatasetState)

					result := appendDatasetTx(add.AccountNameH, add.DatasetName, dbTx{timestamp: time.Now().UnixNano(), txType: dbTxTypeAdd, data: add.State})

					{ // update the db file
						collection := readDBFile(add.AccountNameH, add.DatasetName)
						if collection != nil {
							for i, s := range collection.States {
								if s.State.StateID.Compare(add.State.StateID) == 0 {
									collection.States[i] = collection.States[len(collection.States)-1]
									collection.States = collection.States[:len(collection.States)-1]
									break
								}
							}
						} else {
							collection = &dbStateCollection{}
						}
						collection.States = append(collection.States, core.DatasetStateEntry{State: add.State})
						sort.Sort(collection.States)
						writeDBFile(add.AccountNameH, add.DatasetName, collection)
					}

					q.result <- result

				case accounthandler_removeset:
					del := q.data.(queryRemoveDatasetState)

					result := appendDatasetTx(del.AccountNameH, del.DatasetName, dbTx{timestamp: time.Now().UnixNano(), txType: dbTxTypeDel, data: del.StateID})

					{ // update the db file
						collection := readDBFile(del.AccountNameH, del.DatasetName)
						if collection != nil {
							for i, s := range collection.States {
								if s.State.StateID.Compare(del.StateID) == 0 {
									copy(collection.States[i:], collection.States[i+1:])
									collection.States = collection.States[:len(collection.States)-1]
									break
								}
							}
						} else {
							collection = &dbStateCollection{}
						}
						writeDBFile(del.AccountNameH, del.DatasetName, collection)
					}

					q.result <- result

				default:
					abort("Unknown query in AccountHandler causing hangup: %d", q.query)
				}
			}()
		case _, ok := <-handler.signal: // Signal is closed?
			// TODO: remove this check
			if ok {
				abort("We should not reach this point, it means someone outside this goroutine sent a signal on the channel")
			}
			return
		}
	}
}

func (handler *AccountHandler) doCommand(q ChannelQuery) interface{} {
	defer func() {
		r := recover()
		select {
		case err := <-handler.signal:
			if err != nil {
				abort("AccountHandler panic: %v", err)
			}
		default:
			switch t := r.(type) {
			case error:
				abort("AccountHandler panic: %v", t)
			}
		}
	}()
	handler.query <- q
	r := <-q.result
	return r
}

func (handler *AccountHandler) ListDataset(a core.Byte128, set core.String) *dbStateCollection {
	q := ChannelQuery{accounthandler_listset, queryListDataset{a, set}, make(chan interface{}, 1)}
	return handler.doCommand(q).(*dbStateCollection)
}

func (handler *AccountHandler) AddDatasetState(a core.Byte128, set core.String, state core.DatasetState) error {
	q := ChannelQuery{accounthandler_addset, queryAddDatasetState{a, set, state}, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(error)
	}
	return nil
}
func (handler *AccountHandler) RemoveDatasetState(a core.Byte128, set core.String, stateID core.Byte128) error {
	q := ChannelQuery{accounthandler_removeset, queryRemoveDatasetState{a, set, stateID}, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(error)
	}
	return nil
}
func (handler *AccountHandler) GetInfo(a core.Byte128) *AccountInfo {
	q := ChannelQuery{accounthandler_getinfo, a, make(chan interface{}, 1)}
	return handler.doCommand(q).(*AccountInfo)
	// ToDO: test this with a read-error
}
func (handler *AccountHandler) SetInfo(a AccountInfo) bool {
	q := ChannelQuery{accounthandler_setinfo, a, make(chan interface{}, 1)}
	return handler.doCommand(q).(bool)
	// ToDO: test this with a write-error
}

func (handler *AccountHandler) Close() {
	close(handler.signal)
	handler.wg.Wait()

	// Cleanup here
}
func NewAccountHandler() *AccountHandler {
	handler := &AccountHandler{
		query:  make(chan ChannelQuery, 32),
		signal: make(chan error), // cannot be buffered
	}
	handler.wg.Add(1)

	go handler.dispatcher()
	return handler
}

//*****************************************************************************************************************//
//
// Account file handling below
//
//*****************************************************************************************************************//

const (
	dbVersion                  uint32 = 1
	dbFileTypeTransaction      uint32 = 0x48415458 // "HATX" Hashbox Account Transaction
	dbFileExtensionTransaction string = ".trn"
	dbFileTypeDatabase         uint32 = 0x48414442 // "HADB" Hashbox Account Database
	dbFileExtensionDatabase    string = ".db"
	dbTxTypeAdd                uint32 = 0x2B414444 // "+ADD"
	dbTxTypeDel                uint32 = 0x2D44454C // "-DEL"
)

type dbFileHeader struct {
	filetype    uint32
	version     uint32
	datasetName core.String
}

func (h *dbFileHeader) Serialize(w io.Writer) {
	core.WriteUint32(w, h.filetype)
	core.WriteUint32(w, h.version)
	h.datasetName.Serialize(w)
}
func (h *dbFileHeader) Unserialize(r io.Reader) {
	core.ReadUint32(r, &h.filetype)
	core.ReadUint32(r, &h.version)
	if h.version != dbVersion {
		abort("Invalid version in dbFileHeader")
	}
	h.datasetName.Unserialize(r)
}

type dbTx struct {
	timestamp int64
	txType    uint32
	data      interface{}
}

func (t *dbTx) Serialize(w io.Writer) {
	core.WriteInt64(w, t.timestamp)
	core.WriteUint32(w, t.txType)
	t.data.(core.Serializer).Serialize(w)
}
func (t *dbTx) Unserialize(r io.Reader) {
	core.ReadInt64(r, &t.timestamp)
	core.ReadUint32(r, &t.txType)
	switch t.txType {
	case dbTxTypeAdd:
		var s core.DatasetState
		s.Unserialize(r)
		t.data = s
	case dbTxTypeDel:
		var s core.Byte128
		s.Unserialize(r)
		t.data = s
	default:
		abort("Corrupt transaction file")
	}
}

type dbStateCollection struct {
	// datasetName is already in the file header
	Size   int64        // Size of all data referenced by this dataset
	ListH  core.Byte128 // = md5(States)
	States core.DatasetStateArray
}

func (c *dbStateCollection) Serialize(w io.Writer) {
	c.States.Serialize(w)
}
func (c *dbStateCollection) Unserialize(r io.Reader) {
	c.States.Unserialize(r)
	{
		c.Size = 0
		maxSize := int64(0)
		hash := md5.New()
		for _, s := range c.States {
			s.Serialize(hash)
			if s.State.Size > maxSize {
				maxSize = s.State.Size
			}
			c.Size += s.State.UniqueSize // TODO: this calculation is wrong the moment you start deleting stuff, it needs to be reworked
		}
		copy(c.ListH[:], hash.Sum(nil)[:16])
		c.Size += maxSize
	}
}

func appendDatasetTx(accountNameH core.Byte128, datasetName core.String, tx dbTx) error {
	file, err := os.OpenFile(datasetFilename(accountNameH, string(datasetName))+dbFileExtensionTransaction, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	abortOn(err)
	defer file.Close()

	pos, err := file.Seek(0, 2)
	if pos == 0 { // New file, write the header
		header := dbFileHeader{filetype: dbFileTypeTransaction, version: dbVersion, datasetName: datasetName}
		header.Serialize(file)
	}
	tx.Serialize(file)
	return nil
}
func updateInfoFile(accountNameH core.Byte128, datasetName core.String) {
	collection := readDBFile(accountNameH, datasetName)
	if collection == nil {
		abort("updateInfoFile was called on a DB file which cannot be opened")
	}

	// Now also update account info
	info := readInfoFile(accountNameH)
	for i := 0; i < len(info.Datasets); i++ {
		if info.Datasets[i].Name == datasetName {
			info.Datasets[i] = info.Datasets[len(info.Datasets)-1]
			info.Datasets = info.Datasets[:len(info.Datasets)-1]
			break
		}
	}
	if len(collection.States) > 0 { // Add the dataset state
		info.Datasets = append(info.Datasets, core.Dataset{Name: datasetName, Size: collection.Size, ListH: collection.ListH})
	}
	sort.Sort(info.Datasets)
	writeInfoFile(accountNameH, *info)
}

func stateArrayFromTransactions(accountNameH core.Byte128, datasetName core.String) (states core.DatasetStateArray) {
	filename := datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
	file, err := os.Open(filename)
	abortOn(err)
	defer file.Close()
	var header dbFileHeader
	header.Unserialize(file)
	if header.version != dbVersion || header.filetype != dbFileTypeTransaction {
		abort("%s is not a valid transaction file", filename)
	}

	stateMap := make(map[core.Byte128]core.DatasetState)
	// Go through transaction history and populate the stateMap
	func() {
		defer func() {
			if r := recover(); r != nil && r != io.EOF {
				panic(r) // Any other error is not normal and should panic
			}
			return
		}()

		var pointInHistory int64
		for {
			var tx dbTx
			tx.Unserialize(file)
			if tx.timestamp < pointInHistory {
				abort("%s is corrupt, timestamp check failed (%x < %x)", filename, tx.timestamp, pointInHistory)
			}
			pointInHistory = tx.timestamp
			switch tx.txType {
			case dbTxTypeAdd:
				stateMap[tx.data.(core.DatasetState).StateID] = tx.data.(core.DatasetState)
			case dbTxTypeDel:
				delete(stateMap, tx.data.(core.Byte128))
			default:
				abort("%s is corrupt, invalid transaction type found: %x", filename, tx.txType)
			}
		}
	}()

	for _, s := range stateMap {
		states = append(states, core.DatasetStateEntry{State: s})
	}
	return states
}

func writeDBFile(accountNameH core.Byte128, datasetName core.String, c *dbStateCollection) {
	file, err := os.OpenFile(datasetFilename(accountNameH, string(datasetName))+dbFileExtensionDatabase, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	abortOn(err)
	defer file.Close()

	header := dbFileHeader{filetype: dbFileTypeDatabase, version: dbVersion, datasetName: datasetName}
	header.Serialize(file)
	c.Serialize(file)

	updateInfoFile(accountNameH, datasetName)
}
func readDBFile(accountNameH core.Byte128, datasetName core.String) *dbStateCollection {
	filename := datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionDatabase
	file, err := os.Open(filename)
	if err != nil {
		return nil
	}
	defer file.Close()
	var header dbFileHeader
	header.Unserialize(file)
	if header.version != dbVersion || header.filetype != dbFileTypeDatabase {
		abort("%s is not a valid db file", filename)
	}
	var c dbStateCollection
	c.Unserialize(file)
	return &c
}
func writeInfoFile(accountNameH core.Byte128, a AccountInfo) {
	file, err := os.OpenFile(accountFilename(accountNameH)+".info", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	abortOn(err)
	defer file.Close()

	a.AccountName.Serialize(file)
	a.AccessKey.Serialize(file)
	a.Datasets.Serialize(file)
}
func readInfoFile(accountNameH core.Byte128) *AccountInfo {
	file, err := os.Open(accountFilename(accountNameH) + ".info")
	if err != nil {
		return nil
	}

	var a AccountInfo
	a.AccountName.Unserialize(file)
	a.AccessKey.Unserialize(file)
	a.Datasets.Unserialize(file)
	return &a
}

func base64filename(d []byte) string {
	return base64.RawURLEncoding.EncodeToString(d)
}
func accountFilename(nameHash core.Byte128) string {
	name := base64filename(nameHash[:])
	return filepath.Join(datDirectory, "account", name)
}
func datasetFilename(aH core.Byte128, dName string) string {
	dNameH := core.Hash([]byte(dName))
	return accountFilename(aH) + "." + base64filename(dNameH[:])
}

type BlockSource struct {
	BlockID      core.Byte128
	AccountName  string
	AccountNameH core.Byte128
	DatasetName  core.String
	StateID      core.Byte128
}

func getDatasetNameFromFile(filename string) core.String {
	// Open the file, read and check the file headers
	fil, err := os.Open(filepath.Join(datDirectory, "account", filename))
	abortOn(err)
	defer fil.Close()

	var header dbFileHeader
	header.Unserialize(fil)
	if header.filetype != dbFileTypeTransaction {
		abort("File %s is not a valid transaction file", filename)
	}

	datasetName := header.datasetName
	var datasetNameH core.Byte128
	{
		d, err := base64.RawURLEncoding.DecodeString(filename[23:45])
		abortOn(err)
		datasetNameH.Set(d)
	}
	datasetHashB := core.Hash([]byte(datasetName))
	if datasetHashB.Compare(datasetNameH) != 0 {
		abort("Header for %s does not contain the correct dataset name", filename)
	}
	return datasetName
}
func (handler *AccountHandler) CollectAllRootBlocks(skipInvalid bool) (rootBlocks []BlockSource) {
	// Open each dataset and check the chains
	dir, err := os.Open(filepath.Join(datDirectory, "account"))
	abortOn(err)
	defer dir.Close()
	dirlist, err := dir.Readdir(-1)
	abortOn(err)

	for _, info := range dirlist {
		name := info.Name()
		if m, _ := filepath.Match("??????????????????????.??????????????????????.trn", name); m {
			// Read the accountNameH from the filename
			var accountName string
			var accountNameH core.Byte128
			{
				decoded, err := base64.RawURLEncoding.DecodeString(name[:22])
				abortOn(err)
				accountNameH.Set(decoded)
				info := readInfoFile(accountNameH)
				if info != nil {
					accountName = string(info.AccountName)
				}
			}

			datasetName := getDatasetNameFromFile(name)
			collection := readDBFile(accountNameH, datasetName)
			if collection == nil {
				abort("CollectAllRootBlocks was called on a DB file which cannot be opened")
			}
			for _, e := range collection.States {
				if e.StateFlags&core.StateFlagInvalid == core.StateFlagInvalid {
					if skipInvalid {
						core.Log(core.LogWarning, "All data referenced by %s.%s.%x will be marked for removal unless referenced elsewhere", accountName, datasetName, e.State.StateID[:])
					} else {
						abort("Dataset %s.%s.%x is referencing data with a broken block chain", accountName, datasetName, e.State.StateID[:])
					}
				} else {
					rootBlocks = append(rootBlocks, BlockSource{BlockID: e.State.BlockID, StateID: e.State.StateID, DatasetName: datasetName, AccountNameH: accountNameH, AccountName: string(accountName)})
				}
			}
		}
	}
	return rootBlocks
}

func (handler *AccountHandler) RebuildAccountFiles() (rootBlocks []BlockSource) {
	// Open each dataset and check the chains
	dir, err := os.Open(filepath.Join(datDirectory, "account"))
	abortOn(err)
	defer dir.Close()
	dirlist, err := dir.Readdir(-1)
	abortOn(err)

	for _, info := range dirlist { // Clear all cached dataset information from the info files
		name := info.Name()
		if m, _ := filepath.Match("??????????????????????.info", name); m {
			// Read the accountNameH from the filename
			var accountNameH core.Byte128
			{
				decoded, err := base64.RawURLEncoding.DecodeString(name[:22])
				abortOn(err)
				accountNameH.Set(decoded)
			}

			info := readInfoFile(accountNameH)
			if info != nil {
				info.Datasets = nil
				writeInfoFile(accountNameH, *info)
			}
		}
	}

	for _, info := range dirlist {
		name := info.Name()
		if m, _ := filepath.Match("??????????????????????.??????????????????????.trn", name); m {
			// Read the accountNameH from the filename
			var accountName string
			var accountNameH core.Byte128
			{
				decoded, err := base64.RawURLEncoding.DecodeString(name[:22])
				abortOn(err)
				accountNameH.Set(decoded)
				info := readInfoFile(accountNameH)
				if info != nil {
					accountName = string(info.AccountName)
				}
			}

			datasetName := getDatasetNameFromFile(name)

			core.Log(core.LogDebug, "Regenerating file %s.db (%s.%s)", name[:45], accountName, datasetName)

			// Generate the DB file from transactions
			states := stateArrayFromTransactions(accountNameH, datasetName)
			sort.Sort(states)
			writeDBFile(accountNameH, datasetName, &dbStateCollection{States: states})

			for _, e := range states {
				rootBlocks = append(rootBlocks, BlockSource{BlockID: e.State.BlockID, StateID: e.State.StateID, DatasetName: datasetName, AccountNameH: accountNameH, AccountName: string(accountName)})
			}
		}
	}
	return rootBlocks
}

func (handler *AccountHandler) InvalidateDatasetState(accountNameH core.Byte128, datasetName core.String, stateID core.Byte128) {
	collection := readDBFile(accountNameH, datasetName)
	if collection == nil {
		abort("InvalidateDatasetState was called on a DB file which cannot be opened")
	}
	for i, s := range collection.States {
		if s.State.StateID.Compare(stateID) == 0 {
			collection.States[i].StateFlags |= core.StateFlagInvalid
			break
		}
	}
	writeDBFile(accountNameH, datasetName, collection)
}
