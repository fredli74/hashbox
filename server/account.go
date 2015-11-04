//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/hashbox/core"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
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
			serverLog(r)
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

					// TODO: is is now moving the old dataset, should we do this?
					//old := readInfoFile(accountNameH)
					//if old != nil {
					//	if accountInfo.Datasets == nil {
					//		accountInfo.Datasets = old.Datasets
					//	}
					//}
					writeInfoFile(accountNameH, accountInfo)
					q.result <- true

				case accounthandler_listset:
					list := q.data.(queryListDataset)
					q.result <- readDBFile(list.AccountNameH, list.DatasetName)

				case accounthandler_addset:
					add := q.data.(queryAddDatasetState)

					result := appendDatasetTx(add.AccountNameH, add.DatasetName, dbTx{timestamp: uint64(time.Now().UnixNano()), txType: dbTxTypeAdd, data: add.State})
					// TODO: Just update the collection instead of redoing history each time
					generateDBFile(add.AccountNameH, add.DatasetName)
					q.result <- result

				case accounthandler_removeset:
					del := q.data.(queryRemoveDatasetState)

					result := appendDatasetTx(del.AccountNameH, del.DatasetName, dbTx{timestamp: uint64(time.Now().UnixNano()), txType: dbTxTypeDel, data: del.StateID})
					// TODO: Just update the collection instead of redoing history each time
					generateDBFile(del.AccountNameH, del.DatasetName)
					q.result <- result

				default:
					panic(errors.New(fmt.Sprintf("Unknown query in AccountHandler causing hangup: %d", q.query)))
				}
			}()
		case _, ok := <-handler.signal: // Signal is closed?
			// TODO: remove this check
			if ok {
				panic(errors.New("We should not reach this point, it means someone outside this goroutine sent a signal on the channel"))
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
				panic(errors.New("AccountHandler panic: " + err.Error()))
			}
		default:
			switch t := r.(type) {
			case error:
				panic(errors.New("AccountHandler panic: " + t.Error()))
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
	} else {
		return nil
	}
}
func (handler *AccountHandler) RemoveDatasetState(a core.Byte128, set core.String, stateID core.Byte128) error {
	q := ChannelQuery{accounthandler_removeset, queryRemoveDatasetState{a, set, stateID}, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(error)
	} else {
		return nil
	}
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

func (h dbFileHeader) Serialize(w io.Writer) {
	core.WriteOrPanic(w, h.filetype)
	core.WriteOrPanic(w, h.version)
	h.datasetName.Serialize(w)
}
func (h *dbFileHeader) Unserialize(r io.Reader) {
	core.ReadOrPanic(r, &h.filetype)
	core.ReadOrPanic(r, &h.version)
	if h.version != dbVersion {
		panic(errors.New("Invalid version in dbFileHeader"))
	}
	h.datasetName.Unserialize(r)
}

type dbTx struct {
	timestamp uint64
	txType    uint32
	data      interface{}
}

func (t dbTx) Serialize(w io.Writer) {
	core.WriteOrPanic(w, t.timestamp)
	core.WriteOrPanic(w, t.txType)
	t.data.(core.Serializer).Serialize(w)
}
func (t *dbTx) Unserialize(r io.Reader) {
	core.ReadOrPanic(r, &t.timestamp)
	core.ReadOrPanic(r, &t.txType)
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
		panic(errors.New("Corrupt transaction file"))
	}
}

type dbStateCollection struct {
	// datasetName is already in the file header
	Size   uint64       // Size of all data referenced by this dataset
	ListH  core.Byte128 // = md5(States)
	States core.DatasetStateArray
}

func (c dbStateCollection) Serialize(w io.Writer) {
	core.WriteOrPanic(w, c.Size)
	c.ListH.Serialize(w)
	c.States.Serialize(w)
}
func (c *dbStateCollection) Unserialize(r io.Reader) {
	core.ReadOrPanic(r, &c.Size)
	c.ListH.Unserialize(r)
	c.States.Unserialize(r)
}

func appendDatasetTx(accountNameH core.Byte128, datasetName core.String, tx dbTx) error {
	file, err := os.OpenFile(datasetFilename(accountNameH, string(datasetName))+dbFileExtensionTransaction, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	pos, err := file.Seek(0, 2)
	if pos == 0 { // New file, write the header
		header := dbFileHeader{filetype: dbFileTypeTransaction, version: dbVersion, datasetName: datasetName}
		header.Serialize(file)
	}
	tx.Serialize(file)
	return nil
}
func generateDBFile(accountNameH core.Byte128, datasetName core.String) {
	filename := datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
	file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	var header dbFileHeader
	header.Unserialize(file)
	if header.version != dbVersion || header.filetype != dbFileTypeTransaction {
		panic(errors.New(filename + " is not a valid transaction file"))
	}

	stateMap := make(map[core.Byte128]core.DatasetState)
	// Go through transaction history and populate the stateMap
	func() {
		defer func() {
			recover()
			return
		}()

		var pointInHistory uint64
		for {
			var tx dbTx
			tx.Unserialize(file)
			if tx.timestamp < pointInHistory {
				panic(errors.New(filename + " is corrupt, timestamp check failed"))
			}
			pointInHistory = tx.timestamp
			switch tx.txType {
			case dbTxTypeAdd:
				stateMap[tx.data.(core.DatasetState).StateID] = tx.data.(core.DatasetState)
			case dbTxTypeDel:
				delete(stateMap, tx.data.(core.Byte128))
			default:
				panic(errors.New(fmt.Sprintf("%s is corrupt, invalid transaction type found: %x", filename, tx.txType)))
			}
		}
	}()

	var collection dbStateCollection
	for _, s := range stateMap {
		collection.Size += s.UniqueSize // TODO: this calculation is wrong the moment you start deleting stuff, it needs to be reworked
		collection.States = append(collection.States, s)
	}
	sort.Sort(collection.States)

	hash := md5.New()
	for _, s := range collection.States {
		s.Serialize(hash)
	}
	copy(collection.ListH[:], hash.Sum(nil)[:16])
	writeDBFile(accountNameH, datasetName, collection)

	// Now also update account info
	info := readInfoFile(accountNameH)
	for i := 0; i <= len(info.Datasets); i++ {
		if i >= len(info.Datasets) {
			info.Datasets = append(info.Datasets, core.Dataset{Name: datasetName})
		}
		if info.Datasets[i].Name == datasetName {
			info.Datasets[i].ListH = collection.ListH
			info.Datasets[i].Size = collection.Size
			break
		}
	}
	writeInfoFile(accountNameH, *info)
}

func writeDBFile(accountNameH core.Byte128, datasetName core.String, c dbStateCollection) {
	file, err := os.OpenFile(datasetFilename(accountNameH, string(datasetName))+dbFileExtensionDatabase, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	header := dbFileHeader{filetype: dbFileTypeDatabase, version: dbVersion, datasetName: datasetName}
	header.Serialize(file)
	c.Serialize(file)
}
func readDBFile(accountNameH core.Byte128, datasetName core.String) *dbStateCollection {
	filename := datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionDatabase
	file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil
	}
	defer file.Close()
	var header dbFileHeader
	header.Unserialize(file)
	if header.version != dbVersion || header.filetype != dbFileTypeDatabase {
		panic(errors.New(filename + " is not a valid db file"))
	}
	var c dbStateCollection
	c.Unserialize(file)
	return &c
}
func writeInfoFile(accountNameH core.Byte128, a AccountInfo) {
	file, err := os.OpenFile(accountFilename(accountNameH)+".info", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	a.AccountName.Serialize(file)
	a.AccessKey.Serialize(file)
	a.Datasets.Serialize(file)
}
func readInfoFile(accountNameH core.Byte128) *AccountInfo {
	_ = "breakpoint"
	file, err := os.OpenFile(accountFilename(accountNameH)+".info", os.O_RDONLY, 0666)
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
	return filepath.Join(datDirectory, "account/"+name)
}
func datasetFilename(aH core.Byte128, dName string) string {
	dNameH := core.Hash([]byte(dName))
	return accountFilename(aH) + "." + base64filename(dNameH[:])
}
