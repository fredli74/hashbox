//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package main

import (
	"sort"
	"sync"

	"github.com/fredli74/hashbox/pkg/accountdb"
	"github.com/fredli74/hashbox/pkg/core"
)

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
	store  *accountdb.Store
	signal chan error // goroutine signal channel, returns raised errors and stops goroutine when closed
	query  chan ChannelQuery
	wg     sync.WaitGroup
}

const (
	accounthandlerSetinfo = iota
	accounthandlerGetinfo
	accounthandlerListset
	accounthandlerAddset
	accounthandlerRemoveset
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
				case accounthandlerGetinfo:
					accountNameH := q.data.(core.Byte128)
					q.result <- handler.store.ReadInfoFile(accountNameH)

				case accounthandlerSetinfo:
					accountInfo := q.data.(accountdb.AccountInfo)
					accountNameH := core.Hash([]byte(accountInfo.AccountName))

					handler.store.WriteInfoFile(accountNameH, accountInfo)
					q.result <- true

				case accounthandlerListset:
					list := q.data.(queryListDataset)
					q.result <- handler.store.ReadDBFile(list.AccountNameH, list.DatasetName)

				case accounthandlerAddset:
					add := q.data.(queryAddDatasetState)

					handler.store.AppendAddState(add.AccountNameH, add.DatasetName, add.State)

					{ // update the db file
						collection := handler.store.ReadDBFile(add.AccountNameH, add.DatasetName)
						if collection != nil {
							for i, s := range collection.States {
								if s.State.StateID.Compare(add.State.StateID) == 0 {
									collection.States[i] = collection.States[len(collection.States)-1]
									collection.States = collection.States[:len(collection.States)-1]
									break
								}
							}
						} else {
							collection = &accountdb.DBStateCollection{}
						}
						collection.States = append(collection.States, core.DatasetStateEntry{State: add.State})
						sort.Sort(collection.States)
						handler.store.WriteDBFile(add.AccountNameH, add.DatasetName, collection)
					}

					q.result <- nil

				case accounthandlerRemoveset:
					del := q.data.(queryRemoveDatasetState)

					handler.store.AppendDelState(del.AccountNameH, del.DatasetName, del.StateID)

					{ // update the db file
						collection := handler.store.ReadDBFile(del.AccountNameH, del.DatasetName)
						if collection != nil {
							for i, s := range collection.States {
								if s.State.StateID.Compare(del.StateID) == 0 {
									copy(collection.States[i:], collection.States[i+1:])
									collection.States = collection.States[:len(collection.States)-1]
									break
								}
							}
						} else {
							collection = &accountdb.DBStateCollection{}
						}
						handler.store.WriteDBFile(del.AccountNameH, del.DatasetName, collection)
					}

					q.result <- nil

				default:
					core.Abort("Unknown query in AccountHandler causing hangup: %d", q.query)
				}
			}()
		case _, ok := <-handler.signal: // Signal is closed?
			// TODO: remove this check
			if ok {
				core.Abort("We should not reach this point, it means someone outside this goroutine sent a signal on the channel")
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
				core.Abort("AccountHandler panic: %v", err)
			}
		default:
			switch t := r.(type) {
			case error:
				core.Abort("AccountHandler panic: %v", t)
			}
		}
	}()
	handler.query <- q
	r := <-q.result
	return r
}

func (handler *AccountHandler) ListDataset(a core.Byte128, set core.String) *accountdb.DBStateCollection {
	q := ChannelQuery{accounthandlerListset, queryListDataset{a, set}, make(chan interface{}, 1)}
	return handler.doCommand(q).(*accountdb.DBStateCollection)
}

func (handler *AccountHandler) AddDatasetState(a core.Byte128, set core.String, state core.DatasetState) error {
	q := ChannelQuery{accounthandlerAddset, queryAddDatasetState{a, set, state}, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(error)
	}
	return nil
}
func (handler *AccountHandler) RemoveDatasetState(a core.Byte128, set core.String, stateID core.Byte128) error {
	q := ChannelQuery{accounthandlerRemoveset, queryRemoveDatasetState{a, set, stateID}, make(chan interface{}, 1)}
	r := handler.doCommand(q)
	if r != nil {
		return r.(error)
	}
	return nil
}
func (handler *AccountHandler) GetInfo(a core.Byte128) *accountdb.AccountInfo {
	q := ChannelQuery{accounthandlerGetinfo, a, make(chan interface{}, 1)}
	return handler.doCommand(q).(*accountdb.AccountInfo)
	// ToDO: test this with a read-error
}
func (handler *AccountHandler) SetInfo(a accountdb.AccountInfo) bool {
	q := ChannelQuery{accounthandlerSetinfo, a, make(chan interface{}, 1)}
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
		store:  accountdb.NewStore(datDirectory),
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

type BlockSource struct {
	BlockID      core.Byte128
	AccountName  string
	AccountNameH core.Byte128
	DatasetName  core.String
	StateID      core.Byte128
}

func (handler *AccountHandler) CollectAllRootBlocks(skipInvalid bool) (rootBlocks []BlockSource) {
	accounts, err := handler.store.ListAccounts()
	core.AbortOn(err)
	for _, acc := range accounts {
		datasets, err := handler.store.ListDatasets(&acc.AccountNameH)
		core.AbortOn(err)
		for _, ds := range datasets {
			collection := handler.store.ReadDBFile(ds.AccountNameH, ds.DatasetName)
			if collection == nil {
				core.Abort("CollectAllRootBlocks was called on a DB file which cannot be opened")
			}
			accountName := string(acc.AccountName)
			for _, e := range collection.States {
				if e.StateFlags&core.StateFlagInvalid == core.StateFlagInvalid {
					if skipInvalid {
						core.Log(core.LogWarning, "All data referenced by %s.%s.%x will be marked for removal unless referenced elsewhere", accountName, ds.DatasetName, e.State.StateID[:])
					} else {
						core.Abort("Dataset %s.%s.%x is referencing data with a broken block chain", accountName, ds.DatasetName, e.State.StateID[:])
					}
				} else {
					rootBlocks = append(rootBlocks, BlockSource{BlockID: e.State.BlockID, StateID: e.State.StateID, DatasetName: ds.DatasetName, AccountNameH: ds.AccountNameH, AccountName: accountName})
				}
			}
		}
	}
	return rootBlocks
}

func (handler *AccountHandler) RebuildAccountFiles() {
	accounts, err := handler.store.ListAccounts()
	core.AbortOn(err)
	for _, acc := range accounts {
		handler.store.RebuildAccount(acc.AccountNameH)
	}
}

func (handler *AccountHandler) InvalidateDatasetState(accountNameH core.Byte128, datasetName core.String, stateID core.Byte128) {
	collection := handler.store.ReadDBFile(accountNameH, datasetName)
	if collection == nil {
		core.Abort("InvalidateDatasetState was called on a DB file which cannot be opened")
	}
	for i, s := range collection.States {
		if s.State.StateID.Compare(stateID) == 0 {
			collection.States[i].StateFlags |= core.StateFlagInvalid
			break
		}
	}
	handler.store.WriteDBFile(accountNameH, datasetName, collection)
}
