package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fredli74/hashbox/pkg/accountdb"
	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/hashbox/pkg/storagedb"
)

// commandSet bundles the actual command implementations so main stays thin.
type commandSet struct {
	dataDir string
	idxDir  string

	queueBytes int64
	maxThreads int64
}

func newCommandSet(datDir, idxDir string) *commandSet {
	return &commandSet{dataDir: datDir, idxDir: idxDir}
}

func (c *commandSet) listAccounts() {
	store := accountdb.NewStore(c.dataDir)
	accounts, err := store.ListAccounts()
	core.AbortOnError(err, "list accounts: %v", err)

	sort.Slice(accounts, func(i, j int) bool { return accounts[i].AccountName < accounts[j].AccountName })

	for _, acc := range accounts {
		name := acc.AccountName
		if name == "" {
			name = core.String("<unknown>")
		}
		fmt.Printf("[%s] %s\t(%d datasets)\n", formatHash(acc.AccountNameH), core.Escape(name), len(acc.Datasets))
	}
}

func (c *commandSet) listDatasets(accountName string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOnError(err, "resolve account: %v", err)

	datasets, err := store.ListDatasets(&accountNameH)
	core.AbortOnError(err, "list datasets: %v", err)
	if len(datasets) == 0 {
		fmt.Println("No datasets found")
		return
	}

	sort.Slice(datasets, func(i, j int) bool { return datasets[i].DatasetName < datasets[j].DatasetName })

	fmt.Printf("Account: [%s] %s\n", formatHash(accountNameH), core.Escape(accName))
	for _, ds := range datasets {
		datasetHash := formatHash(ds.DatasetNameH)
		if collection := store.ReadDBFile(accountNameH, ds.DatasetName); collection != nil {
			if len(collection.States) > 0 {
				latest := collection.States[len(collection.States)-1]
				fmt.Printf("- [%s] %s states=%d size=%s latest=%x\n", datasetHash, core.Escape(ds.DatasetName), len(collection.States), compactHumanSize(collection.Size), latest.State.StateID[:])
				continue
			}
			fmt.Printf("- [%s] %s states=0\n", datasetHash, core.Escape(ds.DatasetName))
			continue
		}
		fmt.Printf("- [%s] %s\n", datasetHash, core.Escape(ds.DatasetName))
	}
}

func (c *commandSet) listStates(accountName string, dataset string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOnError(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accountNameH, dataset)
	core.AbortOnError(err, "resolve dataset: %v", err)

	txs := store.ReadTrnFile(accountNameH, datasetName)
	if len(txs) == 0 {
		fmt.Println("No states found")
		return
	}

	deleted := make(map[core.Byte128]bool)
	for _, tx := range txs {
		if tx.TxType == accountdb.DbTxTypeDel {
			deleted[tx.Data.(core.Byte128)] = true
		}
	}

	fmt.Printf("Account: [%s] %s\n", formatHash(accountNameH), core.Escape(accName))
	fmt.Printf("Dataset: [%s] %s\n", formatHash(core.Hash([]byte(datasetName))), core.Escape(datasetName))
	for _, tx := range txs {
		switch tx.TxType {
		case accountdb.DbTxTypeAdd:
			state := tx.Data.(core.DatasetState)
			isDeleted := deleted[state.StateID]
			if !showDeleted && isDeleted {
				continue
			}
			tsNote := time.Unix(0, tx.Timestamp).UTC().Format(time.RFC3339)
			flagNote := ""
			if isDeleted {
				flagNote = " (DELETED)"
			}
			fmt.Printf("%s + %x root=%x size=%s%s\n", tsNote, state.StateID[:], state.BlockID[:], compactHumanSize(state.Size), flagNote)
		case accountdb.DbTxTypeDel:
			if !showDeleted {
				continue
			}
			stateID := tx.Data.(core.Byte128)
			delTime := time.Unix(0, tx.Timestamp).UTC()
			fmt.Printf("%s - %x\n", delTime.Format(time.RFC3339), stateID[:])
		default:
			core.Abort("unknown tx type %x in %s", tx.TxType, datasetName)
		}
	}
}

func (c *commandSet) rebuildDB(account string) {
	store := accountdb.NewStore(c.dataDir)
	accounts, err := store.ListAccounts()
	core.AbortOnError(err, "list accounts: %v", err)
	datasetCount := 0
	accountCount := 0
	for _, acc := range accounts {
		if account != "" {
			want := core.Hash([]byte(account))
			if acc.AccountName != core.String(account) && acc.AccountNameH.Compare(want) != 0 {
				continue
			}
		}
		datasetCount += store.RebuildAccount(acc.AccountNameH)
		accountCount++
	}
	fmt.Printf("Rebuilt %d datasets across %d accounts\n", datasetCount, accountCount)
}

func (c *commandSet) showBlock(blockIDStr string, verify bool) {
	blockID, ok := parseHash(blockIDStr)
	if !ok {
		core.Abort("invalid block id %q", blockIDStr)
	}
	origLog := core.LogLevel
	core.LogLevel = core.LogWarning
	defer func() { core.LogLevel = origLog }()

	store := storagedb.NewStore(c.dataDir, c.idxDir)
	block := store.ReadBlock(blockID)
	if block == nil {
		core.Abort("block %x not found", blockID[:])
	}

	fmt.Printf("Block: %x\n", block.BlockID[:])
	compressed := block.CompressedSize
	if compressed <= 0 {
		compressed = block.Data.Len()
	}
	sizeLine := fmt.Sprintf("Size: %s", compactHumanSize(int64(compressed)))
	switch block.DataType {
	case core.BlockDataTypeZlib:
		sizeLine += " (zlib compressed)"
	case core.BlockDataTypeRaw:
		sizeLine += " (raw data)"
	}
	fmt.Println(sizeLine)
	if verify {
		var verifyErr interface{}
		ok := func() bool {
			defer func() {
				if r := recover(); r != nil {
					verifyErr = r
				}
			}()
			return block.VerifyBlock()
		}()
		if verifyErr != nil {
			fmt.Printf("Verify: FAILED (%v)\n", verifyErr)
		} else if ok {
			fmt.Printf("Verify: OK\n")
		} else {
			fmt.Printf("Verify: FAILED\n")
		}
	}
	fmt.Printf("Links (%d):\n", len(block.Links))
	for _, l := range block.Links {
		fmt.Printf("  %x\n", l[:])
	}
	block.Release()
}

func (c *commandSet) ping(host string, port int) {
	core.ASSERT(host != "", "host required")
	address := fmt.Sprintf("%s:%d", host, port)
	client := core.NewClient("", "", core.Byte128{})
	client.ServerAddress = address
	client.Dial()
	client.Handshake()
	client.Close(true)
}

func (c *commandSet) deleteStates(accountName, dataset string, stateIDStrs []string) {
	store := accountdb.NewStore(c.dataDir)
	_, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOnError(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accountNameH, dataset)
	core.AbortOnError(err, "resolve dataset: %v", err)
	if len(stateIDStrs) == 0 {
		core.Abort("at least one state id required")
	}
	for _, stateIDStr := range stateIDStrs {
		stateID, ok := parseHash(stateIDStr)
		if !ok {
			core.Abort("invalid state id %q", stateIDStr)
		}
		store.AppendDelState(accountNameH, datasetName, stateID)
	}
	store.RebuildDB(accountNameH, datasetName)
}

func (c *commandSet) deleteDataset(accountName, dataset string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOnError(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accountNameH, dataset)
	core.AbortOnError(err, "resolve dataset: %v", err)
	txPath := store.DatasetFilepath(accountNameH, datasetName) + accountdb.DbFileExtensionTransaction

	renameIfExists(txPath, txPath+".bak")
	dbPath := store.DatasetFilepath(accountNameH, datasetName) + accountdb.DbFileExtensionDatabase

	renameIfExists(dbPath, dbPath+".bak")
	store.RebuildAccount(accountNameH)
	fmt.Printf("Deleted dataset %s from account %s\n", core.Escape(datasetName), core.Escape(accName))
}

func (c *commandSet) deleteAccount(accountName string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOnError(err, "resolve account: %v", err)

	accountDir := filepath.Join(c.dataDir, "account")
	accPrefix := base64.RawURLEncoding.EncodeToString(accountNameH[:]) + "."
	entries, err := os.ReadDir(accountDir)
	core.AbortOnError(err, "read account dir: %v", err)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, accPrefix) || strings.HasSuffix(name, ".bak") {
			continue
		}
		full := filepath.Join(accountDir, name)
		renameIfExists(full, full+".bak")
	}

	fmt.Printf("Deleted account %s\n", core.Escape(accName))
}

func (c *commandSet) moveDataset(srcAccount, srcDataset, dstAccount, dstDataset string) {
	core.ASSERT(dstDataset != "", "destination dataset name cannot be empty")
	store := accountdb.NewStore(c.dataDir)
	_, srcAccH, err := resolveAccount(store, srcAccount)
	core.AbortOnError(err, "resolve src account: %v", err)
	srcDatasetName, err := resolveDatasetName(store, srcAccH, srcDataset)
	core.AbortOnError(err, "resolve src dataset: %v", err)
	_, dstAccH, err := resolveAccount(store, dstAccount)
	core.AbortOnError(err, "resolve dst account: %v", err)
	dstDatasetName := core.String(dstDataset)
	if resolved, err := resolveDatasetName(store, dstAccH, dstDataset); err == nil {
		dstDatasetName = resolved
	}
	if srcAccH.Compare(dstAccH) == 0 && srcDatasetName == dstDatasetName {
		core.Abort("source and destination are the same dataset")
	}

	srcTxs := store.ReadTrnFile(srcAccH, srcDatasetName)
	var dstTxs []accountdb.DbTx

	dstPath := store.DatasetFilepath(dstAccH, dstDatasetName) + accountdb.DbFileExtensionTransaction
	if _, err := os.Stat(dstPath); err == nil {
		dstTxs = store.ReadTrnFile(dstAccH, dstDatasetName)
		renameIfExists(dstPath, dstPath+".bak")
	}

	merged := make([]accountdb.DbTx, 0, len(srcTxs)+len(dstTxs))
	i, j := 0, 0
	for i < len(srcTxs) || j < len(dstTxs) {
		if j >= len(dstTxs) || (i < len(srcTxs) && srcTxs[i].Timestamp <= dstTxs[j].Timestamp) {
			merged = append(merged, srcTxs[i])
			i++
		} else {
			merged = append(merged, dstTxs[j])
			j++
		}
	}
	store.WriteTrnFile(dstAccH, dstDatasetName, merged)
	resetSyncWaterMarks(c.dataDir, dstAccH, dstDatasetName)
	store.RebuildDB(dstAccH, dstDatasetName)

	if srcAccH.Compare(dstAccH) != 0 || srcDatasetName != dstDatasetName {
		c.deleteDataset(srcAccount, string(srcDatasetName))
	}

	fmt.Printf("Moved dataset %s:%s -> %s:%s\n", core.Escape(srcAccount), core.Escape(srcDatasetName), core.Escape(dstAccount), core.Escape(dstDatasetName))
}

func (c *commandSet) purgeStates(accountName, dataset string) {
	store := accountdb.NewStore(c.dataDir)
	_, accH, err := resolveAccount(store, accountName)
	core.AbortOnError(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accH, dataset)
	core.AbortOnError(err, "resolve dataset: %v", err)

	txs := store.ReadTrnFile(accH, datasetName)
	path := store.DatasetFilepath(accH, datasetName) + accountdb.DbFileExtensionTransaction
	renameIfExists(path, path+".bak")

outer:
	for i := 0; i < len(txs); {
		tx := txs[i]
		switch tx.TxType {
		case accountdb.DbTxTypeDel:
			txs = append(txs[:i], txs[i+1:]...)
			continue
		case accountdb.DbTxTypeAdd:
			stateID := tx.Data.(core.DatasetState).StateID
			for j := i + 1; j < len(txs); j++ {
				if txs[j].TxType != accountdb.DbTxTypeDel {
					continue
				}
				delID := txs[j].Data.(core.Byte128)
				if (&delID).Compare(stateID) == 0 {
					txs = append(txs[:i], txs[i+1:]...)
					continue outer
				}
			}
			i++
		default:
			core.Abort("unknown tx type %x in purge for %s", tx.TxType, datasetName)
		}
	}

	store.WriteTrnFile(accH, datasetName, txs)
	resetSyncWaterMarks(c.dataDir, accH, datasetName)
	store.RebuildDB(accH, datasetName)
	fmt.Printf("Purged deleted states from dataset %s\n", core.Escape(datasetName))
}
