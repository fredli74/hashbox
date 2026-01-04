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
}

func newCommandSet(datDir, idxDir string) *commandSet {
	return &commandSet{dataDir: datDir, idxDir: idxDir}
}

func (c *commandSet) listAccounts() {
	store := accountdb.NewStore(c.dataDir)
	accounts, err := store.ListAccounts()
	core.AbortOn(err, "list accounts: %v", err)

	sort.Slice(accounts, func(i, j int) bool { return accounts[i].AccountName < accounts[j].AccountName })

	for _, acc := range accounts {
		name := acc.AccountName
		if name == "" {
			name = core.String("<unknown>")
		}
		fmt.Printf("[%s] %s\t(%d datasets)\n", formatHash(acc.AccountNameH), escapeControls(string(name)), len(acc.Datasets))
	}
}

func (c *commandSet) listDatasets(accountName string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOn(err, "resolve account: %v", err)

	datasets, err := store.ListDatasets(&accountNameH)
	core.AbortOn(err, "list datasets: %v", err)
	if len(datasets) == 0 {
		fmt.Println("No datasets found")
		return
	}

	sort.Slice(datasets, func(i, j int) bool { return datasets[i].DatasetName < datasets[j].DatasetName })

	fmt.Printf("Account: [%s] %s\n", formatHash(accountNameH), escapeControls(string(accName)))
	for _, ds := range datasets {
		datasetHash := formatHash(core.Hash([]byte(ds.DatasetName)))
		if collection := store.ReadDBFile(accountNameH, ds.DatasetName); collection != nil {
			if len(collection.States) > 0 {
				latest := collection.States[len(collection.States)-1]
				fmt.Printf("- [%s] %s states=%d size=%s latest=%x\n", datasetHash, escapeControls(string(ds.DatasetName)), len(collection.States), compactHumanSize(collection.Size), latest.State.StateID[:])
				continue
			}
			fmt.Printf("- [%s] %s states=0\n", datasetHash, escapeControls(string(ds.DatasetName)))
			continue
		}
		fmt.Printf("- [%s] %s\n", datasetHash, escapeControls(string(ds.DatasetName)))
	}
}

func (c *commandSet) listStates(accountName string, dataset string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOn(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accountNameH, dataset)
	core.AbortOn(err, "resolve dataset: %v", err)

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

	fmt.Printf("Account: [%s] %s\n", formatHash(accountNameH), escapeControls(string(accName)))
	fmt.Printf("Dataset: [%s] %s\n", formatHash(core.Hash([]byte(datasetName))), escapeControls(string(datasetName)))
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

func (c *commandSet) rebuildDB(account, dataset string) {
	store := accountdb.NewStore(c.dataDir)
	accounts, err := store.ListAccounts()
	core.AbortOn(err, "list accounts: %v", err)
	datasetCount := 0
	stateCount := 0
	for _, acc := range accounts {
		if account != "" {
			want := core.Hash([]byte(account))
			if acc.AccountName != core.String(account) && acc.AccountNameH.Compare(want) != 0 {
				continue
			}
		}
		info := store.ReadInfoFile(acc.AccountNameH)
		if info == nil {
			core.Abort("account info missing for %s", acc.Filename)
		}
		datasets, err := store.ListDatasets(&acc.AccountNameH)
		core.AbortOn(err, "list datasets for %s: %v", info.AccountName, err)
		for _, ds := range datasets {
			if dataset != "" {
				want := core.Hash([]byte(dataset))
				h := core.Hash([]byte(ds.DatasetName))
				if ds.DatasetName != core.String(dataset) && h.Compare(want) != 0 {
					continue
				}
			}
			states := store.RebuildDB(acc.AccountNameH, ds.DatasetName)
			datasetCount++
			stateCount += len(states)
		}
	}
	fmt.Printf("Rebuilt %d datasets across %d accounts (%d states)\n", datasetCount, len(accounts), stateCount)
}

func (c *commandSet) showBlock(blockIDStr string) {
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
	if block.DataType == core.BlockDataTypeZlib {
		sizeLine += " (zlib compressed)"
	} else if block.DataType == core.BlockDataTypeRaw {
		sizeLine += " (raw data)"
	}
	fmt.Println(sizeLine)
	fmt.Printf("Links (%d):\n", len(block.Links))
	for _, l := range block.Links {
		fmt.Printf("  %x\n", l[:])
	}
	block.Release()
}

func (c *commandSet) deleteState(accountName, dataset, stateIDStr string) {
	store := accountdb.NewStore(c.dataDir)
	_, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOn(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accountNameH, dataset)
	core.AbortOn(err, "resolve dataset: %v", err)
	stateID, ok := parseHash(stateIDStr)
	if !ok {
		core.Abort("invalid state id %q", stateIDStr)
	}
	store.AppendDelState(accountNameH, datasetName, stateID)
	store.RebuildDB(accountNameH, datasetName)
}

func (c *commandSet) deleteDataset(accountName, dataset string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOn(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accountNameH, dataset)
	core.AbortOn(err, "resolve dataset: %v", err)
	txPath := store.DatasetFilename(accountNameH, datasetName) + accountdb.DbFileExtensionTransaction

	renameIfExists(txPath, txPath+".bak")
	dbPath := store.DatasetFilename(accountNameH, datasetName) + accountdb.DbFileExtensionDatabase

	renameIfExists(dbPath, dbPath+".bak")
	store.RebuildAccount(accountNameH)
	fmt.Printf("Deleted dataset %s from account %s\n", escapeControls(string(datasetName)), escapeControls(string(accName)))
}

func (c *commandSet) deleteAccount(accountName string) {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	core.AbortOn(err, "resolve account: %v", err)

	accountDir := filepath.Join(c.dataDir, "account")
	accPrefix := base64.RawURLEncoding.EncodeToString(accountNameH[:]) + "."
	entries, err := os.ReadDir(accountDir)
	core.AbortOn(err, "read account dir: %v", err)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, accPrefix) || strings.HasSuffix(name, ".bak") {
			continue
		}
		full := filepath.Join(accountDir, name)
		renameIfExists(full, full+".bak")
	}

	fmt.Printf("Deleted account %s\n", escapeControls(string(accName)))
}

func (c *commandSet) moveDataset(srcAccount, srcDataset, dstAccount, dstDataset string) {
	core.ASSERT(dstDataset != "", "destination dataset name cannot be empty")
	store := accountdb.NewStore(c.dataDir)
	_, srcAccH, err := resolveAccount(store, srcAccount)
	core.AbortOn(err, "resolve src account: %v", err)
	srcDatasetName, err := resolveDatasetName(store, srcAccH, srcDataset)
	core.AbortOn(err, "resolve src dataset: %v", err)
	_, dstAccH, err := resolveAccount(store, dstAccount)
	core.AbortOn(err, "resolve dst account: %v", err)
	dstDatasetName := core.String(dstDataset)
	if resolved, err := resolveDatasetName(store, dstAccH, dstDataset); err == nil {
		dstDatasetName = resolved
	}

	srcTxs := store.ReadTrnFile(srcAccH, srcDatasetName)
	var dstTxs []accountdb.DbTx

	dstPath := store.DatasetFilename(dstAccH, dstDatasetName) + accountdb.DbFileExtensionTransaction
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
	store.RebuildDB(dstAccH, dstDatasetName)

	if srcAccH.Compare(dstAccH) != 0 || srcDatasetName != dstDatasetName {
		c.deleteDataset(srcAccount, string(srcDatasetName))
	}

	fmt.Printf("Moved dataset %s:%s -> %s:%s\n", escapeControls(string(srcAccount)), escapeControls(string(srcDatasetName)), escapeControls(string(dstAccount)), escapeControls(string(dstDatasetName)))
}

func (c *commandSet) purgeStates(accountName, dataset string) {
	store := accountdb.NewStore(c.dataDir)
	_, accH, err := resolveAccount(store, accountName)
	core.AbortOn(err, "resolve account: %v", err)
	datasetName, err := resolveDatasetName(store, accH, dataset)
	core.AbortOn(err, "resolve dataset: %v", err)

	txs := store.ReadTrnFile(accH, datasetName)
	path := store.DatasetFilename(accH, datasetName) + accountdb.DbFileExtensionTransaction
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
	store.RebuildDB(accH, datasetName)
	fmt.Printf("Purged deleted states from dataset %s\n", escapeControls(string(datasetName)))
}
