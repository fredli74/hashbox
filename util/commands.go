package main

import (
	"fmt"
	"sort"

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

func (c *commandSet) listAccounts() error {
	store := accountdb.NewStore(c.dataDir)
	accounts, err := store.ListAccounts()
	if err != nil {
		return err
	}

	sort.Slice(accounts, func(i, j int) bool { return accounts[i].AccountName < accounts[j].AccountName })

	for _, acc := range accounts {
		name := acc.AccountName
		if name == "" {
			name = core.String("<unknown>")
		}
		fmt.Printf("[#%s] %s\t(%d datasets)\n", formatHash(acc.AccountNameH), escapeControls(string(name)), len(acc.Datasets))
	}
	return nil
}

func (c *commandSet) listDatasets(accountName string) error {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	if err != nil {
		return err
	}

	datasets, err := store.ListDatasets(&accountNameH)
	if err != nil {
		return err
	}
	if len(datasets) == 0 {
		fmt.Println("No datasets found")
		return nil
	}

	sort.Slice(datasets, func(i, j int) bool { return datasets[i].DatasetName < datasets[j].DatasetName })

	fmt.Printf("Account: [#%s] %s\n", formatHash(accountNameH), escapeControls(string(accName)))
	for _, ds := range datasets {
		datasetHash := formatHash(core.Hash([]byte(ds.DatasetName)))
		if collection := store.ReadDBFile(accountNameH, ds.DatasetName); collection != nil {
			if len(collection.States) > 0 {
				latest := collection.States[len(collection.States)-1]
				fmt.Printf("- [#%s] %s states=%d size=%s latest=%x\n", datasetHash, escapeControls(string(ds.DatasetName)), len(collection.States), compactHumanSize(collection.Size), latest.State.StateID[:])
				continue
			}
			fmt.Printf("- [#%s] %s states=0\n", datasetHash, escapeControls(string(ds.DatasetName)))
			continue
		}
		fmt.Printf("- [#%s] %s\n", datasetHash, escapeControls(string(ds.DatasetName)))
	}
	return nil
}

func (c *commandSet) listStates(accountName string, dataset string) error {
	store := accountdb.NewStore(c.dataDir)
	accName, accountNameH, err := resolveAccount(store, accountName)
	if err != nil {
		return err
	}
	datasetName, err := resolveDatasetName(store, accountNameH, dataset)
	if err != nil {
		return err
	}

	var states core.DatasetStateArray
	if collection := store.ReadDBFile(accountNameH, datasetName); collection != nil {
		states = collection.States
	} else {
		states = store.StateArrayFromTransactions(accountNameH, datasetName)
	}
	if len(states) == 0 {
		fmt.Println("No states found")
		return nil
	}
	sort.Sort(states)
	fmt.Printf("Account: [#%s] %s\n", formatHash(accountNameH), escapeControls(string(accName)))
	fmt.Printf("Dataset: [#%s] %s\n", formatHash(core.Hash([]byte(datasetName))), escapeControls(string(datasetName)))
	for _, s := range states {
		flagNote := ""
		if s.StateFlags&core.StateFlagInvalid != 0 {
			flagNote = " INVALID!"
		}
		fmt.Printf("%x size=%s root=%x%s\n", s.State.StateID[:], compactHumanSize(s.State.Size), s.State.BlockID[:], flagNote)
	}
	return nil
}

func (c *commandSet) rebuildDB() error {
	store := accountdb.NewStore(c.dataDir)
	accounts, err := store.ListAccounts()
	if err != nil {
		return err
	}
	datasetCount := 0
	stateCount := 0
	for _, acc := range accounts {
		info := store.ReadInfoFile(acc.AccountNameH)
		if info == nil {
			return fmt.Errorf("account info missing for %s", acc.Filename)
		}
		datasets, err := store.ListDatasets(&acc.AccountNameH)
		if err != nil {
			return fmt.Errorf("list datasets for %s: %w", info.AccountName, err)
		}
		for _, ds := range datasets {
			states := store.RebuildDB(acc.AccountNameH, ds.DatasetName)
			datasetCount++
			stateCount += len(states)
		}
	}
	fmt.Printf("Rebuilt %d datasets across %d accounts (%d states)\n", datasetCount, len(accounts), stateCount)
	return nil
}

func (c *commandSet) showBlock(blockIDStr string) error {
	blockID, ok := parseHash(blockIDStr)
	if !ok {
		return fmt.Errorf("invalid block id %q", blockIDStr)
	}
	origLog := core.LogLevel
	core.LogLevel = core.LogWarning
	defer func() { core.LogLevel = origLog }()

	store := storagedb.NewStore(c.dataDir, c.idxDir)
	block := store.ReadBlock(blockID)
	if block == nil {
		return fmt.Errorf("block %x not found", blockID[:])
	}

	fmt.Printf("Block: %x\n", block.BlockID[:])
	fmt.Printf("Type: %s\n", blockTypeString(block.DataType))
	compressed := block.CompressedSize
	if compressed <= 0 {
		compressed = block.Data.Len()
	}
	fmt.Printf("Compressed: %s\n", compactHumanSize(int64(compressed)))
	if block.UncompressedSize > 0 {
		fmt.Printf("Uncompressed: %s\n", compactHumanSize(int64(block.UncompressedSize)))
	}
	fmt.Printf("Links (%d):\n", len(block.Links))
	for _, l := range block.Links {
		fmt.Printf("  %x\n", l[:])
	}
	block.Release()
	return nil
}
