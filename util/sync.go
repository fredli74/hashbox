package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fredli74/hashbox/pkg/accountdb"
	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/hashbox/pkg/lockablefile"
	"github.com/fredli74/hashbox/pkg/storagedb"
)

func syncDir(dataPath string) string {
	return filepath.Join(dataPath, "sync")
}

func syncStatePath(dataPath, id string) string {
	return filepath.Join(syncDir(dataPath), fmt.Sprintf("state-%s.json", id))
}

func parsePatterns(spec string) []string {
	if spec == "" {
		return nil
	}
	parts := strings.Split(spec, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func buildSyncID(host string, port int) string {
	id := fmt.Sprintf("%s-%d", host, port)
	id = strings.ReplaceAll(id, ":", "_")
	id = strings.ReplaceAll(id, "/", "_")
	return id
}

func parsePort(input string) int {
	p, err := strconv.Atoi(input)
	core.AbortOn(err, "invalid port %q: %v", input, err)
	core.ASSERT(p > 0 && p < 65536, "port out of range")
	return p
}

func matchesPattern(acc, ds string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.Contains(p, ":") {
			parts := strings.SplitN(p, ":", 2)
			if parts[0] == acc && (parts[1] == "" || parts[1] == ds) {
				return true
			}
			continue
		}
		if p == acc {
			return true
		}
	}
	return false
}

func shouldInclude(acc, ds string, include, exclude []string) bool {
	if !matchesPattern(acc, ds, include) {
		return false
	}
	if matchesPattern(acc, ds, exclude) {
		return false
	}
	return true
}

func (c *commandSet) syncRun(remoteHost string, remotePort int, include, exclude []string, dryRun bool) {
	accountDB := accountdb.NewStore(c.dataDir)
	dataDB := storagedb.NewStore(c.dataDir, c.idxDir)
	defer dataDB.Close()

	sync := newSyncSession(accountDB, dataDB, remoteHost, remotePort)
	defer sync.close()

	if dryRun {
		sync.dryRun = true
	}

	accounts, err := accountDB.ListAccounts()
	core.AbortOn(err, "list accounts: %v", err)

	// for each account in local data/account, check if it should be included
	for _, acc := range accounts {
		accName := string(acc.AccountName)
		if !shouldInclude(accName, "", include, exclude) {
			continue
		}
		datasets, err := accountDB.ListDatasets(&acc.AccountNameH)
		core.AbortOn(err, "list datasets for %s: %v", accName, err)

		// for each dataset in account, check if it should be included
		for _, ds := range datasets {
			dsName := string(ds.DatasetName)
			if !shouldInclude(accName, dsName, include, exclude) {
				continue
			}
			fmt.Printf("sync %s:%s -> %s:%d\n", accName, dsName, remoteHost, remotePort)
			sync.processDataset(acc.AccountNameH, ds.DatasetName)
		}
	}
}

// ******** SYNC STATE HANDLING ********
//
// syncStateFilename = state-syncID.json
// state-syncID.json schema is {
//	[datasetHash: string]: position: int64 // EOF watermark for last processed transaction, we start reading from here on next sync
// }

func readStateFile(f *lockablefile.LockableFile, path string) map[string]int64 {
	_, err := f.Seek(0, io.SeekStart)
	core.AbortOn(err, "seek %s: %v", path, err)

	info, err := f.Stat()
	core.AbortOn(err, "stat %s: %v", path, err)
	if info.Size() == 0 {
		return make(map[string]int64)
	}

	var state map[string]int64
	dec := json.NewDecoder(f)
	core.AbortOn(dec.Decode(&state), "parse %s: %v", path, err)
	return state
}

func getSyncStateWatermark(dataPath, syncID string, datasetName core.String) int64 {
	path := syncStatePath(dataPath, syncID)
	f, err := lockablefile.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		core.Abort("open %s: %v", path, err)
	}
	defer f.Close()
	f.Lock()
	defer f.Unlock()

	state := readStateFile(f, path)
	key := formatHash(core.Hash([]byte(datasetName)))
	if v, ok := state[key]; ok {
		return v
	}
	return 0
}

func setSyncStateWatermark(dataPath, syncID string, datasetName core.String, position int64) {
	path := syncStatePath(dataPath, syncID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		core.Abort("mkdir %s: %v", filepath.Dir(path), err)
	}
	f, err := lockablefile.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	core.AbortOn(err, "open %s: %v", path, err)
	defer f.Close()
	f.Lock()
	defer f.Unlock()
	state := readStateFile(f, path)
	key := formatHash(core.Hash([]byte(datasetName)))
	state[key] = position

	// Rewind and write updated state
	_, err = f.Seek(0, io.SeekStart)
	core.AbortOn(err, "seek %s: %v", path, err)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")
	err = enc.Encode(state)
	core.AbortOn(err, "write %s: %v", path, err)

	// Truncate in case new content is smaller than old
	offset, err := f.Seek(0, io.SeekCurrent)
	core.AbortOn(err, "seek %s: %v", path, err)
	err = f.Truncate(offset)
	core.AbortOn(err, "truncate %s: %v", path, err)
}

/******** SYNC SESSION ********/

type syncSession struct {
	syncID     string
	accountDB  *accountdb.Store
	dataDB     *storagedb.Store
	dryRun     bool
	client     *core.Client
	remoteHost string
	remotePort int
}

func newSyncSession(accountDB *accountdb.Store, dataDB *storagedb.Store, remoteHost string, remotePort int) *syncSession {
	return &syncSession{
		syncID:     buildSyncID(remoteHost, remotePort),
		accountDB:  accountDB,
		dataDB:     dataDB,
		dryRun:     false,
		remoteHost: remoteHost,
		remotePort: remotePort,
	}
}

func (sync *syncSession) processDataset(accountHash core.Byte128, datasetName core.String) {
	// open local dataset transaction log
	reader, err := sync.accountDB.NewTxReader(accountHash, datasetName)
	core.AbortOn(err, "open trn %s:%s", sync.accountName(accountHash), datasetName)
	defer reader.Close()

	position := getSyncStateWatermark(sync.dataDB.DataDir, sync.syncID, datasetName)
	if position > 0 {
		if _, err := reader.Seek(position, io.SeekStart); err != nil {
			core.Abort("seek trn %s:%s: %v", sync.accountName(accountHash), datasetName, err)
		}
	}
	for {
		tx := reader.Next()
		if tx == nil {
			break
		}
		switch tx.TxType {
		case accountdb.DbTxTypeDel:
			stateID := tx.Data.(core.Byte128)
			sync.sendDeleteTransaction(accountHash, datasetName, stateID, tx.Timestamp)
		case accountdb.DbTxTypeAdd:
			stateObj := tx.Data.(core.DatasetState)
			if sync.hasLaterDelete(reader, stateObj.StateID) {
				continue
			}
			sync.sendAddTransaction(accountHash, datasetName, stateObj)
		default:
			core.Abort("unknown tx type %x in sync for %s:%s", tx.TxType, sync.accountName(accountHash), datasetName)
		}
		// Successfully processed tx, update watermark
		if !sync.dryRun {
			pos, err := reader.Pos()
			core.AbortOn(err, "pos trn %s:%s: %v", sync.accountName(accountHash), datasetName, err)
			setSyncStateWatermark(sync.dataDB.DataDir, sync.syncID, datasetName, pos)
		}
	}
}

func (sync *syncSession) hasLaterDelete(reader *accountdb.TxReader, stateID core.Byte128) bool {
	returnPos, err := reader.Pos()
	core.AbortOn(err, "seek trn")
	defer func() {
		_, _ = reader.Seek(returnPos, io.SeekStart)
	}()
	for {
		tx := reader.Next()
		if tx == nil {
			return false
		}
		if tx.TxType != accountdb.DbTxTypeDel {
			continue
		}
		delID := tx.Data.(core.Byte128)
		if (&delID).Compare(stateID) == 0 {
			return true
		}
	}
}

func (sync *syncSession) ensureConnection(accHash core.Byte128) {
	if sync.client != nil && sync.client.AccountNameH.Compare(accHash) != 0 {
		sync.closeConnection()
	}
	if sync.client == nil {
		info := sync.accountDB.ReadInfoFile(accHash)
		if info == nil {
			core.Abort("account info missing for %x", accHash[:])
		}
		address := fmt.Sprintf("%s:%d", sync.remoteHost, sync.remotePort)
		sync.client = core.NewClient(address, string(info.AccountName), info.AccessKey)
	}
}

func (sync *syncSession) sendDeleteTransaction(accountHash core.Byte128, datasetName core.String, stateID core.Byte128, ts int64) {
	sync.ensureConnection(accountHash)
	if !sync.dryRun {
		sync.client.RemoveDatasetState(string(datasetName), stateID)
	}
}

func (sync *syncSession) sendAddTransaction(accountHash core.Byte128, datasetName core.String, state core.DatasetState) {
	sync.ensureConnection(accountHash)
	sync.sendBlockTree(accountHash, datasetName, state.BlockID)
	if !sync.dryRun {
		sync.client.AddDatasetState(string(datasetName), state)
	}
}

func (sync *syncSession) sendBlockTree(accountHash core.Byte128, datasetName core.String, root core.Byte128) {
	queue := []core.Byte128{root}
	i := 0
	for i >= 0 {
		var b core.Byte128
		if i < len(queue) {
			b = queue[i]
			if sync.client.VerifyBlock(b) {
				queue = append(queue[:i], queue[i+1:]...)
				continue
			}
			meta := sync.dataDB.ReadBlockMeta(b)
			if meta == nil {
				core.Abort("block %x metadata missing locally", b[:])
			}
			links := meta.Links
			if len(links) > 0 {
				queue = append(queue, make([]core.Byte128, len(links))...)
				copy(queue[i+len(links)+1:], queue[i+1:])
				copy(queue[i+1:], links)
				i++
				continue
			}
			// No links, fall through to send
		} else {
			i = len(queue) - 1
			b = queue[i]
			// draining queue, send block
		}

		data := sync.dataDB.ReadBlock(b)
		if data == nil {
			core.Abort("block %x missing locally", b[:])
		}
		if !sync.dryRun {
			sync.client.StoreBlock(data)
		}
		data.Release()
		queue = append(queue[:i], queue[i+1:]...)
	}
}
func (sync *syncSession) close() {
	if sync.client != nil {
		sync.client.Close(true)
		sync.client = nil
	}
}

func (sync *syncSession) closeConnection() {
	if sync.client != nil {
		sync.client.Close(true)
		sync.client = nil
	}
}

func (sync *syncSession) accountName(accHash core.Byte128) string {
	info := sync.accountDB.ReadInfoFile(accHash)
	if info == nil {
		return formatHash(accHash)
	}
	return string(info.AccountName)
}
