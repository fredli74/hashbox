package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	if len(exclude) > 0 && matchesPattern(acc, ds, exclude) {
		return false
	}
	return true
}

func (c *commandSet) syncRun(remoteHost string, remotePort int, include, exclude []string, dryRun bool) {
	accountDB := accountdb.NewStore(c.dataDir)
	dataDB := storagedb.NewStore(c.dataDir, c.idxDir)
	defer dataDB.Close()

	sync := newSyncSession(accountDB, dataDB, remoteHost, remotePort)
	sync.queueBytes = c.queueBytes
	sync.maxThreads = c.maxThreads
	sync.start = time.Now()
	defer sync.close()

	if dryRun {
		sync.dryRun = true
	}

	core.Log(core.LogDebug, "Syncing started for remote server %s:%d", remoteHost, remotePort)

	accounts, err := accountDB.ListAccounts()
	core.AbortOn(err, "list accounts: %v", err)
	if len(accounts) == 0 {
		core.Log(core.LogWarning, "no accounts found under %s/account", c.dataDir)
		return
	}
	core.Log(core.LogDebug, "found %d accounts", len(accounts))

	// for each account in local data/account, check if it should be included
	for _, acc := range accounts {
		accName := string(acc.AccountName)
		accDisp := escapeControls(accName)
		core.Log(core.LogTrace, "consider account %s (%s)", accDisp, formatHash(acc.AccountNameH))
		if !shouldInclude(accName, "", include, exclude) {
			core.Log(core.LogTrace, "skip account %s (filters)", accDisp)
			continue
		}
		core.Log(core.LogDebug, "match account %s", accDisp)
		datasets, err := accountDB.ListDatasets(&acc.AccountNameH)
		core.AbortOn(err, "list datasets for %s: %v", accDisp, err)
		core.Log(core.LogDebug, "found %d datasets for %s", len(datasets), accDisp)

		// for each dataset in account, check if it should be included
		for _, ds := range datasets {
			dsName := string(ds.DatasetName)
			dsDisp := escapeControls(dsName)
			core.Log(core.LogTrace, "consider dataset %s:%s", accDisp, dsDisp)
			if !shouldInclude(accName, dsName, include, exclude) {
				core.Log(core.LogTrace, "skip dataset %s:%s (filters)", accDisp, dsDisp)
				continue
			}
			core.Log(core.LogDebug, "match dataset %s:%s", accDisp, dsDisp)
			core.Log(core.LogInfo, "Syncing dataset %s:%s to %s:%d", accDisp, dsDisp, remoteHost, remotePort)
			sync.processDataset(acc.AccountNameH, ds.DatasetName)
		}
	}
	core.Log(core.LogInfo, "Sync summary: added %d states, deleted %d states", sync.addedStates, sync.deletedStates)
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
			core.Log(core.LogTrace, "state file missing for %s, defaulting offset=0", path)
			return 0
		}
		core.Abort("open %s: %v", path, err)
	}
	defer f.Close()
	f.LockShared()
	defer f.Unlock()

	state := readStateFile(f, path)
	key := formatHash(core.Hash([]byte(datasetName)))
	if v, ok := state[key]; ok {
		core.Log(core.LogTrace, "state offset for %s[%s]=%d", path, datasetName, v)
		return v
	}
	core.Log(core.LogTrace, "state offset for %s[%s] not found, defaulting offset=0", path, datasetName)
	return 0
}

func setSyncStateWatermark(dataPath, syncID string, datasetName core.String, position int64) {
	path := syncStatePath(dataPath, syncID)
	err := os.MkdirAll(filepath.Dir(path), 0o755)
	core.AbortOn(err, "mkdir %s: %v", filepath.Dir(path), err)
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

	start    time.Time
	statTick time.Duration

	queueBytes int64
	maxThreads int64

	addedStates   int64
	deletedStates int64
}

func newSyncSession(accountDB *accountdb.Store, dataDB *storagedb.Store, remoteHost string, remotePort int) *syncSession {
	return &syncSession{
		syncID:     buildSyncID(remoteHost, remotePort),
		accountDB:  accountDB,
		dataDB:     dataDB,
		dryRun:     false,
		remoteHost: remoteHost,
		remotePort: remotePort,
		statTick:   10 * time.Second,
	}
}

type remoteStateCache struct {
	session   *syncSession
	states    map[core.Byte128]bool
	fetchedAt time.Time
}

func newRemoteStateCache(session *syncSession) *remoteStateCache {
	return &remoteStateCache{session: session}
}

func (cache *remoteStateCache) hasState(accountHash core.Byte128, datasetName core.String, stateID core.Byte128) bool {
	cache.session.ensureConnection(accountHash)
	if cache.states != nil && !cache.fetchedAt.IsZero() && time.Since(cache.fetchedAt) < time.Minute {
		return cache.states[stateID]
	}
	list := cache.session.client.ListDataset(string(datasetName))
	states := make(map[core.Byte128]bool, len(list.States))
	for _, entry := range list.States {
		states[entry.State.StateID] = true
	}
	cache.states = states
	cache.fetchedAt = time.Now()
	return cache.states[stateID]
}

func (cache *remoteStateCache) invalidate() {
	cache.states = nil
	cache.fetchedAt = time.Time{}
}

func (sync *syncSession) processDataset(accountHash core.Byte128, datasetName core.String) {
	remoteStateCache := newRemoteStateCache(sync)

	// open local dataset transaction log
	reader, err := sync.accountDB.NewTxReader(accountHash, datasetName)
	core.AbortOn(err, "open trn %s:%s", sync.accountName(accountHash), datasetName)
	defer reader.Close()

	position := getSyncStateWatermark(sync.dataDB.DataDir, sync.syncID, datasetName)
	core.Log(core.LogDebug, "sync start %s:%s from offset %d", escapeControls(sync.accountName(accountHash)), escapeControls(string(datasetName)), position)
	if position > 0 {
		_, err := reader.Seek(position, io.SeekStart)
		core.AbortOn(err, "seek trn %s:%s: %v", sync.accountName(accountHash), datasetName, err)
	}
	for {
		tx := reader.Next()
		if tx == nil {
			break
		}
		switch tx.TxType {
		case accountdb.DbTxTypeDel:
			stateID := tx.Data.(core.Byte128)
			if !remoteStateCache.hasState(accountHash, datasetName, stateID) {
				core.Log(core.LogInfo, "Skipping delete %s:%s stateID %x (remote missing)", escapeControls(sync.accountName(accountHash)), escapeControls(string(datasetName)), stateID[:])
				continue
			}
			sync.sendDeleteTransaction(accountHash, datasetName, stateID, tx.Timestamp)
			remoteStateCache.invalidate()
		case accountdb.DbTxTypeAdd:
			stateObj := tx.Data.(core.DatasetState)
			if sync.hasLaterDelete(reader, stateObj.StateID) {
				continue
			}
			if remoteStateCache.hasState(accountHash, datasetName, stateObj.StateID) {
				core.Log(core.LogInfo, "Skipping add %s:%s stateID %x (remote exists)", escapeControls(sync.accountName(accountHash)), escapeControls(string(datasetName)), stateObj.StateID[:])
				continue
			}
			sync.sendAddTransaction(accountHash, datasetName, stateObj)
			remoteStateCache.invalidate()
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
		sync.client.RetryMax = 3
		if sync.queueBytes > 0 {
			sync.client.QueueMax = sync.queueBytes
		}
		if sync.maxThreads > 0 {
			sync.client.ThreadMax = int32(sync.maxThreads)
		}
	}
}

func (sync *syncSession) sendDeleteTransaction(accountHash core.Byte128, datasetName core.String, stateID core.Byte128, ts int64) {
	sync.ensureConnection(accountHash)
	core.Log(core.LogInfo, "Deleting %s:%s stateID %x", escapeControls(sync.accountName(accountHash)), escapeControls(string(datasetName)), stateID[:])
	if !sync.dryRun {
		sync.client.RemoveDatasetState(string(datasetName), stateID)
		sync.deletedStates++
	}
}

func (sync *syncSession) sendAddTransaction(accountHash core.Byte128, datasetName core.String, state core.DatasetState) {
	sync.ensureConnection(accountHash)
	core.Log(core.LogInfo, "Sending block tree %s:%s stateID=%x root=%x size=%s", escapeControls(sync.accountName(accountHash)), escapeControls(string(datasetName)), state.StateID[:], state.BlockID[:], core.CompactHumanSize(state.Size))
	sync.sendBlockTree(state.BlockID)
	if !sync.dryRun {
		core.Log(core.LogTrace, "commit blocks before dataset state %s:%s state=%x", escapeControls(sync.accountName(accountHash)), escapeControls(string(datasetName)), state.StateID[:])
		sync.client.Commit()
		core.Log(core.LogInfo, "Adding dataset state %s:%s stateID %x to remote server", escapeControls(sync.accountName(accountHash)), escapeControls(string(datasetName)), state.StateID[:])
		sync.client.AddDatasetState(string(datasetName), state)
		sync.addedStates++
	}
}

func (sync *syncSession) sendBlockTree(root core.Byte128) {
	var skipped int32 = 0
	var sent int32 = 0
	var sentBytes int64 = 0
	queue := []core.Byte128{root}
	index := 0
	type progressEvent struct {
		action   string
		block    core.Byte128
		index    int
		queueLen int
		skipped  int32
		sent     int32
	}
	progressCh := make(chan progressEvent, 64)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		defer close(progressCh)
		for {
			if len(queue) == 0 {
				return
			}
			var b core.Byte128
			if index < len(queue) {
				b = queue[index]
				core.Log(core.LogTrace, "queue descend idx=%d size=%d (%d/%d) head=%x", index, len(queue), index+1, len(queue), b[:])
				if sync.client.VerifyBlock(b) {
					core.Log(core.LogTrace, "skip existing block %x (queue=%d)", b[:], len(queue))
					queue = append(queue[:index], queue[index+1:]...)
					skipped++
					progressCh <- progressEvent{action: "-", block: b, index: index, queueLen: len(queue), skipped: skipped, sent: sent}
					continue
				}
				meta := sync.dataDB.ReadBlockMeta(b)
				if meta == nil {
					core.Abort("block %x metadata missing locally", b[:])
				}
				links := meta.Links
				if len(links) > 0 {
					core.Log(core.LogDebug, "enqueue children for %x (links=%d queue=%d)", b[:], len(links), len(queue))
					queue = append(queue, make([]core.Byte128, len(links))...)
					copy(queue[index+len(links)+1:], queue[index+1:])
					copy(queue[index+1:], links)
					index++
					progressCh <- progressEvent{action: "*", block: b, index: index, queueLen: len(queue), skipped: skipped, sent: sent}
					continue
				}
				// No links, fall through to send
			} else {
				// draining queue, send block
				index = len(queue) - 1
				b = queue[index]
				core.Log(core.LogTrace, "queue unwind idx=%d size=%d (%d/%d) head=%x", index, len(queue), index+1, len(queue), b[:])
			}
			data := sync.dataDB.ReadBlock(b)
			if data == nil {
				core.Abort("block %x missing locally", b[:])
			}
			sentBytes += int64(data.Data.Len())
			core.Log(core.LogDebug, "send block %x size=%s queue=%d (%d/%d)", b[:], core.CompactHumanSize(int64(data.Data.Len())), len(queue), index+1, len(queue))
			if sync.dryRun {
				core.Log(core.LogTrace, "dry-run: skip send %x", b[:])
				data.Release()
			} else {
				sync.client.StoreBlock(data)
			}
			sent++
			progressCh <- progressEvent{action: "+", block: b, index: index, queueLen: len(queue), skipped: skipped, sent: sent}
			queue = append(queue[:index], queue[index+1:]...)
		}
	}()

	printUpdate := func(ev progressEvent) {
		fmt.Printf("\r\x1b[KSyncing %d/%d (sent %d, skipped %d) %s %x\r", ev.index+1, ev.queueLen, ev.sent, ev.skipped, ev.action, ev.block[:])
	}
	ticker := time.NewTicker(sync.statTick)
	defer ticker.Stop()
	for {
		select {
		case ev, ok := <-progressCh:
			if ok {
				printUpdate(ev)
			}
		case <-ticker.C:
			sync.reportStats(false)
		case <-doneCh:
			goto waitQueue
		}
	}

waitQueue:
	if _, _, queued, _ := sync.client.GetStats(); queued > 0 {
		core.Log(core.LogInfo, "Waiting for queued blocks to be sent to remote server")
	}
	for !sync.client.Done() {
		sync.reportStats(true)
		time.Sleep(50 * time.Millisecond)
	}
	core.Log(core.LogInfo, "Sent all blocks for tree rooted at %x (sent %d, data %s)", root[:], sent, core.HumanSize(sentBytes))
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

func (sync *syncSession) reportStats(overwriteLine bool) {
	if sync.client == nil {
		return
	}
	now := time.Now()

	newline := "\n"
	if overwriteLine {
		newline = "\r"
	}

	sent, skipped, queued, qsize := sync.client.GetStats()
	fmt.Printf("\r\x1b[K>>> %.1f min, blocks sent %d/%d, queued:%d (%s)%s",
		now.Sub(sync.start).Minutes(),
		sent, sent+skipped, queued, core.HumanSize(qsize), newline)
}
