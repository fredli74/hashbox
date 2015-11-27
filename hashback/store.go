//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/bytearray"
	"bitbucket.org/fredli74/hashbox/core"

	"github.com/smtc/rollsum"

	"bytes"
	_ "crypto/aes"
	_ "crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

func (session *BackupSession) PrintStoreProgress() {
	var compression float64
	if session.Client.WriteData > 0 {
		compression = 100.0 * (float64(session.Client.WriteData) - float64(session.Client.WriteDataCompressed)) / float64(session.Client.WriteData)
	}
	sent, skipped, _, queuedsize := session.Client.GetStats()
	if session.Paint {
		fmt.Println()
	}
	fmt.Printf("*** %.1f min, read: %s, write: %s (%.0f%% compr), %d folders, %d/%d files changed, blocks sent %d/%d, queued:%s\n",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), core.HumanSize(session.Client.WriteDataCompressed), compression, session.Directories, session.Files-session.UnchangedFiles, session.Files,
		sent, skipped+sent, core.HumanSize(int64(queuedsize)))

	fmt.Println(core.MemoryStats())
}

func (session *BackupSession) storeFile(path string, entry *FileEntry) error {
	var links []core.Byte128

	chain := FileChainBlock{}

	fil, err := os.Open(path) // Open = read-only
	if err != nil {
		return err
	}
	if fil == nil {
		panic("ASSERT! err == nil and fil == nil")
	}
	defer fil.Close()

	var maxSum rollsum.Rollsum
	maxSum.Init()

	var fileData bytearray.ByteArray
	defer fileData.Release()

	for offset := int64(0); offset < int64(entry.FileSize); {
		Debug("storeFile(%s) offset %d", path, offset)

		if session.ShowProgress && time.Now().After(session.Progress) {
			session.PrintStoreProgress()
			session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
		}

		var left int64 = int64(entry.FileSize) - offset
		var maxBlockSize int = MAX_BLOCK_SIZE
		if left < int64(maxBlockSize) {
			maxBlockSize = int(left)
		}

		var blockData bytearray.ByteArray

		// Fill the fileData buffer
		if fil == nil {
			panic("ASSERT! fil == nil inside storeFile inner loop")
		}

		core.CopyNOrPanic(&fileData, fil, maxBlockSize-fileData.Len())
		fileData.ReadSeek(0, os.SEEK_CUR)

		var splitPosition int = fileData.Len()
		if fileData.Len() > MIN_BLOCK_SIZE*2 { // Candidate for rolling sum split
			rollIn, rollOut := fileData, fileData // Shallow copy the file data
			rollInBase, rollOutBase := 0, 0
			rollInPos, rollOutPos := 0, 0
			rollInSlice, _ := rollIn.ReadSlice()
			rollOutSlice, _ := rollOut.ReadSlice()

			partSum := maxSum
			var maxd = uint32(0)
			for rollInPos < fileData.Len() {
				if rollInPos-rollInBase >= len(rollInSlice) { // Next slice please
					rollInBase, _ = rollIn.ReadSeek(len(rollInSlice), os.SEEK_CUR)
					rollInSlice, _ = rollIn.ReadSlice()
				}
				if rollOutPos-rollOutBase >= len(rollOutSlice) { // Next slice please
					rollOutBase, _ = rollOut.ReadSeek(len(rollOutSlice), os.SEEK_CUR)
					rollOutSlice, _ = rollOut.ReadSlice()
				}

				if rollInPos >= MIN_BLOCK_SIZE {
					partSum.Rollout(rollOutSlice[rollOutPos-rollOutBase])
					rollOutPos++
				}
				partSum.Rollin(rollInSlice[rollInPos-rollInBase])
				rollInPos++

				if rollInPos >= MIN_BLOCK_SIZE {
					d := partSum.Digest()
					if d >= maxd {
						maxd = d
						splitPosition = rollInPos
						maxSum = partSum // Keep the sum so we can continue from here
					}
				}
			}
		}

		// Split an swap
		right := fileData.Split(splitPosition)
		blockData = fileData
		fileData = right

		offset += int64(blockData.Len())
		session.ReadData += int64(blockData.Len())

		// TODO: add encryption and custom compression here
		var datakey core.Byte128

		id := session.Client.StoreBlock(core.BlockDataTypeZlib, blockData, nil)
		links = append(links, id)
		chain.ChainBlocks = append(chain.ChainBlocks, id)
		chain.DecryptKeys = append(chain.DecryptKeys, datakey)
	}

	if len(chain.ChainBlocks) > 1 {
		id := session.Client.StoreBlock(core.BlockDataTypeZlib, SerializeToByteArray(chain), links)
		entry.ContentType = ContentTypeFileChain
		entry.ContentBlockID = id

	} else {
		entry.ContentType = ContentTypeFileData
		entry.ContentBlockID = chain.ChainBlocks[0]
		entry.DecryptKey = chain.DecryptKeys[0]
	}

	return nil
}

func (session *BackupSession) storeDir(path string, entry *FileEntry) error {
	var links []core.Byte128

	dir := DirectoryBlock{}

	fil, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fil.Close()

	fl, err := fil.Readdir(-1)
	if err != nil {
		return err
	}

	sort.Sort(FileInfoSlice(fl))
	for _, info := range fl {
		e, err := session.storePath(filepath.Join(path, info.Name()), false)
		if err != nil {
			session.Important(fmt.Sprintf("Skipping (ERROR) %v", err))
		} else if e != nil {
			dir.File = append(dir.File, e)
			if e.HasContentBlockID() {
				links = append(links, e.ContentBlockID)
			}
		}
	}

	id := session.Client.StoreBlock(core.BlockDataTypeZlib, SerializeToByteArray(dir), links)
	entry.ContentType = ContentTypeDirectory
	entry.ContentBlockID = id
	return nil
}

func (session *BackupSession) storePath(path string, toplevel bool) (*FileEntry, error) {
	if session.ShowProgress && time.Now().After(session.Progress) {
		session.PrintStoreProgress()
		session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	}

	Debug("storePath %s", path)
	// Get file info from disk
	var info os.FileInfo
	{
		var isDir bool
		var err error

		if toplevel {
			info, err = os.Stat(path) // At top level we follow symbolic links
		} else {
			info, err = os.Lstat(path) // At all other levels we do not
		}
		if info != nil {
			isDir = info.IsDir() // Check ignore even if we cannot open the file (to avoid output errors on files we already ignore)
		}
		if match, pattern := session.ignoreMatch(path, isDir); match {
			session.Log(fmt.Sprintf("Skipping (ignore %s) %s", pattern, path))
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
	}

	entry := FileEntry{
		FileName:    core.String(info.Name()),
		FileSize:    int64(info.Size()),
		FileMode:    uint32(info.Mode()),
		ModTime:     info.ModTime().UnixNano(),
		ReferenceID: session.State.StateID,
	}

	if entry.FileMode&uint32(os.ModeTemporary) > 0 {
		session.Log("Skipping (temporary file)", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeDevice) > 0 {
		session.Log("Skipping (device file)", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeNamedPipe) > 0 {
		session.Log("Skipping (named pipe file)", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSocket) > 0 {
		session.Log("Skipping (socket file)", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSymlink) > 0 {
		entry.ContentType = ContentTypeSymLink
		sym, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		entry.FileLink = core.String(sym)

		//		same := session.findAndReuseReference(path, &entry)
		same := session.reference.findAndReuseReference(path, &entry)
		if !same {
			session.Log("SYMLINK", path, "->", sym)
		}
	} else {
		if entry.FileMode&uint32(os.ModeDir) > 0 {
			//refEntry := session.findReference(path, &entry)
			refEntry := session.reference.findReference(path)
			if err := session.storeDir(path, &entry); err != nil {
				return nil, err
			}
			if refEntry != nil && bytes.Equal(refEntry.ContentBlockID[:], entry.ContentBlockID[:]) {
				entry.ReferenceID = refEntry.ReferenceID
			}

			session.Directories++
		} else {
			//same := session.findAndReuseReference(path, &entry)
			same := session.reference.findAndReuseReference(path, &entry)
			if !same && entry.FileSize > 0 {
				session.Log(fmt.Sprintf("%s" /*os.FileMode(entry.FileMode),*/, path))
				if err := session.storeFile(path, &entry); err != nil {
					return nil, err
				}
				// TODO: UniqueSize is a here calculated by the backup routine, it should be calculated by the server
				session.State.UniqueSize += entry.FileSize
			} else {
				if session.Client.Paint && !session.Verbose && !session.ShowProgress {
					fmt.Print(" ")
				}
				session.UnchangedFiles++
			}
			session.Files++
			session.State.Size += entry.FileSize
		}
	}

	return &entry, nil
}

type ignoreEntry struct {
	pattern   string
	match     string
	pathmatch bool
	dirmatch  bool
}

func (session *BackupSession) ignoreMatch(path string, isDir bool) (bool, string) {
	_, name := filepath.Split(path)
	for _, pattern := range session.ignoreList {
		var match bool
		if pattern.dirmatch && !isDir {
			// no match
		} else if pattern.pathmatch {
			match, _ = filepath.Match(pattern.match, path)
		} else {
			match, _ = filepath.Match(pattern.match, name)
		}
		if match {
			return match, pattern.pattern
		}
	}
	return false, ""
}

func (session *BackupSession) Store(datasetName string, path ...string) {
	session.reference = &referenceEngine{client: session.Client}

	var referenceBlockID *core.Byte128
	if !session.FullBackup {
		list := session.Client.ListDataset(datasetName)
		if len(list.States) > 0 {
			referenceBlockID = &list.States[len(list.States)-1].BlockID
		}
	}

	for i := 0; i < len(path); i++ {
		p, err := filepath.Abs(path[i])
		if err == nil {
			path[i] = p
		}
	}

	// Do we need a virtual root folder?
	var virtualRoot bool
	if len(path) > 1 {
		virtualRoot = true
	} else {
		info, err := os.Lstat(path[0])
		if err != nil {
			panic(err)
		}
		virtualRoot = !info.IsDir()
	}

	if virtualRoot {
		var links []core.Byte128

		var refdir DirectoryBlock
		if referenceBlockID != nil {
			blockData := session.Client.ReadBlock(*referenceBlockID).Data
			refdir.Unserialize(&blockData)
			blockData.Release()

			for _, s := range path {
				name := filepath.Base(s)
				for _, r := range refdir.File {
					if string(r.FileName) == name {
						r.FileName = core.String(s)
						session.reference.queue = append(session.reference.queue, r)
						//session.reference.addPath(s, r)
						//						session.refQueue = append(session.refQueue, refEntry{s, r})
						break
					}
				}
			}
			go session.reference.downloadWorker()
		}

		dir := DirectoryBlock{}
		for _, s := range path {
			e, err := session.storePath(s, true)
			if err != nil {
				panic(err)
			} else if e != nil {
				dir.File = append(dir.File, e)
				if e.ContentType != ContentTypeEmpty {
					links = append(links, e.ContentBlockID)
				}
			}
		}
		session.State.BlockID = session.Client.StoreBlock(core.BlockDataTypeZlib, SerializeToByteArray(dir), links)
	} else {
		p := filepath.Clean(path[0])
		if referenceBlockID != nil {
			// push the last backup root to reference list
			session.reference.path = append(session.reference.path, p)
			session.reference.pushReference(*referenceBlockID)
			go session.reference.downloadWorker()
		}
		e, err := session.storePath(p, true)
		if err != nil {
			panic(err)
		}
		if e == nil {
			panic(errors.New("Nothing to store"))
		}
		session.State.BlockID = e.ContentBlockID
	}

	// Commit all pending writes
	session.Client.Commit()
	session.Client.AddDatasetState(datasetName, *session.State)

	fmt.Println()
	session.PrintStoreProgress()
}

func truncateSecondsToDay(t int64) int64 {
	return (t / (24 * 60 * 60)) * 24 * 60 * 60
}
func (session *BackupSession) Retention(datasetName string, retainDays int, retainWeeks int) {
	_ = "breakpoint"

	var timenow int64 = time.Now().Unix()
	var today = truncateSecondsToDay(timenow) // 00:00:00 today
	var dailyLimit int64
	if retainDays > 0 {
		dailyLimit = today - (int64(retainDays) * 24 * 60 * 60)
	}
	var weeklyLimit int64
	if retainWeeks > 0 {
		weeklyLimit = today - (int64(retainWeeks) * 7 * 24 * 60 * 60)
	}

	var lastbackup int64 = 0

	list := session.Client.ListDataset(datasetName)
	for i, s := range list.States {
		if i >= len(list.States)-2 { // Always keep the last two
			break
		}

		// Extract the backup date from the stateID
		timestamp := int64(binary.BigEndian.Uint64(s.StateID[:]) / 1e9) // nano timestamp in seconds

		age := (timenow - timestamp)
		interval := (timestamp - truncateSecondsToDay(lastbackup)) // interval from last backup

		var throw bool
		var reason string

		if interval < (24*60*60) && age > 24*60*60 {
			throw = true
			reason = "keep one daily"
		}
		if interval < (7*24*60*60) && timestamp < dailyLimit {
			throw = true
			reason = "keep one weekly"
		}
		if weeklyLimit < dailyLimit && timestamp < weeklyLimit {
			throw = true
			reason = fmt.Sprintf("older than %d weeks", retainWeeks)
		}
		if weeklyLimit >= dailyLimit && timestamp < dailyLimit {
			throw = true
			reason = fmt.Sprintf("older than %d days", retainDays)
		}

		if throw {
			date := time.Unix(int64(timestamp), 0)
			fmt.Printf("Removing backup %s (%s)\n", date.Format(time.RFC3339), reason)
		} else {
			date := time.Unix(int64(timestamp), 0)
			fmt.Printf("Keeping backup %s\n", date.Format(time.RFC3339))
			lastbackup = timestamp
		}
	}
}

//***********************************************************************//
//                             referenceEngine                           //
//***********************************************************************//

// referenceEngine is used to compare this backup with the previous backup and only go through files that have changed.
// It does this by downloading the last backup structure into a sorted queue and poppin away line by line while finding matches.
type referenceEngine struct {
	client    *core.Client
	path      []string     // path hierarchy, used for traversing up and down subdirectories without having to save full path for each queue entry
	queue     []*FileEntry // last backup structure, sorted
	loadpoint int          // current point in the queue where to load in new information, we do this so we do not have to sort the list after each insert
	lock      sync.Mutex   // used because downloading of the structure is a concurrent goprocess
}

var entryEOD = &FileEntry{} // end of dir marker

// Download worker is a separate goprocess to let it download the last backup structure in the background
func (r *referenceEngine) downloadWorker() {
	for func() bool {
		r.lock.Lock()
		if r.loadpoint >= len(r.queue) {
			r.lock.Unlock()
			return false
		} else {
			e := r.queue[r.loadpoint]
			if e.ContentType == ContentTypeDirectory {
				r.lock.Unlock()
				r.pushReference(e.ContentBlockID)
				return true
			} else {
				r.loadpoint++
				r.lock.Unlock()
				return true
			}
		}
	}() {
		runtime.Gosched()
	}
}

// pushReference adds a subdir structure at the current loadpoint
func (r *referenceEngine) pushReference(referenceBlockID core.Byte128) {
	var refdir DirectoryBlock

	blockData := r.client.ReadBlock(referenceBlockID).Data
	refdir.Unserialize(&blockData)
	blockData.Release()

	var list []*FileEntry
	for _, e := range refdir.File {
		list = append(list, e)
	}
	list = append(list, entryEOD)

	r.lock.Lock()
	defer r.lock.Unlock()
	r.loadpoint++
	if r.loadpoint > len(r.queue) {
		r.loadpoint = len(r.queue)
	}
	list = append(list, r.queue[r.loadpoint:]...)
	r.queue = append(r.queue[:r.loadpoint], list...)
}

// popReference pops the first queue entry and sets the path hierarchy correctly
func (r *referenceEngine) popReference() *FileEntry {
	r.lock.Lock()
	defer r.lock.Unlock()

	var e *FileEntry
	if len(r.queue) > 0 {
		e = r.queue[0]
		if e.ContentType == ContentTypeDirectory {
			r.path = append(r.path, string(e.FileName))
		} else if e == entryEOD {
			r.path = r.path[:len(r.path)-1]
		}
		r.queue = r.queue[1:]
		r.loadpoint--
	}
	return e
}

// joinPath puts the path hierarchy list together to a slash separated path string
func (r *referenceEngine) joinPath() (path string) {
	for _, p := range r.path {
		path = filepath.Join(path, p)
	}
	return
}

// peekPath returns the full path of the next queue entry
func (r *referenceEngine) peekPath() (path string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for len(r.queue) > 0 && r.loadpoint < 1 {
		r.lock.Unlock()
		time.Sleep(10 * time.Millisecond)
		r.lock.Lock()
	}

	if len(r.queue) > 0 {
		path = r.joinPath()
		path = filepath.Join(path, string(r.queue[0].FileName))
	}
	return
}

// findReference tries to find a specified path in the last backup structure
func (r *referenceEngine) findReference(path string) *FileEntry {
	for {
		p := r.peekPath()
		if pathLess(p, path) && r.popReference() != nil {
			Debug("Reference %s < %s, roll forward", p, path)
			continue
		} else if p == path {
			Debug("Reference %s == %s", p, path)
			return r.popReference()
		} else {
			Debug("Reference %s > %s", p, path)
			break
		}
	}
	return nil
}

// findAndResuseReference finds and reuses the FileEntry for the last backup structure
func (r *referenceEngine) findAndReuseReference(path string, entry *FileEntry) bool {
	refEntry := r.findReference(path)
	if refEntry != nil && refEntry.FileName == entry.FileName && refEntry.FileSize == entry.FileSize && refEntry.FileMode == entry.FileMode && refEntry.ModTime == entry.ModTime && refEntry.FileLink == entry.FileLink {
		// It's the same!
		*entry = *refEntry
		return true
	}
	return false
}
