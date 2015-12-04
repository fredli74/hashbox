//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/bytearray"
	"bitbucket.org/fredli74/hashbox/core"

	"github.com/smtc/rollsum"

	"bufio"
	"bytes"
	_ "crypto/aes"
	_ "crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"
)

func (session *BackupSession) PrintStoreProgress(interval time.Duration) {
	if session.ShowProgress && (interval == 0 || time.Now().After(session.Progress)) {
		var compression float64
		if session.Client.WriteData > 0 {
			compression = 100.0 * (float64(session.Client.WriteData) - float64(session.Client.WriteDataCompressed)) / float64(session.Client.WriteData)
		}
		sent, skipped, _, queuedsize := session.Client.GetStats()
		if session.Paint {
			fmt.Println()
		}
		fmt.Printf(">>> %.1f min, read: %s, write: %s (%.0f%% compr), %d folders, %d/%d files changed, blocks sent %d/%d, queued:%s\n",
			time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), core.HumanSize(session.Client.WriteDataCompressed), compression, session.Directories, session.Files-session.UnchangedFiles, session.Files,
			sent, skipped+sent, core.HumanSize(int64(queuedsize)))

		//fmt.Println(core.MemoryStats())
		session.Progress = time.Now().Add(interval)
	}
}

func (session *BackupSession) storeFile(path string, entry *FileEntry) (err error) {
	defer func() {
		// Panic error handling
		if r := recover(); r != nil {
			// we need this because some obscure files on OSX does open but then generates "bad file descriptor" on read
			if e, ok := r.(*os.PathError); ok && e.Err == syscall.EBADF {
				err = e.Err
			} else {
				panic(r) // Any other error is not normal and should panic
			}
		}
	}()

	var links []core.Byte128

	chain := FileChainBlock{}

	var file *os.File
	if file, err = os.Open(path); err != nil {
		return err
	}
	defer file.Close()

	var maxSum rollsum.Rollsum
	maxSum.Init()

	var fileData bytearray.ByteArray
	defer fileData.Release()

	for offset := int64(0); offset < int64(entry.FileSize); {
		Debug("storeFile(%s) offset %d", path, offset)

		session.PrintStoreProgress(PROGRESS_INTERVAL_SECS)

		var left int64 = int64(entry.FileSize) - offset
		var maxBlockSize int = MAX_BLOCK_SIZE
		if left < int64(maxBlockSize) {
			maxBlockSize = int(left)
		}

		var blockData bytearray.ByteArray

		// Fill the fileData buffer
		core.CopyNOrPanic(&fileData, file, maxBlockSize-fileData.Len())
		fileData.ReadSeek(0, os.SEEK_CUR) // TODO: figure out why this line is here because I do not remember

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

		id := session.Client.StoreData(core.BlockDataTypeZlib, blockData, nil)
		links = append(links, id)
		chain.ChainBlocks = append(chain.ChainBlocks, id)
		chain.DecryptKeys = append(chain.DecryptKeys, datakey)
	}

	if len(chain.ChainBlocks) > 1 {
		id := session.Client.StoreData(core.BlockDataTypeZlib, SerializeToByteArray(chain), links)
		entry.ContentType = ContentTypeFileChain
		entry.ContentBlockID = id

	} else {
		entry.ContentType = ContentTypeFileData
		entry.ContentBlockID = chain.ChainBlocks[0]
		entry.DecryptKey = chain.DecryptKeys[0]
	}

	return nil
}

func (session *BackupSession) storeDir(path string, entry *FileEntry) (id core.Byte128, err error) {
	var links []core.Byte128

	dir := DirectoryBlock{}

	var file *os.File
	if file, err = os.Open(path); err != nil {
		return
	}
	defer file.Close()

	var filelist []os.FileInfo
	if filelist, err = file.Readdir(-1); err != nil {
		return
	}

	sort.Sort(FileInfoSlice(filelist))
	for _, info := range filelist {
		e, err := session.storePath(filepath.Join(path, info.Name()), false)
		if err != nil {
			session.Important(fmt.Sprintf("Skipping (ERROR) %v", err))
		}
		if e != nil {
			dir.File = append(dir.File, e)
			if e.HasContentBlockID() {
				links = append(links, e.ContentBlockID)
			}
		}
	}
	block := core.NewHashboxBlock(core.BlockDataTypeZlib, SerializeToByteArray(dir), links)
	id = block.BlockID
	if entry == nil || entry.ContentBlockID.Compare(id) != 0 {
		if id.Compare(session.Client.StoreBlock(block)) != 0 {
			panic(errors.New("ASSERT, server blockID != local blockID"))
		}
	} else {
		block.Release()
	}
	return
}

func (session *BackupSession) storePath(path string, toplevel bool) (entry *FileEntry, err error) {
	session.PrintStoreProgress(PROGRESS_INTERVAL_SECS)

	Debug("storePath %s", path)
	// Get file info from disk
	var info os.FileInfo
	{
		var isDir bool

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

	entry = &FileEntry{
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

		same := session.reference.findAndReuseReference(path, entry)
		if !same {
			session.Log("SYMLINK", path, "->", sym)
		} else {
			session.UnchangedFiles++
		}
		session.Files++
		session.State.Size += entry.FileSize

		session.reference.storeReference(entry)

	} else if entry.FileMode&uint32(os.ModeDir) > 0 {
		entry.ContentType = ContentTypeDirectory
		reservation := session.reference.reserveReference(entry) // We do this because directories needs to be written before files, but we also need contentblockID to be correct
		defer session.reference.storeReferenceDir(entry, reservation)

		refEntry := session.reference.findReference(path)

		if entry.ContentBlockID, err = session.storeDir(path, refEntry); err != nil {
			return nil, err
		}
		if refEntry != nil && bytes.Equal(refEntry.ContentBlockID[:], entry.ContentBlockID[:]) {
			entry.ReferenceID = refEntry.ReferenceID
		}
		session.Directories++
	} else {
		refEntry := session.reference.findReference(path)
		if refEntry != nil && refEntry.FileName == entry.FileName && refEntry.FileSize == entry.FileSize && refEntry.FileMode == entry.FileMode && refEntry.ModTime == entry.ModTime {
			// It's the same!
			entry = refEntry

			if session.Client.Paint && !session.Verbose && !session.ShowProgress {
				fmt.Print(" ")
			}
			session.UnchangedFiles++
		} else {
			if entry.FileSize > 0 {
				session.Log(fmt.Sprintf("%s", path))
				if err = session.storeFile(path, entry); err != nil {
					if e, ok := err.(*os.PathError); ok && runtime.GOOS == "windows" && e.Err == syscall.Errno(0x20) { // Windows ERROR_SHARING_VIOLATION
						return refEntry, err // Returning refEntry here in case this file existed and could be opened in a previous backup
					}
					return nil, err
				}
				if session.reference.loaded { // We are using unique as a diff-size, so first backup (with no reference) has no diff-size
					// TODO: UniqueSize is a here calculated by the backup routine, it should be calculated by the server?
					session.State.UniqueSize += entry.FileSize
				}
			}
		}
		session.Files++
		session.State.Size += entry.FileSize

		session.reference.storeReference(entry)
	}
	return
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

type ByBase []string

func (s ByBase) Len() int           { return len(s) }
func (s ByBase) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByBase) Less(i, j int) bool { return filepath.Base(s[i]) < filepath.Base(s[j]) }

func (session *BackupSession) Store(datasetName string, path ...string) {
	var err error

	// Setup the reference backup engine
	session.reference = NewReferenceEngine(session.Client, core.Hash([]byte(datasetName)))
	defer session.reference.Close()

	// Convert relative paths to absolute paths
	for i := 0; i < len(path); i++ {
		p, err := filepath.Abs(path[i])
		if err == nil {
			path[i] = p
		}
	}
	sort.Sort(ByBase(path))

	// Do we need a virtual root folder?
	var virtualRootDir *DirectoryBlock
	{
		info, err := os.Lstat(path[0])
		if err != nil {
			panic(err)
		}
		if !info.IsDir() || len(path) > 1 {
			virtualRootDir = &DirectoryBlock{}
			session.reference.virtualRoot = make(map[string]string)
			for _, s := range path {
				session.reference.virtualRoot[filepath.Base(s)] = s
			}
		} else {
			session.reference.path = append(session.reference.path, path[0])
		}
	}

	// Load up last backup into the reference engine
	if !session.FullBackup {
		list := session.Client.ListDataset(datasetName)
		if len(list.States) > 0 {
			session.reference.load(list.States[len(list.States)-1].BlockID)
		}
	}

	if virtualRootDir != nil {
		var links []core.Byte128
		var entry *FileEntry
		for _, s := range path {
			entry, err = session.storePath(s, true)
			if err != nil {
				panic(err)
			} else if entry == nil {
				panic(errors.New(fmt.Sprintf("Unable to store %s", s)))
			} else if virtualRootDir != nil {
				virtualRootDir.File = append(virtualRootDir.File, entry)
				if entry.ContentType != ContentTypeEmpty {
					links = append(links, entry.ContentBlockID)
				}
			}
		}

		session.State.BlockID = session.Client.StoreData(core.BlockDataTypeZlib, SerializeToByteArray(virtualRootDir), links)
	} else {
		session.State.BlockID, err = session.storeDir(path[0], nil)
		PanicOn(err)
	}

	// Commit all pending writes
	//	session.Client.Commit()
	for !session.Client.Done() {
		session.PrintStoreProgress(PROGRESS_INTERVAL_SECS)
		time.Sleep(100 * time.Millisecond)
	}
	session.Client.AddDatasetState(datasetName, *session.State)

	// Close and rename the current reference cache file for future use
	session.reference.Commit(session.State.BlockID)

	fmt.Println()
	session.PrintStoreProgress(0)
}

func truncateSecondsToDay(t int64) int64 {
	return (t / (24 * 60 * 60)) * 24 * 60 * 60
}
func (session *BackupSession) Retention(datasetName string, retainDays int, retainWeeks int) {
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

		var throwAway bool
		var reason string

		if interval < (24*60*60) && age > 24*60*60 {
			throwAway = true
			reason = "keeping only one daily"
		}
		if interval < (7*24*60*60) && timestamp < dailyLimit {
			throwAway = true
			reason = "keeping only one weekly"
		}
		if weeklyLimit < dailyLimit && timestamp < weeklyLimit {
			throwAway = true
			reason = fmt.Sprintf("older than %d weeks", retainWeeks)
		}
		if weeklyLimit >= dailyLimit && timestamp < dailyLimit {
			throwAway = true
			reason = fmt.Sprintf("older than %d days", retainDays)
		}

		date := time.Unix(int64(timestamp), 0)
		if throwAway {
			fmt.Printf("Removing backup %s (%s)\n", date.Format(time.RFC3339), reason)
			// session.Client.RemoveDatasetState(datasetName, s.StateID)
		} else {
			Debug("Keeping backup %s\n", date.Format(time.RFC3339))
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
	client *core.Client
	path   []string     // path hierarchy, used for traversing up and down subdirectories without having to save full path for each queue entry
	queue  []*FileEntry // last backup structure, sorted

	loaded    bool       // indicates that a reference backup was loaded (or started to load)
	loadpoint int        // current point in the queue where to load in new information, we do this so we do not have to sort the list after each insert
	lock      sync.Mutex // used because downloading of the structure is a concurrent goprocess

	virtualRoot  map[string]string
	datasetNameH core.Byte128
	cacheCurrent *os.File
}

var entryEOD = &FileEntry{} // end of dir marker

// Download worker is a separate goprocess to let it download the last backup structure in the background
func (r *referenceEngine) load(rootBlockID core.Byte128) {
	// Check if we have last backup cached on disk
	cacheLast, _ := os.Open(r.cacheName(rootBlockID))
	if cacheLast != nil {
		defer cacheLast.Close()
		info, err := cacheLast.Stat()
		PanicOn(err)
		cacheSize := info.Size()
		reader := bufio.NewReader(cacheLast)
		for offset := int64(0); offset < cacheSize; {
			var entry FileEntry
			offset += int64(entry.Unserialize(reader))
			r.queue = append(r.queue, &entry)
		}
		r.loadpoint = len(r.queue)
	} else {
		r.downloadReference(rootBlockID)
		go func() {
			for {
				r.lock.Lock()
				if r.loadpoint >= len(r.queue) {
					r.lock.Unlock()
					break
				} else {
					e := r.queue[r.loadpoint]
					if e.ContentType == ContentTypeDirectory {
						r.lock.Unlock()
						r.downloadReference(e.ContentBlockID)
					} else {
						r.loadpoint++
						r.lock.Unlock()
					}
				}
				runtime.Gosched()
			}
		}()
	}
	r.loaded = true
}

// downloadReference adds a subdir structure at the current loadpoint
func (r *referenceEngine) downloadReference(referenceBlockID core.Byte128) {
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
			path := string(e.FileName)
			if len(r.path) == 0 && r.virtualRoot != nil && r.virtualRoot[path] != "" {
				path = r.virtualRoot[path]
			}
			r.path = append(r.path, path)
		} else if string(e.FileName) == "" { // EOD
			r.path = r.path[:len(r.path)-1]
		}
		r.queue = r.queue[1:]
		r.loadpoint--
	}
	return e
}

// joinPath puts the path hierarchy list together to a slash separated path string
func (r *referenceEngine) joinPath(elem string) (path string) {
	for _, p := range r.path {
		path = filepath.Join(path, p)
	}
	path = filepath.Join(path, elem)
	if len(r.path) == 0 && r.virtualRoot != nil && r.virtualRoot[path] != "" {
		return r.virtualRoot[path]
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
		path = r.joinPath(string(r.queue[0].FileName))
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

func (r *referenceEngine) cacheName(rootID core.Byte128) string {
	filename := fmt.Sprintf("%s.%s.cache", base64.RawURLEncoding.EncodeToString(r.datasetNameH[:]), base64.RawURLEncoding.EncodeToString(rootID[:]))
	return filepath.Join(LocalStoragePath, filename)
}

func (r *referenceEngine) reserveReference(entry *FileEntry) (location int64) {
	if r.cacheCurrent != nil {
		l, err := r.cacheCurrent.Seek(0, os.SEEK_CUR)
		PanicOn(err)
		entry.Serialize(r.cacheCurrent)
		return l
	} else {
		return
	}
}

func (r *referenceEngine) storeReference(entry *FileEntry) {
	if r.cacheCurrent != nil {
		entry.Serialize(r.cacheCurrent)
	}
}

func (r *referenceEngine) storeReferenceDir(entry *FileEntry, location int64) {
	if r.cacheCurrent != nil {
		r.cacheCurrent.Seek(location, os.SEEK_SET)
		entry.Serialize(r.cacheCurrent)

		r.cacheCurrent.Seek(0, os.SEEK_END)
		entryEOD.Serialize(r.cacheCurrent)
	}
}

func (r *referenceEngine) Commit(rootID core.Byte128) {
	cleanup := fmt.Sprintf("%s.*.cache", base64.RawURLEncoding.EncodeToString(r.datasetNameH[:]))
	filepath.Walk(LocalStoragePath, func(path string, info os.FileInfo, err error) error {
		if match, _ := filepath.Match(cleanup, info.Name()); match {
			os.Remove(path)
		}
		return nil
	})

	if r.cacheCurrent != nil {
		r.cacheCurrent.Close()
		os.Rename(r.cacheCurrent.Name(), r.cacheName(rootID))
		r.cacheCurrent = nil
	}
}
func (r *referenceEngine) Close() {
	if r.cacheCurrent != nil {
		r.cacheCurrent.Close()
		os.Remove(r.cacheCurrent.Name())
		r.cacheCurrent = nil
	}
}
func NewReferenceEngine(client *core.Client, datasetNameH core.Byte128) *referenceEngine {
	tempfile, err := ioutil.TempFile("", "hbcache")
	PanicOn(err)

	r := &referenceEngine{
		client:       client,
		datasetNameH: datasetNameH,
		cacheCurrent: tempfile,
	}
	return r
}
