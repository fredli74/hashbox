//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package main

import (
	"github.com/fredli74/bytearray"
	"github.com/fredli74/hashbox/pkg/core"

	"github.com/smtc/rollsum"

	"bufio"
	"bytes"
	_ "crypto/aes"
	_ "crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func (session *BackupSession) PrintStoreProgress(interval time.Duration) {
	if session.ShowProgress && (interval == 0 || time.Now().After(session.Progress)) {
		var compression float64
		if session.Client.WriteData > 0 {
			compression = 100.0 * (float64(session.Client.WriteData) - float64(session.Client.WriteDataCompressed)) / float64(session.Client.WriteData)
		}
		sent, skipped, _, queuedsize := session.Client.GetStats()
		session.Log(">>> %.1f min, read: %s, written: %s (%.0f%% compr), %d folders, %d/%d files changed, blocks sent %d/%d, queued:%s",
			time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), core.HumanSize(session.Client.WriteDataCompressed), compression, session.Directories, session.Files-session.UnchangedFiles, session.Files,
			sent, skipped+sent, core.HumanSize(int64(queuedsize)))

		//fmt.Println(core.MemoryStats())
		session.Progress = time.Now().Add(interval)
	}
}

func (session *BackupSession) PrintRecoverProgress(progress float64, interval time.Duration) {
	if session.ShowProgress && (interval == 0 || time.Now().After(session.Progress)) {
		session.Log(">>> %.1f min, resuming last backup: %.0f%%", time.Since(session.Start).Minutes(), progress)
		session.Progress = time.Now().Add(interval)
	}
}

func compareEntries(fileInfo os.FileInfo, new *FileEntry, old *FileEntry) bool {
	if old.FileName != new.FileName {
		core.Log(core.LogTrace, "FileName is different: %s != %s", new.FileName, old.FileName)
		return false
	}
	if old.FileSize != new.FileSize {
		core.Log(core.LogTrace, "FileSize is different: %d != %d", new.FileSize, old.FileSize)
		return false
	}
	if isOfflineFile(fileInfo) {
		if old.ModTime/1e9 != new.ModTime/1e9 {
			// compare with second precision because of Dropbox Online Only files
			core.Log(core.LogTrace, "ModTime is different (OFFLINE FILE): %d != %d", new.ModTime, old.ModTime)
			return false
		}
	} else {
		if old.FileMode != new.FileMode {
			core.Log(core.LogTrace, "FileMode is different: %d != %d", new.FileMode, old.FileMode)
			return false
		}
		if old.ModTime != new.ModTime {
			core.Log(core.LogTrace, "ModTime is different: %d != %d", new.ModTime, old.ModTime)
			return false
		}
	}
	return true
}

func (session *BackupSession) storeFile(path string, entry *FileEntry) (err error) {
	defer func() {
		// Panic error handling
		if r := recover(); r != nil {
			if e := minorPathError(r); e != nil {
				err = e
			} else {
				panic(r) // Any other error while reading is not normal and should panic
			}
		}
	}()

	var links []core.Byte128

	chain := FileChainBlock{}

	var file *os.File
	if file, err = os.Open(path); err != nil {
		return err
	}
	defer func() {
		core.AbortOnError(file.Close())
	}()

	var fileData bytearray.ByteArray
	defer fileData.Release()

	for offset := int64(0); offset < int64(entry.FileSize); {
		core.Log(core.LogTrace, "storeFile(%s) offset %d", path, offset)

		session.PrintStoreProgress(PROGRESS_INTERVAL_SECS)

		var left int64 = int64(entry.FileSize) - offset
		var maxBlockSize int = MAX_BLOCK_SIZE
		if left < int64(maxBlockSize) {
			maxBlockSize = int(left)
		}

		var blockData bytearray.ByteArray

		// Fill the fileData buffer
		core.CopyNOrPanic(&fileData, file, maxBlockSize-fileData.Len())
		_, err := fileData.ReadSeek(0, io.SeekCurrent) // TODO: figure out why this line is here because I do not remember
		core.AbortOnError(err)

		var splitPosition int = fileData.Len()
		if fileData.Len() > MIN_BLOCK_SIZE*2 { // Candidate for rolling sum split
			var rollSum rollsum.Rollsum
			rollSum.Init()
			var maxd = uint32(0)

			rollIn, rollOut := fileData, fileData // Shallow copy the file data
			rollInBase, rollOutBase := 0, 0
			rollInPos, rollOutPos := 0, 0
			rollInSlice, _ := rollIn.ReadSlice()
			rollOutSlice, _ := rollOut.ReadSlice()

			for rollInPos < fileData.Len() {
				if rollInPos-rollInBase >= len(rollInSlice) { // Next slice please
					rollInBase, _ = rollIn.ReadSeek(len(rollInSlice), io.SeekCurrent)
					rollInSlice, _ = rollIn.ReadSlice()
				}
				if rollOutPos-rollOutBase >= len(rollOutSlice) { // Next slice please
					rollOutBase, _ = rollOut.ReadSeek(len(rollOutSlice), io.SeekCurrent)
					rollOutSlice, _ = rollOut.ReadSlice()
				}

				if rollInPos >= MIN_BLOCK_SIZE {
					rollSum.Rollout(rollOutSlice[rollOutPos-rollOutBase])
					rollOutPos++
				}
				rollSum.Rollin(rollInSlice[rollInPos-rollInBase])
				rollInPos++

				if rollInPos >= MIN_BLOCK_SIZE {
					d := rollSum.Digest()
					if d >= maxd {
						maxd = d
						splitPosition = rollInPos
					}
				}
			}
		}

		// Split and swap
		right := fileData.Split(splitPosition)
		blockData = fileData
		fileData = right

		offset += int64(blockData.Len())
		session.ReadData += int64(blockData.Len())

		// TODO: add encryption and custom compression here
		var datakey core.Byte128

		id := session.Client.StoreData(core.BlockDataTypeZlib, blockData, nil)
		core.Log(core.LogTrace, "split at %d, store %d bytes as %x", offset, blockData.Len(), id)

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
	defer func() {
		core.AbortOnError(file.Close())
	}()

	var filelist []os.FileInfo
	if filelist, err = file.Readdir(-1); err != nil {
		return
	}

	sort.Sort(FileInfoSlice(filelist))
	for _, info := range filelist {
		e, err := session.storePath(filepath.Join(path, info.Name()), false)
		if err != nil {
			session.Log("Skipping (ERROR) %v", err)
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
			panic(errors.New("assert, server blockID != local blockID"))
		}
	} else {
		block.Release()
	}
	return
}

func (session *BackupSession) entryFromFileInfo(fileInfo os.FileInfo) *FileEntry {
	return &FileEntry{
		FileName:    core.String(fileInfo.Name()),
		FileSize:    int64(fileInfo.Size()),
		FileMode:    uint32(fileInfo.Mode()),
		ModTime:     fileInfo.ModTime().UnixNano(),
		ReferenceID: session.State.StateID,
	}
}

func (session *BackupSession) storePath(path string, toplevel bool) (entry *FileEntry, err error) {
	session.PrintStoreProgress(PROGRESS_INTERVAL_SECS)

	core.Log(core.LogDebug, "storePath %s", path)
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
			core.Log(core.LogTrace, "%+v", info)
			isDir = info.IsDir() // Check ignore even if we cannot open the file (to avoid output errors on files we already ignore)
		}

		if match, pattern := session.ignoreMatch(path, isDir); match {
			session.LogVerbose("Skipping (ignore %s) %s", pattern, path)
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
	}

	entry = session.entryFromFileInfo(info)
	if entry.FileMode&uint32(os.ModeTemporary) > 0 {
		session.LogVerbose("Skipping (temporary file) %s", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeDevice) > 0 {
		session.LogVerbose("Skipping (device file) %s", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeNamedPipe) > 0 {
		session.LogVerbose("Skipping (named pipe file) %s", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSocket) > 0 {
		session.LogVerbose("Skipping (socket file) %s", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSymlink) > 0 {
		entry.ContentType = ContentTypeSymLink
		entry.FileSize = 0

		sym, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		entry.FileLink = core.String(sym)

		refEntry := session.reference.findReference(path)
		if refEntry != nil && refEntry.FileLink == entry.FileLink && compareEntries(info, entry, refEntry) {
			// It's the same!
			entry = refEntry
			if entry.ReferenceID.Compare(session.State.StateID) != 0 {
				session.UnchangedFiles++
			}
		} else {
			session.LogVerbose("SYMLINK %s -> %s", path, sym)
		}

		session.Files++
		session.reference.storeReference(entry)

	} else if entry.FileMode&uint32(os.ModeDir) > 0 {
		entry.ContentType = ContentTypeDirectory
		entry.FileSize = 0

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
		if refEntry != nil && compareEntries(info, entry, refEntry) {
			// It's the same!
			entry = refEntry

			if entry.ReferenceID.Compare(session.State.StateID) != 0 {
				session.Client.Paint(" ")
				session.UnchangedFiles++

				if !session.reference.loaded { // We are using unique as a diff-size, so first backup (with no reference) has full diff-size
					// TODO: UniqueSize is a here calculated by the backup routine, it should be calculated by the server?
					session.State.UniqueSize += entry.FileSize
				}
			} else {
				// Resuming backup, still count it as unique
				session.State.UniqueSize += entry.FileSize
			}
		} else {
			if entry.FileSize > 0 {
				session.LogVerbose("%s", path)
				if err = session.storeFile(path, entry); err != nil {
					if e := minorPathError(err); e != nil {
						return refEntry, e // Returning refEntry here in case this file existed and could be opened in a previous backup
					}
					return nil, err
				}
				if isOfflineFile(info) {
					var updatedInfo os.FileInfo
					if toplevel {
						updatedInfo, err = os.Stat(path) // At top level we follow symbolic links
					} else {
						updatedInfo, err = os.Lstat(path) // At all other levels we do not
					}
					if updatedInfo != nil {
						core.Log(core.LogTrace, "%+v", updatedInfo)
						updated := session.entryFromFileInfo(updatedInfo)

						if entry.ModTime != updated.ModTime {
							session.LogVerbose("%s changed ModTime (%d != %d) during backup", path, updated.ModTime, entry.ModTime)
							entry.ModTime = updated.ModTime
						}
						if entry.FileSize != updated.FileSize {
							session.LogVerbose("%s changed FileSize (%d != %d) during backup", path, updated.FileSize, entry.FileSize)
							entry.FileSize = updated.FileSize
						}
						if entry.FileMode != updated.FileMode {
							session.LogVerbose("%s changed FileMode (%d != %d) during backup", path, updated.FileMode, entry.FileMode)
							entry.FileMode = updated.FileMode
						}
					}
				}
				// TODO: UniqueSize is a here calculated by the backup routine, it should be calculated by the server?
				session.State.UniqueSize += entry.FileSize
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
	session.reference = newReferenceEngine(session, core.Hash([]byte(datasetName)))
	defer func() {
		session.reference.Close()
		session.reference = nil
	}()

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
		core.AbortOnError(err)
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
		found := len(list.States) - 1
		for ; found >= 0; found-- {
			if list.States[found].StateFlags&core.StateFlagInvalid != core.StateFlagInvalid {
				break
			}
		}
		if found >= 0 {
			referenceBackup := binary.BigEndian.Uint64(list.States[found].State.StateID[:])
			date := time.Unix(0, int64(referenceBackup))
			session.Log("Starting differential backup with %x (%s) as reference.", list.States[found].State.StateID[:], date.Format(time.RFC3339))
			session.reference.start(&list.States[found].State.BlockID)
		} else {
			session.Log("Starting new backup.")
			session.reference.start(nil)
		}
	} else {
		session.Log("Starting full backup.")
		session.reference.start(nil)
	}

	if virtualRootDir != nil {
		var links []core.Byte128
		var entry *FileEntry
		for _, s := range path {
			entry, err = session.storePath(s, true)
			core.AbortOnError(err)
			if entry == nil {
				panic(fmt.Errorf("unable to store %s", s))
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
		core.AbortOnError(err)
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
func (session *BackupSession) Retention(datasetName string, retainDays int, retainWeeks int, retainYearly bool) {
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

	var lastbackup int64
	var lastbackupYear int
	var lastbackupDate int64

	list := session.Client.ListDataset(datasetName)
	for i := len(list.States) - 1; i >= 0; i-- {
		e := list.States[i]
		// Extract the backup date from the stateID
		timestamp := int64(binary.BigEndian.Uint64(e.State.StateID[:]) / 1e9) // nano timestamp in seconds
		backuptime := time.Unix(int64(timestamp), 0)
		year := backuptime.Year()
		date := truncateSecondsToDay(timestamp)

		var throwAway bool
		var reason string

		if i < len(list.States)-2 && (timenow-timestamp) > (24*60*60) && (!retainYearly || year == lastbackupYear) {
			// Not the last or current backup, older than 1 day and not the last of the year (if yearly retention is on)

			if date == lastbackupDate {
				throwAway = true
				reason = fmt.Sprintf("keeping only one daily, %s", time.Unix(lastbackup, 0).Format(time.RFC3339))
			} else if lastbackupDate-date < 7*24*60*60 && date < dailyLimit {
				throwAway = true
				reason = fmt.Sprintf("keeping only one weekly, %s", time.Unix(lastbackup, 0).Format(time.RFC3339))
			} else if weeklyLimit < dailyLimit && date < weeklyLimit {
				throwAway = true
				reason = fmt.Sprintf("older than %d weeks", retainWeeks)
			} else if weeklyLimit >= dailyLimit && date < dailyLimit {
				throwAway = true
				reason = fmt.Sprintf("older than %d days", retainDays)
			}
		}

		if throwAway {
			session.Log("Removing backup %s (%s)", backuptime.Format(time.RFC3339), reason)
			session.Client.RemoveDatasetState(datasetName, e.State.StateID)
		} else {
			core.Log(core.LogDebug, "Keeping backup %s", backuptime.Format(time.RFC3339))
			lastbackup = timestamp
			lastbackupYear = year
			lastbackupDate = date
		}
	}
}

//***********************************************************************//
//                             referenceEngine                           //
//***********************************************************************//

// referenceEngine is used to compare this backup with the previous backup and only go through files that have changed.
// It does this by downloading the last backup structure into a sorted queue and poppin away line by line while finding matches.
type referenceEngine struct {
	session *BackupSession

	path      []string   // path hierarchy, used for traversing up and down subdirectories without having to save full path for each queue entry
	nextEntry *FileEntry // next entry from entryChannel

	loaded bool // indicates that a reference backup was loaded (or started to load)

	wg           sync.WaitGroup
	entryChannel chan *FileEntry // output from goroutine; last backup and recovery structure, sorted queue
	errorChannel chan error      // output from goroutine; reference loading goroutine encountered an error
	stopChannel  chan struct{}   // input to goroutine; signals that the loader should quit
	stopped      bool

	virtualRoot  map[string]string
	datasetNameH core.Byte128
	cacheCurrent *os.File
}

var entryEOD = &FileEntry{} // end of dir marker

func (r *referenceEngine) cacheName(state string) string {
	return fmt.Sprintf("%s.%s.cache", base64.RawURLEncoding.EncodeToString(r.datasetNameH[:]), state)
}

func (r *referenceEngine) cacheFilePathName(rootID core.Byte128) string {
	return filepath.Join(LocalStoragePath, r.cacheName(base64.RawURLEncoding.EncodeToString(rootID[:])))
}

// Stop reference loader
func (r *referenceEngine) stop() {
	if r.stopChannel != nil {
		close(r.stopChannel)
		r.wg.Wait()
		r.stopChannel = nil
	}
}

// Start reference loader
func (r *referenceEngine) start(rootBlockID *core.Byte128) {
	if r.stopChannel != nil {
		panic(errors.New("assert, r.stopChannel != nil, we called start twice"))
	}

	// Create new channels and start a new worker goroutine
	r.stopChannel = make(chan struct{})
	r.errorChannel = make(chan error, 1)
	r.entryChannel = make(chan *FileEntry, 100)

	go r.loader(rootBlockID)
}

func (r *referenceEngine) pushChannelEntry(entry *FileEntry) {
	if r.stopped {
		panic(errors.New("assert, pushChannelEntry was called after reference engine was signalled to stop"))
	}
	select {
	case <-r.stopChannel:
		core.Log(core.LogDebug, "Reference loader received stop signal")
		r.stopped = true
		panic(errors.New("reference loader was stopped"))
	case r.entryChannel <- entry:
		// Dispatched next reference entry
		return
	}
}

// downloadReference adds a subdir structure at the current loadpoint
func (r *referenceEngine) downloadReference(referenceBlockID core.Byte128) {
	var refdir DirectoryBlock

	blockData := r.session.Client.ReadBlock(referenceBlockID).Data
	refdir.Unserialize(&blockData)
	blockData.Release()

	for _, e := range refdir.File {
		r.pushChannelEntry(e)
		if e.ContentType == ContentTypeDirectory {
			r.downloadReference(e.ContentBlockID)
		}
	}
	r.pushChannelEntry(entryEOD)
}

func (r *referenceEngine) loadResumeFile(filename string) {
	treedepth := 0
	func() {
		defer func() {
			if !r.stopped {
				if r := recover(); r != nil {
					core.Log(core.LogDebug, "Non-fatal error encountered while resuming backup %s : %v", filename, r)
				}
			}
		}()
		cacheRecover, _ := os.Open(filepath.Join(LocalStoragePath, filename))
		if cacheRecover != nil {
			defer func() {
				core.AbortOnError(cacheRecover.Close())
			}()
			core.Log(core.LogInfo, "Opened resume cache %s", cacheRecover.Name())

			info, err := cacheRecover.Stat()
			core.AbortOnError(err)
			cacheSize := info.Size()
			reader := bufio.NewReader(cacheRecover)

			skipcheck := 0
			var resumeID core.Byte128
			for offset := int64(0); offset < cacheSize; {
				var entry FileEntry
				core.Log(core.LogTrace, "Read cache entry at %x", offset)
				offset += int64(entry.Unserialize(reader))
				if resumeID.Compare(entry.ReferenceID) < 0 {
					// We're guessing the resume referenceID just to make changedFiles count a little better
					resumeID.Set(entry.ReferenceID[:])
				}

				if entry.FileName == "" { // EOD
					treedepth--
					if skipcheck > 0 {
						skipcheck--
					}
				} else {
					if entry.ContentType == ContentTypeDirectory {
						treedepth++
						if skipcheck > 0 {
							skipcheck++
						} else if r.session.Client.VerifyBlock(entry.ContentBlockID) {
							core.Log(core.LogTrace, "Cache entry for %s verified against server", entry.FileName)
							skipcheck = 1
						}
					} else if skipcheck > 0 {
						core.Log(core.LogTrace, "Skipping cache verification for %s as parent is already verified", entry.FileName)
					} else if !entry.HasContentBlockID() {
						core.Log(core.LogTrace, "Cache entry for %s has no content to verify", entry.FileName)
					} else if r.session.Client.VerifyBlock(entry.ContentBlockID) {
						core.Log(core.LogTrace, "Cache entry for %s verified against server", entry.FileName)
					} else {
						core.Log(core.LogTrace, "Unable to verify %s against server", entry.FileName)
						continue
					}

					if entry.ReferenceID.Compare(resumeID) == 0 {
						entry.ReferenceID = r.session.State.StateID // self reference
					}
				}
				r.pushChannelEntry(&entry)
			}
		}
	}()

	// Insert any missing EOD marker into queue
	for ; treedepth > 0; treedepth-- {
		r.pushChannelEntry(entryEOD)
	}
}

func (r *referenceEngine) loader(rootBlockID *core.Byte128) {
	r.wg.Add(1)
	defer func() {
		if err := recover(); err != nil {
			if r.stopped {
				core.Log(core.LogDebug, "Reference loader stopped gracefully")
			} else {
				core.Log(core.LogDebug, "Error: Panic raised in reference loader process (%v)", err)

				select {
				case r.errorChannel <- fmt.Errorf("panic raised in reference loader process (%v)", err):
					core.Log(core.LogDebug, "Reference loader sent error on error channel")
					r.stopped = true
				default:
					core.Log(core.LogDebug, "Reference loader error channel buffer is full, no message sent")
				}
			}
		}
		close(r.entryChannel)
		r.wg.Done()
	}()

	// Load partial cache files from previous backup attempt (newest first)
	{
		var resumeFileList []os.FileInfo
		recoverMatch := fmt.Sprintf("%s.partial.*.cache", base64.RawURLEncoding.EncodeToString(r.datasetNameH[:]))
		err := filepath.Walk(LocalStoragePath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if match, _ := filepath.Match(recoverMatch, info.Name()); match {
				resumeFileList = append(resumeFileList, info)
			}
			return nil
		})
		core.AbortOnError(err)
		sort.Slice(resumeFileList, func(i, j int) bool {
			return resumeFileList[i].ModTime().Before(resumeFileList[j].ModTime())
		})
		for i := len(resumeFileList) - 1; i >= 0; i-- {
			r.loadResumeFile(resumeFileList[i].Name())
			// Remove the resume file after it is consumed. Yes I know we could lose the last n* cached entries
			// if process is aborted. But it is not that important, it's better to clean up to avoid downward
			// spirals of making resume file after resume file after resume file
			err := os.Remove(resumeFileList[i].Name())
			if err != nil && !os.IsNotExist(err) {
				core.AbortOnError(err)
			}
		}
	}

	// Load previous completed backup
	if rootBlockID != nil {
		r.loaded = true
		// Check if we have last backup cached on disk
		cacheLast, _ := os.Open(r.cacheFilePathName(*rootBlockID))
		if cacheLast != nil {
			defer func() {
				core.AbortOnError(cacheLast.Close())
			}()
			core.Log(core.LogInfo, "Opened local cache %s", cacheLast.Name())

			info, err := cacheLast.Stat()
			core.AbortOnError(err)
			cacheSize := info.Size()
			reader := bufio.NewReader(cacheLast)
			for offset := int64(0); offset < cacheSize; {
				var entry FileEntry
				core.Log(core.LogTrace, "Read cache entry at %x", offset)
				offset += int64(entry.Unserialize(reader))
				r.pushChannelEntry(&entry)
			}
		} else {
			core.Log(core.LogTrace, "Downloading block %x to local cache", rootBlockID)
			r.downloadReference(*rootBlockID)
		}
	}
}

func (r *referenceEngine) popChannelEntry() *FileEntry {
	select {
	case e := <-r.errorChannel:
		core.Log(core.LogError, "Reference loader encountered an error")
		panic(e)
	case e := <-r.entryChannel:
		return e
	}
}

// popReference pops the first queue entry and sets the path hierarchy correctly
func (r *referenceEngine) popReference() *FileEntry {
	e := r.nextEntry
	if e == nil {
		e = r.popChannelEntry()
	}
	r.nextEntry = nil

	if e != nil {
		if e.ContentType == ContentTypeDirectory {
			r.path = append(r.path, string(e.FileName))
		} else if string(e.FileName) == "" && len(r.path) > 0 { // EOD
			r.path = r.path[:len(r.path)-1]
		}
	}
	return e
}

// peekPath returns the full path of the next queue entry
func (r *referenceEngine) peekPath() (path string) {
	if r.nextEntry == nil {
		r.nextEntry = r.popChannelEntry()
	}
	if r.nextEntry != nil {
		path = r.joinPath(string(r.nextEntry.FileName))
	}
	return
}

// joinPath puts the path hierarchy list together to a slash separated path string
func (r *referenceEngine) joinPath(elem string) (path string) {
	for _, p := range r.path {
		path = filepath.Join(path, p)
	}
	path = filepath.Join(path, elem)
	return
}

// findReference tries to find a specified path in the last backup structure
func (r *referenceEngine) findReference(path string) *FileEntry {
	// Convert absolute local path to virtual relative path
	for rel, abs := range r.virtualRoot {
		if len(path) >= len(abs) && path[0:len(abs)] == abs {
			path = rel + path[len(abs):]
			break
		}
	}

	for {
		p := r.peekPath()
		if pathLess(p, path) && r.popReference() != nil {
			core.Log(core.LogTrace, "Reference %s < %s, roll forward", p, path)
			continue
		} else if p == path {
			core.Log(core.LogTrace, "Reference %s == %s", p, path)
			return r.popReference()
		} else {
			core.Log(core.LogTrace, "Reference %s > %s", p, path)
			break
		}
	}
	return nil
}

func (r *referenceEngine) reserveReference(entry *FileEntry) (location int64) {
	core.ASSERT(r.cacheCurrent != nil, "cacheCurrent == nil on reserveReference in an active referenceEngine")
	l, err := r.cacheCurrent.Seek(0, io.SeekCurrent)
	core.AbortOnError(err)
	entry.Serialize(r.cacheCurrent)
	return l
}

func (r *referenceEngine) storeReference(entry *FileEntry) {
	core.ASSERT(r.cacheCurrent != nil, "cacheCurrent == nil on storeReference in an active referenceEngine")
	entry.Serialize(r.cacheCurrent)
}

func (r *referenceEngine) storeReferenceDir(entry *FileEntry, location int64) {
	core.ASSERT(r.cacheCurrent != nil, "cacheCurrent == nil on storeReferenceDir in an active referenceEngine")
	_, err := r.cacheCurrent.Seek(location, io.SeekStart)
	core.AbortOnError(err)
	entry.Serialize(r.cacheCurrent)

	_, err = r.cacheCurrent.Seek(0, io.SeekEnd)
	core.AbortOnError(err)
	entryEOD.Serialize(r.cacheCurrent)
}

func (r *referenceEngine) Commit(rootID core.Byte128) {
	core.ASSERT(r.cacheCurrent != nil, "cacheCurrent must not be nil on commit")

	tempPath := r.cacheCurrent.Name()
	newCachePath := r.cacheFilePathName(rootID)

	cleanup := fmt.Sprintf("%s.*.cache*", base64.RawURLEncoding.EncodeToString(r.datasetNameH[:]))
	err := filepath.Walk(LocalStoragePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == tempPath {
			// Current temp cache, do not delete it
			return nil
		}
		if match, _ := filepath.Match(cleanup, info.Name()); match {
			err := os.Remove(path)
			if err != nil && !os.IsNotExist(err) {
				core.AbortOnError(err)
			}
		}
		return nil
	})
	core.AbortOnError(err)
	core.AbortOnError(r.cacheCurrent.Close())
	core.AbortOnError(os.Rename(tempPath, newCachePath))
	r.cacheCurrent = nil
}
func (r *referenceEngine) Close() {
	r.stop()

	// If not commited, we need to close and save the current cache
	if r.cacheCurrent != nil {
		currentName := r.cacheCurrent.Name()
		core.AbortOnError(r.cacheCurrent.Close())
		r.cacheCurrent = nil

		partialName := (func() string {
			number := time.Now().Unix()
			for {
				name := filepath.Join(LocalStoragePath, r.cacheName(fmt.Sprintf("partial.%08x", number)))
				info, _ := os.Stat(name)
				if info == nil {
					return name
				}
				number++
			}
		})()

		core.Log(core.LogDebug, "Saving %s as recovery cache %s", currentName, partialName)
		core.AbortOnError(os.Rename(currentName, partialName))
	}
}
func newReferenceEngine(session *BackupSession, datasetNameH core.Byte128) *referenceEngine {
	r := &referenceEngine{
		session:      session,
		datasetNameH: datasetNameH,
	}

	var err error
	r.cacheCurrent, err = os.CreateTemp(LocalStoragePath, r.cacheName("temp.*"))
	core.AbortOnError(err)

	return r
}
