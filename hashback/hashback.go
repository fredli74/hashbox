//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	//"log"
	//"net/http"
	//_ "net/http/pprof"

	cmd "bitbucket.org/fredli74/cmdparser"
	"bitbucket.org/fredli74/hashbox/core"

	"github.com/smtc/rollsum"

	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const PROGRESS_INTERVAL_SECS = 10 * time.Second
const MAX_BLOCK_SIZE int = 8 * 1024 * 1024 // 8MiB max blocksize
const MIN_BLOCK_SIZE int = 64 * 1024       // 64kb minimum blocksize (before splitting it)

const QUEUE_MAX_BYTES int = 48 * 1024 * 1024 // 48 MiB max memory (will use up to CPU*MAX_BLOCK_SIZE more when compressing)

var DefaultIgnoreList []string // Default ignore list, populated by init() function from each platform

// TODO: do we need this when we do not follow symlinks?
// const MAX_DEPTH int = 512 // Safety limit to avoid cyclic symbolic links and such

var DEBUG bool = false

func Debug(format string, a ...interface{}) {
	if DEBUG {
		fmt.Print("DEBUG: ")
		fmt.Printf(format, a...)
		fmt.Println()
	}
}

func SoftError(err error) {
	fmt.Println("!!!", err)
}
func HardError(err error) {
	panic(err)
}

func SerializeToBuffer(what core.Serializer) []byte {
	buf := bytes.NewBuffer(nil)
	what.Serialize(buf)
	return buf.Bytes()
}
func UnserializeFromBuffer(data []byte, what core.Unserializer) core.Unserializer {
	buf := bytes.NewReader(data)
	what.Unserialize(buf)
	return what
}
func SerializeToByteArray(what core.Serializer) core.ByteArray {
	var output core.ByteArray
	what.Serialize(&output)
	return output
}

type FileInfoSlice []os.FileInfo

func (p FileInfoSlice) Len() int           { return len(p) }
func (p FileInfoSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }
func (p FileInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func pathLess(a, b string) bool {
	return strings.Replace(a, string(os.PathSeparator), "\x01", -1) < strings.Replace(b, string(os.PathSeparator), "\x01", -1)
}

type FileEntry struct {
	FileName       core.String  // serialize as uint32(len) + []byte
	FileSize       int64        // (!dir)
	FileMode       uint32       // ModeType | ModeTemporary | ModeSetuid | ModeSetgid | ModePerm
	ModTime        int64        //
	ReferenceID    core.Byte128 // BackupID
	ContentType    uint8        // 0 = no data (empty file), 1 = DirectoryBlock, 2 = FileData, 3 = FileChainBlock, 4 = Symbolic link
	ContentBlockID core.Byte128 // only written for type 1,2 and 3
	DecryptKey     core.Byte128 // only written for type 2
	FileLink       core.String  // only written for type 4
	// TODO: Add Platform specific data  hidden files, file permissions etc.
}

const (
	ContentTypeEmpty     = 0
	ContentTypeDirectory = 1
	ContentTypeFileData  = 2
	ContentTypeFileChain = 3
	ContentTypeSymLink   = 4
)

func (e FileEntry) HasContentBlockID() bool {
	return (e.ContentType == ContentTypeDirectory || e.ContentType == ContentTypeFileData || e.ContentType == ContentTypeFileChain)
}
func (e FileEntry) HasDecryptKey() bool {
	return (e.ContentType == ContentTypeFileData)
}
func (e FileEntry) HasFileLink() bool {
	return (e.ContentType == ContentTypeSymLink)
}

func (e FileEntry) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, uint32(0x66656E74)) // "fent"
	size += e.FileName.Serialize(w)
	size += core.WriteOrPanic(w, e.FileSize)
	size += core.WriteOrPanic(w, e.FileMode)
	size += core.WriteOrPanic(w, e.ModTime)
	size += e.ReferenceID.Serialize(w)

	size += core.WriteOrPanic(w, e.ContentType)
	if e.HasContentBlockID() {
		size += e.ContentBlockID.Serialize(w)
	}
	if e.HasDecryptKey() {
		size += e.DecryptKey.Serialize(w)
	}
	if e.HasFileLink() {
		size += e.FileLink.Serialize(w)
	}
	return
}
func (e *FileEntry) Unserialize(r io.Reader) (size int) {
	var check uint32
	size += core.ReadOrPanic(r, &check)
	if check != 0x66656E74 { // "fent"
		panic(errors.New("Corrupted FileEntry!"))
	}
	size += e.FileName.Unserialize(r)
	size += core.ReadOrPanic(r, &e.FileSize)
	size += core.ReadOrPanic(r, &e.FileMode)
	size += core.ReadOrPanic(r, &e.ModTime)
	size += e.ReferenceID.Unserialize(r)

	size += core.ReadOrPanic(r, &e.ContentType)
	if e.HasContentBlockID() {
		size += e.ContentBlockID.Unserialize(r)
	}
	if e.HasDecryptKey() {
		size += e.DecryptKey.Unserialize(r)
	}
	if e.HasFileLink() {
		size += e.FileLink.Unserialize(r)
	}
	return size
}

type FileChainBlock struct {
	//	ChainLength uint32
	ChainBlocks []core.Byte128
	DecryptKeys []core.Byte128
}

func (b FileChainBlock) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, uint32(0x6663686E)) // "fchn"
	size += core.WriteOrPanic(w, uint32(len(b.ChainBlocks)))
	for i := range b.ChainBlocks {
		size += b.ChainBlocks[i].Serialize(w)
		size += b.DecryptKeys[i].Serialize(w)
	}
	return
}
func (b *FileChainBlock) Unserialize(r io.Reader) (size int) {
	var check uint32
	size += core.ReadOrPanic(r, &check)
	if check != 0x6663686E { // "fchn"
		panic(errors.New("Corrupted FileChainBlock!"))
	}
	var l uint32
	size += core.ReadOrPanic(r, &l)
	b.ChainBlocks = make([]core.Byte128, l)
	b.DecryptKeys = make([]core.Byte128, l)
	for i := range b.ChainBlocks {
		size += b.ChainBlocks[i].Unserialize(r)
		size += b.DecryptKeys[i].Unserialize(r)
	}
	return
}

type DirectoryBlock struct {
	File []*FileEntry
}

func (b DirectoryBlock) Serialize(w io.Writer) (size int) {
	size += core.WriteOrPanic(w, uint32(0x64626C6B)) // "dblk"
	size += core.WriteOrPanic(w, uint32(len(b.File)))
	for _, f := range b.File {
		size += f.Serialize(w)
	}
	return
}
func (b *DirectoryBlock) Unserialize(r io.Reader) (size int) {
	var check uint32
	size += core.ReadOrPanic(r, &check)
	if check != 0x64626C6B { // "dblk"
		panic(errors.New("Corrupted DirectoryBlock!"))
	}
	var l uint32
	size += core.ReadOrPanic(r, &l)
	b.File = make([]*FileEntry, l)
	for i := range b.File {
		b.File[i] = new(FileEntry)
		size += b.File[i].Unserialize(r)
	}
	return
}

type BackupSession struct {
	ServerString string
	User         string
	AccessKey    *core.Byte128
	BackupKey    *core.Byte128
	FullBackup   bool
	Verbose      bool
	ShowProgress bool
	Paint        bool

	Client   *core.Client
	State    *core.DatasetState
	Start    time.Time
	Progress time.Time

	ReadData       int64
	WriteData      int64
	Directories    int
	Files          int
	UnchangedFiles int

	ignoreList []ignoreEntry
	reference  *referenceEngine
}

func NewBackupSession() *BackupSession {
	return &BackupSession{Start: time.Now()}
}

func (session *BackupSession) Connect() *core.Client {
	if session.AccessKey == nil || session.BackupKey == nil {
		panic(errors.New("Missing -password option"))
	}
	// fmt.Printf("%x\n", *session.AccessKey)
	// fmt.Printf("%x\n", *session.BackupKey)

	fmt.Println("Connecting to", session.ServerString)
	conn, err := net.Dial("tcp", session.ServerString)
	if err != nil {
		panic(err)
	}

	client := core.NewClient(conn, session.User, *session.AccessKey)
	client.QueueMax = QUEUE_MAX_BYTES
	client.Paint = session.Paint
	session.Client = client
	return session.Client
}
func (session *BackupSession) Close() {
	session.Client.Close()
}

func (session *BackupSession) Log(v ...interface{}) {
	if session.Verbose {
		if session.Paint {
			fmt.Println()
		}
		fmt.Println(v...)
	}
}
func (session *BackupSession) Important(v ...interface{}) {
	if session.Paint {
		fmt.Println()
	}
	fmt.Println(v...)
}

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
func (session *BackupSession) PrintRestoreProgress() {
	var compression float64
	if session.WriteData > 0 {
		compression = 100.0 * (float64(session.WriteData) - float64(session.ReadData)) / float64(session.WriteData)
	}
	fmt.Printf("*** %.1f min, read: %s (%.0f%% compr), write: %s, %d folders, %d files\n",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), compression, core.HumanSize(session.WriteData), session.Directories, session.Files)

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

	var fileData core.ByteArray
	defer fileData.Release()

	for offset := int64(0); offset < int64(entry.FileSize); {
		// TODO: add progress on storePath as well because incremental backups do not reach down here that often
		if session.ShowProgress && time.Now().After(session.Progress) {
			session.PrintStoreProgress()
			session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
		}

		var left int64 = int64(entry.FileSize) - offset
		var maxBlockSize int = MAX_BLOCK_SIZE
		if left < int64(maxBlockSize) {
			maxBlockSize = int(left)
		}

		var blockData core.ByteArray

		// Fill the fileData buffer
		if fil == nil {
			panic("ASSERT! fil == nil inside storeFile inner loop")
		}

		core.CopyNOrPanic(&fileData, fil, maxBlockSize-fileData.Len())
		fileData.ReadSeek(0, core.SEEK_CUR)

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
					rollInBase, _ = rollIn.ReadSeek(len(rollInSlice), core.SEEK_CUR)
					rollInSlice, _ = rollIn.ReadSlice()
				}
				if rollOutPos-rollOutBase >= len(rollOutSlice) { // Next slice please
					rollOutBase, _ = rollOut.ReadSeek(len(rollOutSlice), core.SEEK_CUR)
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

var entryEOD = &FileEntry{} // end of dir marker
type referenceEngine struct {
	client    *core.Client
	path      []string
	queue     []*FileEntry
	loadpoint int

	lock sync.Mutex
}

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
func (r *referenceEngine) joinPath() (path string) {
	for _, p := range r.path {
		path = filepath.Join(path, p)
	}
	return
}
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
func (r *referenceEngine) findAndReuseReference(path string, entry *FileEntry) bool {
	refEntry := r.findReference(path)
	if refEntry != nil && refEntry.FileName == entry.FileName && refEntry.FileSize == entry.FileSize && refEntry.FileMode == entry.FileMode && refEntry.ModTime == entry.ModTime && refEntry.FileLink == entry.FileLink {
		// It's the same!
		*entry = *refEntry
		return true
	}
	return false
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
		list := session.Client.ListDataset(cmd.Args[2])
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

func (session *BackupSession) restoreFileData(f *os.File, blockID core.Byte128, DecryptKey core.Byte128) error {
	if session.ShowProgress && time.Now().After(session.Progress) {
		session.PrintRestoreProgress()
		session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	}

	block := session.Client.ReadBlock(blockID)
	if !block.VerifyBlock() {
		panic(errors.New(fmt.Sprintf("Block %x corrupted, hash does not match")))
	}

	session.ReadData += int64(block.CompressedSize)
	// TODO: Decrypt block

	if !block.VerifyBlock() {
		panic(errors.New("Unable to verify block content, data is corrupted"))
	}
	d := block.Data
	d.ReadSeek(0, core.SEEK_SET)
	core.CopyOrPanic(f, &d)
	session.WriteData += int64(d.Len())
	d.Release()
	return nil
}

func (backup *BackupSession) restoreEntry(e *FileEntry, path string) error {
	localname := filepath.Join(path, string(e.FileName))
	backup.Log(localname)

	if e.FileMode&uint32(os.ModeSymlink) > 0 {
		switch e.ContentType {
		case ContentTypeSymLink:
			if err := os.Symlink(string(e.FileLink), localname); err != nil {
				return err
			}
		default:
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a Symlink (%d)", e.ContentType)))
		}
	} else if e.FileMode&uint32(os.ModeDir) > 0 {
		if err := os.MkdirAll(localname, os.FileMode(e.FileMode) /*&os.ModePerm*/); err != nil {
			return err
		}

		switch e.ContentType {
		case ContentTypeDirectory:
			if err := backup.restoreDir(e.ContentBlockID, localname); err != nil {
				return err
			}
		case ContentTypeEmpty:
		default:
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a directory (%d)", e.ContentType)))
		}
		backup.Directories++
	} else {
		if e.ContentType != ContentTypeFileChain && e.ContentType != ContentTypeFileData && e.ContentType != ContentTypeEmpty {
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a file (%d)", e.ContentType)))
		}
		err := func() error {
			fil, err := os.OpenFile(localname, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(e.FileMode) /*&os.ModePerm*/)
			if err != nil {
				return err
			}
			defer fil.Close()

			switch e.ContentType {
			case ContentTypeEmpty:
				// No data
			case ContentTypeFileChain:
				var chain FileChainBlock

				blockData := backup.Client.ReadBlock(e.ContentBlockID).Data
				chain.Unserialize(&blockData)
				blockData.Release()

				for i := range chain.ChainBlocks {
					if err := backup.restoreFileData(fil, chain.ChainBlocks[i], chain.DecryptKeys[i]); err != nil {
						return err
					}
				}
			case ContentTypeFileData:
				if err := backup.restoreFileData(fil, e.ContentBlockID, e.DecryptKey); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
		if err := os.Chtimes(localname, time.Now(), time.Unix(0, e.ModTime)); err != nil {
			return err
		}
		backup.Files++
	}
	return nil
}

func (backup *BackupSession) restoreDir(blockID core.Byte128, path string) error {
	var dir DirectoryBlock

	blockData := backup.Client.ReadBlock(blockID).Data
	dir.Unserialize(&blockData)
	blockData.Release()

	for _, e := range dir.File {
		if err := backup.restoreEntry(e, path); err != nil {
			return err
		}
	}
	return nil
}

func (backup *BackupSession) changeDir(blockID core.Byte128, pathList []string) (*DirectoryBlock, int, error) {
	var dir DirectoryBlock
	blockData := backup.Client.ReadBlock(blockID).Data
	dir.Unserialize(&blockData)
	blockData.Release()

	if len(pathList) > 0 {
		for _, f := range dir.File {
			if string(f.FileName) == pathList[0] && f.ContentType == ContentTypeDirectory {
				return backup.changeDir(f.ContentBlockID, pathList[1:])
			}
		}
		return &dir, len(pathList), errors.New("Path not found")
	}
	return &dir, len(pathList), nil
}

func (backup *BackupSession) findPathMatch(rootBlockID core.Byte128, path string) ([]*FileEntry, error) {
	//var fileList []*FileEntry

	pathList := core.SplitPath(path)
	dir, unmatched, err := backup.changeDir(rootBlockID, pathList)
	if unmatched == 1 {
		filtered := dir.File[:0]
		for _, x := range dir.File {
			if match, _ := filepath.Match(pathList[len(pathList)-1], string(x.FileName)); match {
				filtered = append(filtered, x)
			}
		}
		if len(filtered) > 0 {
			return filtered, nil
		} else {
			return nil, errors.New(fmt.Sprintf("No match found on \"%s\"", path))
		}
	} else if unmatched > 0 {
		return nil, err
	} else {
		return dir.File, nil
	}
}
func filterDir(dir []*FileEntry, match string) []*FileEntry {
	filtered := dir[:0]
	for _, x := range dir {
		if match, _ := filepath.Match(match, string(x.FileName)); match {
			filtered = append(filtered, x)
		}
	}
	return filtered
}

func main() {
	var lockFile *core.LockFile

	//runtime.SetBlockProfileRate(1000)
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

	/*	defer func() {
		// Panic error handling
		if r := recover(); r != nil {
			fmt.Println(r)
			if lockFile != nil {
				lockFile.Close()
			}
			os.Exit(1)
		}
	}()*/

	// Figure out where to load/save options
	var preferencesBaseFolder = "./"
	usr, err := user.Current()
	if err == nil {
		f := usr.HomeDir
		s, err := os.Stat(f)
		if err == nil && s.IsDir() {
			preferencesBaseFolder = f
		}
	}

	session := NewBackupSession()

	cmd.Title = "Hashback 0.2.5-go (Hashbox Backup Client)"
	cmd.OptionsFile = filepath.Join(preferencesBaseFolder, ".hashback", "options.json")

	cmd.BoolOption("debug", "", "Debug output", &DEBUG, cmd.Hidden)

	cmd.StringOption("user", "", "<username>", "Username", &session.User, cmd.Preference|cmd.Required)
	var accesskey []byte
	cmd.ByteOption("accesskey", "", "", "Hashbox server accesskey", &accesskey, cmd.Preference|cmd.Hidden).OnChange(func() {
		var key core.Byte128
		copy(key[:16], accesskey[:])
		session.AccessKey = &key
	})
	var backupkey []byte
	cmd.ByteOption("backupkey", "", "", "Hashbox server backupkey", &backupkey, cmd.Preference|cmd.Hidden).OnChange(func() {
		var key core.Byte128
		copy(key[:16], backupkey[:])
		session.BackupKey = &key
	})
	var password string
	cmd.StringOption("password", "", "<password>", "Password", &password, cmd.Standard).OnChange(func() {
		{
			key := GenerateAccessKey(session.User, password)
			session.AccessKey = &key
			accesskey = session.AccessKey[:]
		}
		{
			key := GenerateBackupKey(session.User, password)
			session.BackupKey = &key
			backupkey = session.BackupKey[:]
		}
	}).OnSave(func() {
		if session.User == "" {
			panic(errors.New("Unable to save login unless both user and password options are specified"))
		}
	})

	cmd.StringOption("server", "", "<ip>:<port>", "Hashbox server address", &session.ServerString, cmd.Preference|cmd.Required)
	cmd.BoolOption("full", "", "Force a non-incremental store", &session.FullBackup, cmd.Preference)
	cmd.BoolOption("v", "", "Show verbose output", &session.Verbose, cmd.Preference)
	cmd.BoolOption("progress", "", "Show progress during store", &session.ShowProgress, cmd.Preference)
	cmd.BoolOption("paint", "", "Paint!", &session.Paint, cmd.Preference).OnChange(func() {
		if session.Paint {
			session.Verbose = false
			session.ShowProgress = false
		}
	})

	cmd.Command("info", "", func() {
		session.Connect()
		defer session.Close()

		info := session.Client.GetAccountInfo()
		var hashbackEnabled bool = false
		var dlist []core.Dataset
		for _, d := range info.DatasetList {
			if d.Name == "\x07HASHBACK_DEK" {
				hashbackEnabled = true
			}
			if d.Name[0] > 32 {
				dlist = append(dlist, d)
			}
		}

		fmt.Println("* TODO: Add quota and total size info")
		if hashbackEnabled {
			fmt.Println("Account is setup for Hashback")
		} else {
			fmt.Println("Account is NOT setup for Hashback")
		}
		fmt.Println("")
		if len(dlist) == 0 {
			fmt.Println("No datasets")
		} else {
			fmt.Println("Size        Dataset")
			fmt.Println("--------    -------")
			for _, d := range dlist {
				fmt.Printf("%8s    %s\n", core.HumanSize(d.Size), d.Name)
			}
		}

	})
	cmd.Command("list", "<dataset> [(<backup id>|.) [\"<path>\"]]", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}

		session.Connect()
		defer session.Close()

		list := session.Client.ListDataset(cmd.Args[2])
		if len(list.States) > 0 {

			if len(cmd.Args) < 4 {

				fmt.Println("Backup id                           Backup date                  Total size     Diff prev")
				fmt.Println("--------------------------------    -------------------------    ----------    ----------")

				for _, s := range list.States {
					timestamp := binary.BigEndian.Uint64(s.StateID[:])
					date := time.Unix(0, int64(timestamp))

					fmt.Printf("%-32x    %-25s    %10s    %10s\n", s.StateID, date.Format(time.RFC3339), core.HumanSize(s.Size), core.HumanSize(s.UniqueSize))
				}
			} else {
				var state *core.DatasetState
				for i, s := range list.States {
					if cmd.Args[3] == "." || fmt.Sprintf("%x", s.StateID[:]) == cmd.Args[3] {
						state = &list.States[i]
					}
				}
				if state == nil {
					panic(errors.New("Backup id not found"))
				}

				var filelist []*FileEntry
				var listpath string = "*"
				if len(cmd.Args) > 4 {
					listpath = cmd.Args[4]
				}
				filelist, err = session.findPathMatch(state.BlockID, listpath)
				if err != nil {
					panic(err)
				}

				fmt.Printf("Listing %s\n", listpath)
				if len(filelist) > 0 {
					for _, f := range filelist {
						var niceDate, niceSize string
						date := time.Unix(0, int64(f.ModTime))

						if time.Since(date).Hours() > 24*300 { // 300 days
							niceDate = date.Format("Jan _2  2006")
						} else {
							niceDate = date.Format("Jan _2 15:04")
						}
						if f.ContentType != ContentTypeDirectory {
							niceSize = core.ShortHumanSize(f.FileSize)
						}
						fmt.Printf("%-10s  %6s   %-12s   %s\n", os.FileMode(f.FileMode), niceSize, niceDate /*date.Format(time.RFC3339)*/, f.FileName)
					}
				} else {
					fmt.Println("No files matching")
				}
			}
		} else {
			fmt.Println("Dataset is empty or does not exist")
		}

	})

	var pidName string = ""
	cmd.StringOption("pid", "store", "<filename>", "Create a PID file (lock-file)", &pidName, cmd.Standard)
	cmd.StringListOption("ignore", "store", "<pattern>", "Ignore files matching pattern", &DefaultIgnoreList, cmd.Standard|cmd.Preference)
	cmd.Command("store", "<dataset> (<folder> | <file>)...", func() {
		for _, d := range DefaultIgnoreList {
			ignore := ignoreEntry{pattern: d, match: core.ExpandEnv(d)} // Expand ignore patterns

			if ignore.match == "" {
				continue
			}
			if _, err := filepath.Match(ignore.match, "ignore"); err != nil {
				panic(errors.New(fmt.Sprintf("Invalid ignore pattern %s", ignore.pattern)))
			}

			if os.IsPathSeparator(ignore.match[len(ignore.match)-1]) {
				ignore.match = ignore.match[:len(ignore.match)-1]
				ignore.dirmatch = true
			}
			if strings.IndexRune(ignore.match, os.PathSeparator) >= 0 { // path in pattern
				ignore.pathmatch = true
			}

			session.ignoreList = append(session.ignoreList, ignore)
		}

		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}
		if len(cmd.Args) < 4 {
			panic(errors.New("Missing source file or folder argument"))
		}

		if pidName != "" {
			var err error
			if lockFile, err = core.NewLockFile(pidName); err != nil {
				fmt.Println(err)
				os.Exit(0)
			} else {
				defer lockFile.Close()
			}
		}

		session.Connect()
		defer session.Close()
		session.State = &core.DatasetState{StateID: session.Client.SessionNonce}
		session.Store(cmd.Args[2], cmd.Args[3:]...)
	})
	cmd.Command("restore", "<dataset> (<backup id>|.) [\"<path>\"...] <dest-folder>", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}
		if len(cmd.Args) < 4 {
			panic(errors.New("Missing backup id (or \".\")"))
		}
		if len(cmd.Args) < 5 {
			panic(errors.New("Missing destination folder argument"))
		}

		session.Connect()
		defer session.Close()

		list := session.Client.ListDataset(cmd.Args[2])

		var stateid string = cmd.Args[3]
		var restorepath string = cmd.Args[len(cmd.Args)-1]
		var restorelist []string = cmd.Args[4 : len(cmd.Args)-1]

		var found int = -1
		if stateid == "." {
			found = len(list.States) - 1
		} else {
			for i, s := range list.States {
				if stateid == fmt.Sprintf("%x", s.StateID) {
					found = i
					break
				}
			}
			if found < 0 {
				panic(errors.New("Backup id " + cmd.Args[3] + " not found in dataset " + cmd.Args[2]))
			}
		}
		if found < 0 {
			panic(errors.New("No backup found under dataset " + cmd.Args[2]))
		}

		timestamp := binary.BigEndian.Uint64(list.States[found].StateID[:])
		date := time.Unix(0, int64(timestamp))
		fmt.Printf("Restoring from %x (%s) to path %s\n", list.States[found].StateID, date.Format(time.RFC3339), restorepath)

		if err := os.MkdirAll(restorepath, 0777); err != nil {
			panic(err)
		}

		if len(restorelist) > 0 {
			for _, r := range restorelist {
				list, err := session.findPathMatch(list.States[found].BlockID, r)
				if err != nil {
					panic(err)
				}
				for _, e := range list {
					if err := session.restoreEntry(e, restorepath); err != nil {
						panic(err)
					}
				}
			}
		} else {
			if err := session.restoreDir(list.States[found].BlockID, restorepath); err != nil {
				panic(err)
			}
		}
		session.PrintRestoreProgress()
	})

	signalchan := make(chan os.Signal, 1)
	defer close(signalchan)
	signal.Notify(signalchan, os.Interrupt)
	signal.Notify(signalchan, os.Kill)
	go func() {
		for range signalchan {
			if lockFile != nil {
				lockFile.Close()
			}
			os.Exit(2)
		}
	}()

	if err := cmd.Parse(); err != nil {
		panic(err)
	}
}

func GenerateAccessKey(account string, password string) core.Byte128 {
	return core.DeepHmac(20000, append([]byte(account), []byte("*ACCESS*KEY*PAD*")...), core.Hash([]byte(password)))
}
func GenerateBackupKey(account string, password string) core.Byte128 {
	return core.DeepHmac(20000, append([]byte(account), []byte("*ENCRYPTION*PAD*")...), core.Hash([]byte(password)))
}
func GenerateDataEncryptionKey() core.Byte128 {
	var key core.Byte128
	rand.Read(key[:])
	return key
}
func DecryptDataInPlace(cipherdata []byte, key core.Byte128) {
	aesCipher, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}

	aesStream := cipher.NewCBCDecrypter(aesCipher, []byte("*HB*AES*DATA*IV*"))
	aesStream.CryptBlocks(cipherdata, cipherdata)
}
func EncryptDataInPlace(data []byte, key core.Byte128) {
	aesCipher, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}

	aesStream := cipher.NewCBCEncrypter(aesCipher, []byte("*HB*AES*DATA*IV*"))
	aesStream.CryptBlocks(data, data)
}
