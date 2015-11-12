//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	// "log"
	// "net/http"
	// _ "net/http/pprof"
	// "runtime"

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
	"sort"
	"strings"
	"time"
)

const PROGRESS_INTERVAL_SECS = 10 * time.Second
const MAX_BLOCK_SIZE int = 8 * 1024 * 1024 // 8MiB max blocksize
const MIN_BLOCK_SIZE int = 64 * 1024       // 64kb minimum blocksize (before splitting it)

// const MAX_DEPTH int = 512 // Safety limit to avoid cyclic symbolic links and such

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

type FileInfoSlice []os.FileInfo

func (p FileInfoSlice) Len() int           { return len(p) }
func (p FileInfoSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }
func (p FileInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func pathLess(a, b string) bool {
	return strings.Replace(a, "\\", "\x01", -1) < strings.Replace(b, "\\", "\x01", -1)
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

func (e FileEntry) Serialize(w io.Writer) {
	core.WriteOrPanic(w, uint32(0x66656E74))
	e.FileName.Serialize(w)
	core.WriteOrPanic(w, e.FileSize)
	core.WriteOrPanic(w, e.FileMode)
	core.WriteOrPanic(w, e.ModTime)
	e.ReferenceID.Serialize(w)

	core.WriteOrPanic(w, e.ContentType)
	if e.HasContentBlockID() {
		e.ContentBlockID.Serialize(w)
	}
	if e.HasDecryptKey() {
		e.DecryptKey.Serialize(w)
	}
	if e.HasFileLink() {
		e.FileLink.Serialize(w)
	}
}
func (e *FileEntry) Unserialize(r io.Reader) {
	var check uint32
	core.ReadOrPanic(r, &check)
	if check != 0x66656E74 {
		panic(errors.New("Corrupted FileEntry!"))
	}
	e.FileName.Unserialize(r)
	core.ReadOrPanic(r, &e.FileSize)
	core.ReadOrPanic(r, &e.FileMode)
	core.ReadOrPanic(r, &e.ModTime)
	e.ReferenceID.Unserialize(r)

	core.ReadOrPanic(r, &e.ContentType)
	if e.HasContentBlockID() {
		e.ContentBlockID.Unserialize(r)
	}
	if e.HasDecryptKey() {
		e.DecryptKey.Unserialize(r)
	}
	if e.HasFileLink() {
		e.FileLink.Unserialize(r)
	}
}

type FileChainBlock struct {
	//	ChainLength uint32
	ChainBlocks []core.Byte128
	DecryptKeys []core.Byte128
}

func (b FileChainBlock) Serialize(w io.Writer) {
	core.WriteOrPanic(w, uint32(0x6663686E))
	core.WriteOrPanic(w, uint32(len(b.ChainBlocks)))
	for i := range b.ChainBlocks {
		b.ChainBlocks[i].Serialize(w)
		b.DecryptKeys[i].Serialize(w)
	}
}
func (b *FileChainBlock) Unserialize(r io.Reader) {
	var check uint32
	core.ReadOrPanic(r, &check)
	if check != 0x6663686E {
		panic(errors.New("Corrupted FileChainBlock!"))
	}
	var l uint32
	core.ReadOrPanic(r, &l)
	b.ChainBlocks = make([]core.Byte128, l)
	b.DecryptKeys = make([]core.Byte128, l)
	for i := range b.ChainBlocks {
		b.ChainBlocks[i].Unserialize(r)
		b.DecryptKeys[i].Unserialize(r)
	}
}

type DirectoryBlock struct {
	File []*FileEntry
}

func (b DirectoryBlock) Serialize(w io.Writer) {
	core.WriteOrPanic(w, uint32(0x64626C6B))
	core.WriteOrPanic(w, uint32(len(b.File)))
	for _, f := range b.File {
		f.Serialize(w)
	}
}
func (b *DirectoryBlock) Unserialize(r io.Reader) {
	var check uint32
	core.ReadOrPanic(r, &check)
	if check != 0x64626C6B {
		panic(errors.New("Corrupted DirectoryBlock!"))
	}
	var l uint32
	core.ReadOrPanic(r, &l)
	b.File = make([]*FileEntry, l)
	for i := range b.File {
		b.File[i] = new(FileEntry)
		b.File[i].Unserialize(r)
	}
}

type referenceEntry struct {
	Path string
	File *FileEntry
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

	refQueue []referenceEntry // Used to compare against a reference backup during backup
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
	client.Paint = session.Paint
	session.Client = client
	return session.Client
}
func (session *BackupSession) Close() {
	session.Client.Close()
}

func (session *BackupSession) Log(v ...interface{}) {
	if session.Verbose {
		fmt.Println(v...)
	}
}
func (session *BackupSession) PrintStoreProgress() {
	var compression float64
	if session.Client.WriteData > 0 {
		compression = 100.0 * (float64(session.Client.WriteData) - float64(session.Client.WriteDataCompressed)) / float64(session.Client.WriteData)
	}
	sent, skipped, _, queuedsize := session.Client.GetStats()
	fmt.Printf("*** %.1f min, read: %s, write: %s (%.0f%% compr), %d folders, %d/%d files changed, blocks sent %d/%d, queued:%s\n",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), core.HumanSize(session.Client.WriteDataCompressed), compression, session.Directories, session.Files-session.UnchangedFiles, session.Files,
		sent, skipped+sent, core.HumanSize(int64(queuedsize)))

}
func (session *BackupSession) PrintRestoreProgress() {
	var compression float64
	if session.WriteData > 0 {
		compression = 100.0 * (float64(session.WriteData) - float64(session.ReadData)) / float64(session.WriteData)
	}
	fmt.Printf("*** %.1f min, read: %s (%.0f%% compr), write: %s, %d folders, %d files\n",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), compression, core.HumanSize(session.WriteData), session.Directories, session.Files)
}

var filebuffer []byte

func (session *BackupSession) storeFile(path string, entry *FileEntry) error {
	var links []core.Byte128

	chain := FileChainBlock{}

	fil, err := os.OpenFile(path, os.O_RDONLY, 444)
	if err != nil {
		return err
	}
	defer fil.Close()

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

		if filebuffer == nil {
			filebuffer = make([]byte, MAX_BLOCK_SIZE)
		}
		rawdata := filebuffer[:maxBlockSize]
		//rawdata := make([]byte, maxBlockSize)
		{
			rawlength, err := fil.ReadAt(rawdata, offset)
			if err != nil {
				panic(err)
			}

			if rawlength > MIN_BLOCK_SIZE {
				var sum rollsum.Rollsum
				sum.Init()
				var maxd = uint32(0)
				var maxi = rawlength
				for i := 0; i < rawlength; {
					if i >= MIN_BLOCK_SIZE {
						sum.Rollout(rawdata[i-MIN_BLOCK_SIZE])
					}
					sum.Rollin(rawdata[i])
					i++
					if i >= MIN_BLOCK_SIZE {
						d := sum.Digest()
						if d >= maxd {
							maxd = d
							maxi = i
						}
					}
				}
				rawlength = maxi
			}
			rawdata = rawdata[:rawlength]
			offset += int64(rawlength)

			session.ReadData += int64(rawlength)
		}

		// TODO: add encryption and custom compression here
		var datakey core.Byte128

		blockdata := make([]byte, len(rawdata))
		copy(blockdata, rawdata)
		id := session.Client.StoreBlock(core.BlockDataTypeZlib, blockdata, nil)
		//id := session.Client.StoreBlock(core.BlockDataTypeZlib, rawdata, nil)
		links = append(links, id)
		chain.ChainBlocks = append(chain.ChainBlocks, id)
		chain.DecryptKeys = append(chain.DecryptKeys, datakey)
	}

	if len(chain.ChainBlocks) > 1 {
		id := session.Client.StoreBlock(core.BlockDataTypeZlib, SerializeToBuffer(chain), links)
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
			SoftError(err)
		} else if e != nil {
			dir.File = append(dir.File, e)
			if e.HasContentBlockID() {
				links = append(links, e.ContentBlockID)
			}
		}
	}

	id := session.Client.StoreBlock(core.BlockDataTypeZlib, SerializeToBuffer(dir), links)
	entry.ContentType = ContentTypeDirectory
	entry.ContentBlockID = id
	return nil
}

func (session *BackupSession) storePath(path string, toplevel bool) (*FileEntry, error) {
	// At top level we follow symbolic links
	var statFunc func(name string) (os.FileInfo, error)
	if toplevel {
		statFunc = os.Stat
	} else {
		statFunc = os.Lstat
	}

	// Get file info from disk
	info, err := statFunc(path)
	if err != nil {
		return nil, err
	}

	entry := FileEntry{
		FileName:    core.String(info.Name()),
		FileSize:    int64(info.Size()),
		FileMode:    uint32(info.Mode()),
		ModTime:     info.ModTime().UnixNano(),
		ReferenceID: session.State.StateID,
	}

	if entry.FileMode&uint32(os.ModeTemporary) > 0 {
		session.Log("Skipping temporary file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeDevice) > 0 {
		session.Log("Skipping device file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeNamedPipe) > 0 {
		session.Log("Skipping named pipe file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSocket) > 0 {
		session.Log("Skipping socket file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSymlink) > 0 {
		entry.ContentType = ContentTypeSymLink
		sym, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		entry.FileLink = core.String(sym)
		session.findAndReuseReference(path, &entry)
	} else {
		session.Log(path, entry.FileMode)
		if entry.FileMode&uint32(os.ModeDir) > 0 {
			refEntry := session.findReference(path, &entry)
			if err := session.storeDir(path, &entry); err != nil {
				return nil, err
			}
			if refEntry != nil && bytes.Equal(refEntry.ContentBlockID[:], entry.ContentBlockID[:]) {
				entry.ReferenceID = refEntry.ReferenceID
			}

			session.Directories++
		} else {
			same := session.findAndReuseReference(path, &entry)

			if !same && entry.FileSize > 0 {
				if err := session.storeFile(path, &entry); err != nil {
					return nil, err
				}
			} else {
				if session.Client.Paint {
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

func (session *BackupSession) popReference() *FileEntry {
	n := session.refQueue[0]
	session.refQueue = session.refQueue[1:]
	if n.File.ContentType == ContentTypeDirectory {
		session.pushReference(n.Path, n.File.ContentBlockID)
	}
	return n.File
}
func (session *BackupSession) pushReference(path string, referenceBlockID core.Byte128) {
	var refdir DirectoryBlock
	UnserializeFromBuffer(session.Client.ReadBlock(referenceBlockID).Data, &refdir)
	var list []referenceEntry
	for _, r := range refdir.File {
		list = append(list, referenceEntry{filepath.Join(path, string(r.FileName)), r})
	}
	session.refQueue = append(list, session.refQueue...)
}
func (session *BackupSession) findReference(path string, entry *FileEntry) *FileEntry {
	for len(session.refQueue) > 0 {
		if pathLess(session.refQueue[0].Path, path) {
			session.popReference()
		} else if session.refQueue[0].Path == path {
			return session.popReference()
		} else {
			break
		}
	}
	return nil
}
func (session *BackupSession) findAndReuseReference(path string, entry *FileEntry) bool {
	refEntry := session.findReference(path, entry)
	if refEntry != nil && refEntry.FileName == entry.FileName && refEntry.FileSize == entry.FileSize && refEntry.FileMode == entry.FileMode && refEntry.ModTime == entry.ModTime && refEntry.FileLink == entry.FileLink {
		// It's the same!
		*entry = *refEntry
		return true
	}
	return false
}

func (session *BackupSession) Store(datasetName string, path ...string) {
	var referenceBlockID *core.Byte128
	if !session.FullBackup {
		list := session.Client.ListDataset(cmd.Args[2])
		if len(list.States) > 0 {
			referenceBlockID = &list.States[len(list.States)-1].BlockID
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
			UnserializeFromBuffer(session.Client.ReadBlock(*referenceBlockID).Data, &refdir)
			for _, s := range path {
				name := filepath.Base(s)
				for _, r := range refdir.File {
					if string(r.FileName) == name {
						session.refQueue = append(session.refQueue, referenceEntry{s, r})
					}
				}
			}
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
		session.State.BlockID = session.Client.StoreBlock(core.BlockDataTypeZlib, SerializeToBuffer(dir), links)
	} else {
		if referenceBlockID != nil {
			// push the last backup root to reference list
			session.pushReference(path[0], *referenceBlockID)
		}
		e, err := session.storePath(path[0], true)
		if err != nil {
			panic(err)
		}
		session.State.BlockID = e.ContentBlockID
	}

	// Commit all pending writes
	session.Client.Commit()
	session.State.UniqueSize = session.Client.WriteDataCompressed
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
	session.ReadData += int64(block.EncodedSize())
	// TODO: Decrypt block
	// Uncompress block

	if !block.VerifyBlock() {
		panic(errors.New("Unable to verify block content, data is corrupted"))
	}
	core.WriteOrPanic(f, block.Data)
	session.WriteData += int64(block.DecodedSize())
	/*	buf := bytes.NewReader(block.Data)
		zr, err := zlib.NewReader(buf)
		if err != nil {
			return err
		}
		defer zr.Close()
		written, err := io.Copy(f, zr)
		if err != nil {
			return err
		}

		session.WriteData += int64(written)*/
	return nil
}

func (backup *BackupSession) restoreDir(blockID core.Byte128, path string) error {
	var dir DirectoryBlock
	UnserializeFromBuffer(backup.Client.ReadBlock(blockID).Data, &dir)
	for _, e := range dir.File {
		localname := filepath.Join(path, string(e.FileName))
		if backup.Verbose {
			fmt.Println(localname)
		}
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
					UnserializeFromBuffer(backup.Client.ReadBlock(e.ContentBlockID).Data, &chain)
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
	}
	return nil
}

func main() {
	var lockFile *core.LockFile

	/*runtime.SetBlockProfileRate(1000)
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()*/

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

	cmd.Title = "Hashback 0.2.2-go (Hashbox Backup Client)"
	cmd.OptionsFile = filepath.Join(preferencesBaseFolder, ".hashback", "options.json")
	/*	cmd.SaveOptions = func(options map[string]interface{}) error {
		if options["password"] != nil {
			if options["user"] == nil {
				return errors.New("Unable to save login unless both user and password options are specified")
			} else {
				key := GenerateAccessKey(cmd.Option["user"].String(), cmd.Option["password"].String())
				options["accesskey"] = base64.RawURLEncoding.EncodeToString(key[:])
				key = GenerateBackupKey(cmd.Option["user"].String(), cmd.Option["password"].String())
				options["backupkey"] = base64.RawURLEncoding.EncodeToString(key[:])
			}
		}
		return nil
	}*/

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

	var pidName string = ""
	cmd.StringOption("pid", "store", "<filename>", "Create a PID file (lock-file)", &pidName, cmd.Standard)

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
	cmd.Command("list", "<dataset>", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}

		session.Connect()
		defer session.Close()

		list := session.Client.ListDataset(cmd.Args[2])
		if len(list.States) > 0 {
			fmt.Println("Backup id                           Backup date                  Total size    Used space")
			fmt.Println("--------------------------------    -------------------------    ----------    ----------")

			for _, s := range list.States {
				timestamp := binary.BigEndian.Uint64(s.StateID[:])
				date := time.Unix(0, int64(timestamp))

				fmt.Printf("%-32x    %-25s    %10s    %10s\n", s.StateID, date.Format(time.RFC3339), core.HumanSize(s.Size), core.HumanSize(s.UniqueSize))
			}
		} else {
			fmt.Println("Dataset is empty or does not exist")
		}
	})
	cmd.Command("store", "<dataset> (<folder> | <file>)...", func() {
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
	cmd.Command("restore", "<dataset> [backup-id] <folder>", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}
		if len(cmd.Args) < 4 {
			panic(errors.New("Missing destination folder argument"))
		}

		session.Connect()
		defer session.Close()

		list := session.Client.ListDataset(cmd.Args[2])

		var found int = -1
		var path string
		if len(cmd.Args) > 4 {
			path = cmd.Args[4]
			for i, s := range list.States {
				if cmd.Args[3] == fmt.Sprintf("%x", s.StateID) {
					found = i
					break
				}
			}
		} else {
			found = len(list.States) - 1
			if found < 0 {
				panic(errors.New("No backup found under dataset " + cmd.Args[2]))
			}
			path = cmd.Args[3]
		}

		if found < 0 {
			panic(errors.New("Backup id " + cmd.Args[3] + " not found in dataset " + cmd.Args[2]))
		}

		timestamp := binary.BigEndian.Uint64(list.States[found].StateID[:])
		date := time.Unix(0, int64(timestamp))
		fmt.Printf("Restoring %x (%s) to path %s\n", list.States[found].StateID, date.Format(time.RFC3339), path)

		if err := os.MkdirAll(path, 0777); err != nil {
			panic(err)
		}

		if err := session.restoreDir(list.States[found].BlockID, path); err != nil {
			panic(err)
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

	/*



		Store("c:\\projects")

		// create a file tree

		FileRoot, err := GetFileInfo(os.Args[1])
		if err != nil {
			panic(err)
		}
		fmt.Println("Done!")

		_ = FileRoot
		/*
			var filecount = 0
			var foldercount = 0

			filepath.Walk("c:\\projects", func(path string, f os.FileInfo, err error) error {
					if err != nil {
						fmt.Println(err)
						// TODO: Add these to a post-report list instead

					} else {
						if f.Mode()&os.ModeDir > 0 {
							foldercount++
						} else {
							filecount++
						}

						fmt.Printf("\r%d folders and %d files", foldercount, filecount)
					}
					return nil
				})
	*/
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
