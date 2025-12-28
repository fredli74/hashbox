//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package main

import (
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/fredli74/bytearray"
	cmd "github.com/fredli74/cmdparser"
	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/lockfile"

	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"
)

var Version = "(dev-build)"

const PROGRESS_INTERVAL_SECS = 10 * time.Second
const MAX_BLOCK_SIZE int = 8 * 1024 * 1024 // 8MiB max blocksize
const MIN_BLOCK_SIZE int = 64 * 1024       // 64kb minimum blocksize (before splitting it)

var SendingQueueSize int64 = 48 * 1024 * 1024 // Try to keep queue to 48 MiB (can use more for directory blocks and will use up to an additional CPU*MAX_BLOCK_SIZE when compressing)
var SendingQueueThreads int32 = 0

// DefaultIgnoreList populated by init() function from each platform
var DefaultIgnoreList []string

// CustomIgnoreList populated by argument or options.json file
var CustomIgnoreList []string

// TODO: do we need this when we do not follow symlinks?
// const MAX_DEPTH int = 512 // Safety limit to avoid cyclic symbolic links and such

var DEBUG bool = false

func PanicOn(err error) {
	if err != nil {
		panic(err)
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
func SerializeToByteArray(what core.Serializer) bytearray.ByteArray {
	var output bytearray.ByteArray
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
	// TODO: Add Platform specific data, hidden files, ACL file permissions etc.
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
	size += core.WriteUint32(w, uint32(0x66656E74)) // "fent"
	size += e.FileName.Serialize(w)
	size += core.WriteInt64(w, e.FileSize)
	size += core.WriteUint32(w, e.FileMode)
	size += core.WriteInt64(w, e.ModTime)
	size += e.ReferenceID.Serialize(w)

	size += core.WriteUint8(w, e.ContentType)
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
	size += core.ReadUint32(r, &check)
	if check != 0x66656E74 { // "fent"
		panic(errors.New("Corrupted FileEntry"))
	}
	size += e.FileName.Unserialize(r)
	size += core.ReadInt64(r, &e.FileSize)
	size += core.ReadUint32(r, &e.FileMode)
	size += core.ReadInt64(r, &e.ModTime)
	size += e.ReferenceID.Unserialize(r)

	size += core.ReadUint8(r, &e.ContentType)
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
	size += core.WriteUint32(w, uint32(0x6663686E)) // "fchn"
	size += core.WriteUint32(w, uint32(len(b.ChainBlocks)))
	for i := range b.ChainBlocks {
		size += b.ChainBlocks[i].Serialize(w)
		size += b.DecryptKeys[i].Serialize(w)
	}
	return
}
func (b *FileChainBlock) Unserialize(r io.Reader) (size int) {
	var check uint32
	size += core.ReadUint32(r, &check)
	if check != 0x6663686E { // "fchn"
		panic(errors.New("Corrupted FileChainBlock"))
	}
	var l uint32
	size += core.ReadUint32(r, &l)
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
	size += core.WriteUint32(w, uint32(0x64626C6B)) // "dblk"
	size += core.WriteUint32(w, uint32(len(b.File)))
	for _, f := range b.File {
		size += f.Serialize(w)
	}
	return
}
func (b *DirectoryBlock) Unserialize(r io.Reader) (size int) {
	var check uint32
	size += core.ReadUint32(r, &check)
	if check != 0x64626C6B { // "dblk"
		panic(errors.New("Corrupted DirectoryBlock"))
	}
	var l uint32
	size += core.ReadUint32(r, &l)
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

	DifferentFiles int

	ignoreList []ignoreEntry
	reference  *referenceEngine
}

func NewBackupSession() *BackupSession {
	return &BackupSession{}
}

func (session *BackupSession) Connect() *core.Client {
	if session.AccessKey == nil || session.BackupKey == nil {
		panic(errors.New("Missing -password option"))
	}

	// Resetting statistics
	session.Start = time.Now()
	session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	session.ReadData = 0
	session.WriteData = 0
	session.Directories = 0
	session.Files = 0
	session.UnchangedFiles = 0
	session.DifferentFiles = 0

	// fmt.Printf("%x\n", *session.AccessKey)
	// fmt.Printf("%x\n", *session.BackupKey)

	core.Log(core.LogTrace, "Connecting to %s", session.ServerString)
	client := core.NewClient(session.ServerString, session.User, *session.AccessKey)
	client.QueueMax = SendingQueueSize
	if SendingQueueThreads > 0 {
		client.ThreadMax = SendingQueueThreads
	}
	client.EnablePaint = session.Paint
	session.Client = client
	return session.Client
}
func (session *BackupSession) Close(polite bool) {
	if session.reference != nil {
		session.reference.Close()
		session.reference = nil
	}
	if session.Client != nil {
		session.Client.Close(polite)
		session.Client = nil
	}

	core.Log(core.LogTrace, "Disconnected from %s", session.ServerString)
}

func (session *BackupSession) Log(format string, a ...interface{}) {
	if session.Client != nil {
		session.Client.Paint("\n")
	}
	core.Log(core.LogInfo, format, a...)
	//	fmt.Printf(format, a...)
	//	fmt.Println()
}
func (session *BackupSession) LogVerbose(format string, a ...interface{}) {
	if session.Verbose {
		if session.Client != nil {
			session.Client.Paint("\n")
		}
		core.Log(core.LogInfo, format, a...)
		//		fmt.Printf(format, a...)
		//		fmt.Println()
	}
}

func (session *BackupSession) findPathMatch(rootBlockID core.Byte128, path string) ([]*FileEntry, error) {
	//var fileList []*FileEntry

	pathList := core.SplitPath(path)
	dir, unmatched, err := session.changeDir(rootBlockID, pathList)
	if unmatched == 1 {
		filtered := dir.File[:0]
		for _, x := range dir.File {
			if match, _ := filepath.Match(pathList[len(pathList)-1], string(x.FileName)); match {
				filtered = append(filtered, x)
			}
		}
		if len(filtered) > 0 {
			return filtered, nil
		}
		return nil, fmt.Errorf("No match found on \"%s\"", path)

	} else if unmatched > 0 {
		return nil, err
	} else {
		return dir.File, nil
	}
}

var LocalStoragePath string

func main() {
	bytearray.EnableAutoGC(60, 74)

	var lockFile *lockfile.LockFile

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			if lockFile != nil {
				lockFile.Unlock()
			}
			panic(r)
			// os.Exit(1)
		}
	}()

	// Figure out where to load/save options
	{
		LocalStoragePath = filepath.Join(".", ".hashback")
		home, err := filepath.Abs(userHomeFolder())
		if err == nil {
			s, err := os.Stat(home)
			if err == nil && s.IsDir() {
				LocalStoragePath = filepath.Join(home, ".hashback")
			}
		}
		err = os.MkdirAll(LocalStoragePath, 0700)
		PanicOn(err)
	}

	session := NewBackupSession()

	cmd.Title = fmt.Sprintf("Hashback %s (Hashbox Backup Client)", Version)

	cmd.OptionsFile = filepath.Join(LocalStoragePath, "options.json")

	cmd.BoolOption("debug", "", "Debug output", &DEBUG, cmd.Hidden).OnChange(func() {
		if DEBUG {
			runtime.SetBlockProfileRate(1000)
			go func() {
				core.Log(core.LogInfo, "%v", http.ListenAndServe(":6060", nil))
			}()
			core.LogLevel = core.LogTrace
		}
	})

	var queueSizeMB int64
	cmd.IntOption("queuesize", "", "<MiB>", "Change sending queue size", &queueSizeMB, cmd.Hidden|cmd.Preference).OnChange(func() {
		SendingQueueSize = queueSizeMB * 1024 * 1024
	})
	var threadCount int64
	cmd.IntOption("threads", "", "<num>", "Change sending queue max threads", &threadCount, cmd.Hidden|cmd.Preference).OnChange(func() {
		SendingQueueThreads = int32(threadCount)
	})

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
	cmd.BoolOption("verbose", "", "Show verbose output", &session.Verbose, cmd.Preference)
	cmd.BoolOption("progress", "", "Show progress during store", &session.ShowProgress, cmd.Preference)
	cmd.BoolOption("paint", "", "Paint!", &session.Paint, cmd.Preference).OnChange(func() {
		if session.Paint {
			session.Verbose = false
			session.ShowProgress = false
		}
	})

	cmd.Command("info", "", func() {
		session.Log(cmd.Title)

		session.Connect()
		defer session.Close(true)

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

		// fmt.Println("* TODO: Add quota and total size info")
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

		session.Log(cmd.Title)

		session.Connect()
		defer session.Close(true)

		list := session.Client.ListDataset(cmd.Args[2])
		if len(list.States) > 0 {

			if len(cmd.Args) < 4 {

				fmt.Println("Backup id                           Backup start                 Total size     Diff prev")
				fmt.Println("--------------------------------    -------------------------    ----------    ----------")

				for _, e := range list.States {
					timestamp := binary.BigEndian.Uint64(e.State.StateID[:])
					date := time.Unix(0, int64(timestamp))

					if e.StateFlags&core.StateFlagInvalid == core.StateFlagInvalid {
						fmt.Printf("%-32x    %-25s    !!! INVALID DATASET\n", e.State.StateID, date.Format(time.RFC3339))
					} else {
						fmt.Printf("%-32x    %-25s    %10s    %10s\n", e.State.StateID, date.Format(time.RFC3339), core.HumanSize(e.State.Size), core.HumanSize(e.State.UniqueSize))
					}
				}
			} else {
				var state *core.DatasetState
				for i, e := range list.States {
					if e.StateFlags&core.StateFlagInvalid != core.StateFlagInvalid && cmd.Args[3] == "." || fmt.Sprintf("%x", e.State.StateID[:]) == cmd.Args[3] {
						state = &list.States[i].State
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
				var err error
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
	var retainWeeks int64 = 0
	var retainDays int64 = 0
	var intervalBackup int64 = 0
	cmd.StringOption("pid", "store", "<filename>", "Create a PID file (lock-file)", &pidName, cmd.Standard)
	cmd.StringListOption("ignore", "store", "<pattern>", "Ignore files matching pattern", &CustomIgnoreList, cmd.Standard|cmd.Preference)
	cmd.IntOption("interval", "store", "<minutes>", "Keep running backups every <minutes> until interrupted", &intervalBackup, cmd.Standard)
	cmd.IntOption("retaindays", "store", "<days>", "Remove backups older than 24h but keep one per day for <days>, 0 = keep all daily", &retainDays, cmd.Standard|cmd.Preference)
	cmd.IntOption("retainweeks", "store", "<weeks>", "Remove backups older than 24h but keep one per week for <weeks>, 0 = keep all weekly", &retainWeeks, cmd.Standard|cmd.Preference)
	cmd.Command("store", "<dataset> (<folder> | <file>)...", func() {
		ignoreList := append(DefaultIgnoreList, CustomIgnoreList...)
		ignoreList = append(ignoreList, filepath.Join(LocalStoragePath, "*.cache*"))

		for _, d := range ignoreList {
			ignore := ignoreEntry{pattern: d, match: core.ExpandEnv(d)} // Expand ignore patterns

			if ignore.match == "" {
				continue
			}
			if _, err := filepath.Match(ignore.match, "ignore"); err != nil {
				panic(fmt.Errorf("Invalid ignore pattern %s", ignore.pattern))
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

		session.Log(cmd.Title)

		if pidName != "" {
			var err error
			if lockFile, err = lockfile.Lock(pidName); err != nil {
				fmt.Println(err)
				os.Exit(0)
			} else {
				defer lockFile.Unlock()
			}
		}

		var latestBackup uint64
		for latestBackup == 0 || intervalBackup > 0 { // Keep looping if interval backupping

			func() {
				session.Connect()
				defer session.Close(true)
				if latestBackup > 0 || intervalBackup == 0 {
					session.State = &core.DatasetState{StateID: session.Client.SessionNonce}
					session.Store(cmd.Args[2], cmd.Args[3:]...)
					if retainWeeks > 0 || retainDays > 0 {
						session.Retention(cmd.Args[2], int(retainDays), int(retainWeeks))
					}
					latestBackup = binary.BigEndian.Uint64(session.State.StateID[:])
					date := time.Unix(0, int64(latestBackup))
					fmt.Printf("Backup %s %x (%s) completed\n", cmd.Args[2], session.State.StateID[:], date.Format(time.RFC3339))
				} else {
					list := session.Client.ListDataset(cmd.Args[2])
					for i := len(list.States) - 1; i >= 0; i-- {
						if list.States[i].StateFlags&core.StateFlagInvalid != core.StateFlagInvalid {
							latestBackup = binary.BigEndian.Uint64(list.States[i].State.StateID[:])
							break
						}
					}
				}
			}()
			if intervalBackup > 0 {
				date := time.Unix(0, int64(latestBackup)).Add(time.Duration(intervalBackup) * time.Minute)
				if date.After(time.Now()) {
					fmt.Printf("Next backup scheduled for %s\n", date.Format(time.RFC3339))
					// fmt.Println(time.Since(date))
					for date.After(time.Now()) {
						time.Sleep(10 * time.Second)
					}
				} else {
					latestBackup = 1 // trigger a new backup already
				}
			}
		}
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

		session.Log(cmd.Title)

		session.Connect()
		defer session.Close(true)

		list := session.Client.ListDataset(cmd.Args[2])

		var stateid string = cmd.Args[3]
		var restorepath string = cmd.Args[len(cmd.Args)-1]
		var restorelist []string = cmd.Args[4 : len(cmd.Args)-1]

		var found int = -1
		if stateid == "." {
			found = len(list.States) - 1
		} else {
			for i, e := range list.States {
				if stateid == fmt.Sprintf("%x", e.State.StateID) {
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

		timestamp := binary.BigEndian.Uint64(list.States[found].State.StateID[:])
		date := time.Unix(0, int64(timestamp))
		fmt.Printf("Restoring from %x (%s) to path %s\n", list.States[found].State.StateID, date.Format(time.RFC3339), restorepath)

		session.Restore(list.States[found].State.BlockID, restorepath, restorelist...)
	})
	cmd.Command("diff", "<dataset> (<backup id>|.) [\"<path>\"...] <local-folder>", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}
		if len(cmd.Args) < 4 {
			panic(errors.New("Missing backup id (or \".\")"))
		}
		if len(cmd.Args) < 5 {
			panic(errors.New("Missing destination folder argument"))
		}

		session.Log(cmd.Title)

		session.Connect()
		defer session.Close(true)

		list := session.Client.ListDataset(cmd.Args[2])

		var stateid string = cmd.Args[3]
		var restorepath string = cmd.Args[len(cmd.Args)-1]
		var restorelist []string = cmd.Args[4 : len(cmd.Args)-1]

		var found int = -1
		if stateid == "." {
			found = len(list.States) - 1
		} else {
			for i, e := range list.States {
				if stateid == fmt.Sprintf("%x", e.State.StateID) {
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

		timestamp := binary.BigEndian.Uint64(list.States[found].State.StateID[:])
		date := time.Unix(0, int64(timestamp))
		fmt.Printf("Comparing %x (%s) to path %s\n", list.States[found].State.StateID, date.Format(time.RFC3339), restorepath)

		session.DiffRestore(list.States[found].State.BlockID, restorepath, restorelist...)
		if session.DifferentFiles > 0 {
			os.Exit(2)
		}
	})
	cmd.Command("remove", "<dataset> <backup id>", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}
		if len(cmd.Args) < 4 {
			panic(errors.New("Missing backup id"))
		}

		session.Log(cmd.Title)

		session.Connect()
		defer session.Close(true)

		list := session.Client.ListDataset(cmd.Args[2])

		var stateid string = cmd.Args[3]
		var found int = -1
		for i, e := range list.States {
			if stateid == fmt.Sprintf("%x", e.State.StateID) {
				found = i
				break
			}
		}
		if found < 0 {
			panic(errors.New("Backup id " + cmd.Args[3] + " not found in dataset " + cmd.Args[2]))
		}

		fmt.Printf("Removing backup %x from %s\n", list.States[found].State.StateID, cmd.Args[2])
		session.Client.RemoveDatasetState(cmd.Args[2], list.States[found].State.StateID)
	})

	signalchan := make(chan os.Signal, 1)
	defer close(signalchan)
	signal.Notify(signalchan, os.Interrupt)
	signal.Notify(signalchan, os.Kill)
	go func() {
		for range signalchan {
			if session != nil {
				session.Close(false)
			}
			if lockFile != nil {
				lockFile.Unlock()
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
