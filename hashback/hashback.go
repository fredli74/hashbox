//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	cmd "bitbucket.org/fredli74/cmdparser"
	"bitbucket.org/fredli74/hashbox/core"

	"github.com/smtc/rollsum"

	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"time"

	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"os/user"
	"path/filepath"
)

const PROGRESS_INTERVAL_SECS = 10 * time.Second
const MAX_BLOCK_SIZE int = 512 * 1024
const SYNC_SIZE int = 1024
const MAX_DEPTH int = 512 // Safety limit to avoid cyclic symbolic links and such

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

type FileEntry struct {
	FileName       core.String  // serialize as uint32(len) + []byte
	FileSize       uint64       // (!dir)
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
	Client   *core.Client
	State    *core.DatasetState
	Start    time.Time
	Progress time.Time

	ReadData       int64
	WriteData      int64
	Directories    int
	Files          int
	UnchangedFiles int

	Verbose      bool
	ShowProgress bool

	refQueue []referenceEntry // Used to compare against a reference backup during backup
}

func (backup *BackupSession) Log(v ...interface{}) {
	if backup.Verbose {
		fmt.Println(v...)
	}
}
func (backup *BackupSession) PrintStoreProgress() {
	var compression float64
	if backup.ReadData > 0 {
		compression = 100.0 * (float64(backup.ReadData) - float64(backup.WriteData)) / float64(backup.ReadData)
	}
	sent, skipped, queued := backup.Client.GetStats()
	fmt.Printf("*** %.1f min, read: %s (%.0f%% compr), %d folders, %d/%d files changed, blcks (%dq) %d:%d\n",
		time.Since(backup.Start).Minutes(), humanSize(uint64(backup.ReadData)), compression, backup.Directories, backup.Files-backup.UnchangedFiles, backup.Files,
		queued, sent, skipped)

}
func (backup *BackupSession) PrintRestoreProgress() {
	var compression float64
	if backup.WriteData > 0 {
		compression = 100.0 * (float64(backup.WriteData) - float64(backup.ReadData)) / float64(backup.WriteData)
	}
	fmt.Printf("*** %.1f min, read: %s (%.0f%% compr), write: %s, %d folders, %d files\n",
		time.Since(backup.Start).Minutes(), humanSize(uint64(backup.ReadData)), compression, humanSize(uint64(backup.WriteData)), backup.Directories, backup.Files)
}

func (backup *BackupSession) storeFile(path string, entry *FileEntry) error {
	var links []core.Byte128

	chain := FileChainBlock{}

	fil, err := os.OpenFile(path, os.O_RDONLY, 444)
	if err != nil {
		return err
	}
	defer fil.Close()

	for offset := int64(0); offset < int64(entry.FileSize); {
		if backup.ShowProgress && time.Now().After(backup.Progress) {
			backup.PrintStoreProgress()
			backup.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
		}
		// TODO: print progress

		var left int64 = int64(entry.FileSize) - offset
		var maxBlockSize int = MAX_BLOCK_SIZE
		if left < int64(maxBlockSize) {
			maxBlockSize = int(left)
		}

		rawdata := make([]byte, maxBlockSize)
		{
			rawlength, err := fil.ReadAt(rawdata, offset)
			if err != nil {
				panic(err)
			}

			var sum rollsum.Rollsum
			sum.Init()
			var maxd = uint32(0)
			var maxi = rawlength
			for i := 0; i < rawlength; {
				if i >= SYNC_SIZE {
					sum.Rollout(rawdata[i-SYNC_SIZE])
				}
				sum.Rollin(rawdata[i])
				i++
				if i >= SYNC_SIZE {
					d := sum.Digest()
					if d >= maxd {
						maxd = d
						maxi = i
					}
				}
			}
			rawlength = maxi
			rawdata = rawdata[:rawlength]
			offset += int64(rawlength)

			backup.ReadData += int64(rawlength)
		}
		// TODO: compress data
		var zdata []byte
		{
			buf := bytes.NewBuffer(nil)
			zw, err := zlib.NewWriterLevel(buf, zlib.BestCompression)
			if err != nil {
				panic(err)
			}
			zw.Write(rawdata)
			zw.Close()
			zdata = buf.Bytes()
			backup.WriteData += int64(len(zdata))
		}
		backup.State.UniqueSize += uint64(len(zdata))

		// TODO: encrypt data with deterministic encryption

		var datakey core.Byte128

		id := backup.Client.StoreBlock(zdata, nil)
		links = append(links, id)
		chain.ChainBlocks = append(chain.ChainBlocks, id)
		chain.DecryptKeys = append(chain.DecryptKeys, datakey)
	}

	if len(chain.ChainBlocks) > 1 {
		id := backup.Client.StoreBlock(SerializeToBuffer(chain), links)
		entry.ContentType = ContentTypeFileChain
		entry.ContentBlockID = id

	} else {
		entry.ContentType = ContentTypeFileData
		entry.ContentBlockID = chain.ChainBlocks[0]
		entry.DecryptKey = chain.DecryptKeys[0]
	}

	return nil
}
func (backup *BackupSession) storeDir(path string, entry *FileEntry) error {
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
		e, err := backup.storePath(filepath.Join(path, info.Name()), false)
		if err != nil {
			SoftError(err)
		} else if e != nil {
			dir.File = append(dir.File, e)
			if e.HasContentBlockID() {
				links = append(links, e.ContentBlockID)
			}
		}
	}

	id := backup.Client.StoreBlock(SerializeToBuffer(dir), links)
	entry.ContentType = ContentTypeDirectory
	entry.ContentBlockID = id

	return nil
}

func (backup *BackupSession) storePath(path string, toplevel bool) (*FileEntry, error) {
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
		FileSize:    uint64(info.Size()),
		FileMode:    uint32(info.Mode()),
		ModTime:     info.ModTime().UnixNano(),
		ReferenceID: backup.State.StateID,
	}

	if entry.FileMode&uint32(os.ModeTemporary) > 0 {
		backup.Log("Skipping temporary file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeDevice) > 0 {
		backup.Log("Skipping device file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeNamedPipe) > 0 {
		backup.Log("Skipping named pipe file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSocket) > 0 {
		backup.Log("Skipping socket file", path)
		return nil, nil
	} else if entry.FileMode&uint32(os.ModeSymlink) > 0 {
		entry.ContentType = ContentTypeSymLink
		sym, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		entry.FileLink = core.String(sym)
		_ = "breakpoint"
		backup.findAndReuseReference(path, &entry)
	} else {
		backup.Log(path)
		if entry.FileMode&uint32(os.ModeDir) > 0 {
			if err := backup.storeDir(path, &entry); err != nil {
				return nil, err
			}
			backup.Directories++
		} else {
			same := backup.findAndReuseReference(path, &entry)

			if !same && entry.FileSize > 0 {
				if err := backup.storeFile(path, &entry); err != nil {
					return nil, err
				}
			} else {
				if backup.Client.Paint {
					fmt.Print(" ")
				}
				backup.UnchangedFiles++
			}
			backup.Files++
			backup.State.Size += entry.FileSize
		}
	}
	return &entry, nil
}

func (backup *BackupSession) popReference() *FileEntry {
	n := backup.refQueue[0]
	backup.refQueue = backup.refQueue[1:]
	if n.File.ContentType == ContentTypeDirectory {
		backup.pushReference(n.Path, n.File.ContentBlockID)
	}
	return n.File
}
func (backup *BackupSession) pushReference(path string, referenceBlockID core.Byte128) {
	var refdir DirectoryBlock
	UnserializeFromBuffer(backup.Client.ReadBlock(referenceBlockID).Data, &refdir)
	var list []referenceEntry
	for _, r := range refdir.File {
		list = append(list, referenceEntry{filepath.Join(path, string(r.FileName)), r})
	}
	backup.refQueue = append(list, backup.refQueue...)
}
func (backup *BackupSession) findAndReuseReference(path string, entry *FileEntry) bool {
	_ = "breakpoint"
	for len(backup.refQueue) > 0 {
		if backup.refQueue[0].Path < path {
			backup.popReference()
		} else if backup.refQueue[0].Path == path {
			master := backup.popReference()
			if master.FileName == entry.FileName && master.FileSize == entry.FileSize && master.FileMode == entry.FileMode && master.ModTime == entry.ModTime && master.FileLink == entry.FileLink {
				// It's the same!
				*entry = *master
				return true
			}
			break
		} else {
			break
		}
	}
	return false
}

func Store(c *core.Client, datasetName string, path ...string) {

	backup := BackupSession{Client: c, Start: time.Now(), State: &core.DatasetState{StateID: c.SessionNonce}}
	c.Paint = cmd.Option["paint"].Value.(bool)
	backup.ShowProgress = !c.Paint && cmd.Option["progress"].Value.(bool)
	backup.Verbose = !c.Paint && cmd.Option["v"].Value.(bool)

	var referenceBlockID *core.Byte128
	if !cmd.Option["full"].Value.(bool) {
		list := c.ListDataset(cmd.Args[2])
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
		_ = "breakpoint"
		var links []core.Byte128

		var refdir DirectoryBlock
		if referenceBlockID != nil {
			UnserializeFromBuffer(backup.Client.ReadBlock(*referenceBlockID).Data, &refdir)
			for _, s := range path {
				name := filepath.Base(s)
				for _, r := range refdir.File {
					if string(r.FileName) == name {
						backup.refQueue = append(backup.refQueue, referenceEntry{s, r})
					}
				}
			}
		}

		dir := DirectoryBlock{}
		for _, s := range path {
			e, err := backup.storePath(s, true)
			if err != nil {
				panic(err)
			} else if e != nil {
				dir.File = append(dir.File, e)
				if e.ContentType != ContentTypeEmpty {
					links = append(links, e.ContentBlockID)
				}
			}
		}
		backup.State.BlockID = c.StoreBlock(SerializeToBuffer(dir), links)
	} else {
		if referenceBlockID != nil {
			// push the last backup root to reference list
			backup.pushReference(path[0], *referenceBlockID)
		}
		e, err := backup.storePath(path[0], true)
		if err != nil {
			panic(err)
		}
		backup.State.BlockID = e.ContentBlockID
	}

	// Commit all pending writes
	c.Commit()
	c.AddDatasetState(datasetName, *backup.State)

	fmt.Println()
	backup.PrintStoreProgress()
}

func (backup *BackupSession) restoreFileData(f *os.File, blockID core.Byte128, DecryptKey core.Byte128) error {
	if backup.ShowProgress && time.Now().After(backup.Progress) {
		backup.PrintRestoreProgress()
		backup.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	}

	block := backup.Client.ReadBlock(blockID)
	backup.ReadData += int64(len(block.Data))
	// TODO: Decrypt block
	// Uncompress block

	buf := bytes.NewReader(block.Data)
	zr, err := zlib.NewReader(buf)
	if err != nil {
		return err
	}
	defer zr.Close()
	written, err := io.Copy(f, zr)
	if err != nil {
		return err
	}

	backup.WriteData += int64(written)
	return nil
}

func (backup *BackupSession) restoreDir(blockID core.Byte128, path string) error {
	_ = "breakpoint"
	var dir DirectoryBlock
	UnserializeFromBuffer(backup.Client.ReadBlock(blockID).Data, &dir)
	for _, e := range dir.File {
		localname := filepath.Join(path, string(e.FileName))
		if backup.Verbose {
			fmt.Println(localname)
		}
		if e.FileMode&uint32(os.ModeDir) > 0 {
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
				panic(errors.New("Invalid ContentType for a directory"))
			}
			backup.Directories++
		} else if e.FileMode&uint32(os.ModeSymlink) > 0 {
			switch e.ContentType {
			case ContentTypeSymLink:
				if err := os.Symlink(string(e.FileLink), localname); err != nil {
					return err
				}
			default:
				panic(errors.New("Invalid ContentType for a Symlink"))
			}
		} else {
			if e.ContentType != ContentTypeFileChain && e.ContentType != ContentTypeFileData && e.ContentType != ContentTypeEmpty {
				panic(errors.New("Invalid ContentType for a file"))
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

var humanUnitName []string

func init() {
	humanUnitName = []string{"B", "KiB", "MiB", "TiB", "PiB", "EiB", "really?"}
}

func humanSize(size uint64) string {
	floatSize := float64(size)
	unit := 0
	for ; floatSize > 1000; floatSize /= 1024 {
		unit++
	}
	precision := 0
	if unit > 0 && floatSize < 10 {
		precision = 2
	} else if unit > 0 && floatSize < 100 {
		precision = 1
	}
	return fmt.Sprintf("%.*f %s", precision, floatSize, humanUnitName[unit])
}

func Connect() *core.Client {
	if !cmd.Option["user"].IsSet() {
		panic(errors.New("Missing -user option"))
	}
	if !cmd.Option["server"].IsSet() {
		panic(errors.New("Missing -server option"))
	}
	var accesskey, backupkey core.Byte128
	if cmd.Option["password"].IsSet() {
		accesskey = GenerateAccessKey(cmd.Option["user"].String(), cmd.Option["password"].String())
		backupkey = GenerateBackupKey(cmd.Option["user"].String(), cmd.Option["password"].String())
	} else if cmd.Option["accesskey"].IsSet() && cmd.Option["backupkey"].IsSet() {
		key, err := base64.RawURLEncoding.DecodeString(cmd.Option["accesskey"].String())
		if err != nil {
			panic(err)
		}
		copy(accesskey[:], key[:16])
		key, err = base64.RawURLEncoding.DecodeString(cmd.Option["backupkey"].String())
		if err != nil {
			panic(err)
		}
		copy(backupkey[:], key[:16])
	} else {
		panic(errors.New("Missing -password option"))
	}
	//fmt.Printf("%x\n", accesskey)
	//fmt.Printf("%x\n", backupkey)

	fmt.Println("Connecting to", cmd.Option["server"].String())
	conn, err := net.Dial("tcp", cmd.Option["server"].String())
	if err != nil {
		panic(err)
	}

	client := core.NewClient(conn, cmd.Option["user"].String(), accesskey)
	return client
}

func main() {
	defer func() {
		// Panic error handling
		if r := recover(); r != nil {
			fmt.Println(r)
			os.Exit(1)
		}
	}()

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

	cmd.Title = "Hashback (Hashbox Backup Client)"
	cmd.OptionsFile = filepath.Join(preferencesBaseFolder, ".hashback", "options.json")
	cmd.SaveOptions = func(options map[string]interface{}) error {
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
	}

	cmd.AddOption("user", "", "Username", "", cmd.Preference|cmd.Required)
	cmd.AddOption("password", "", "Password", "", cmd.Preference)
	cmd.AddOption("server", "", "Hashbox server address (<ip>:<port>)", "", cmd.Preference|cmd.Required)
	cmd.AddOption("accesskey", "", "Hashbox server accesskey", nil, cmd.Preference|cmd.Hidden)
	cmd.AddOption("backupkey", "", "Hashbox server backupkey", nil, cmd.Preference|cmd.Hidden)
	cmd.AddOption("full", "", "Force a non-incremental store", false, cmd.Preference)
	cmd.AddOption("v", "", "Show verbose output", false, cmd.Preference)
	cmd.AddOption("progress", "", "Show progress during store", false, cmd.Preference)
	cmd.AddOption("paint", "", "Paint!", false, cmd.Preference)

	cmd.AddCommand("info", "", func() {
		client := Connect()
		defer client.Close()

		info := client.GetAccountInfo()
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
				fmt.Printf("%8s    %s\n", humanSize(d.Size), d.Name)
			}
		}

	})
	cmd.AddCommand("list", "<dataset>", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}

		client := Connect()
		defer client.Close()

		list := client.ListDataset(cmd.Args[2])
		if len(list.States) > 0 {
			fmt.Println("Backup id                           Backup date                  Total size    Used space")
			fmt.Println("--------------------------------    -------------------------    ----------    ----------")

			for _, s := range list.States {
				timestamp := binary.BigEndian.Uint64(s.StateID[:])
				date := time.Unix(0, int64(timestamp))

				fmt.Printf("%-32x    %-25s    %10s    %10s\n", s.StateID, date.Format(time.RFC3339), humanSize(s.Size), humanSize(s.UniqueSize))
			}
		} else {
			fmt.Println("Dataset is empty or does not exist")
		}
	})
	cmd.AddCommand("store", "<dataset> (<folder> | <file>)...", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}
		if len(cmd.Args) < 4 {
			panic(errors.New("Missing source file or folder argument"))
		}

		client := Connect()
		defer client.Close()

		Store(client, cmd.Args[2], cmd.Args[3:]...)
	})
	cmd.AddCommand("restore", "<dataset> [backup-id] <folder>", func() {
		if len(cmd.Args) < 3 {
			panic(errors.New("Missing dataset argument"))
		}
		if len(cmd.Args) < 4 {
			panic(errors.New("Missing destination folder argument"))
		}

		client := Connect()
		defer client.Close()

		list := client.ListDataset(cmd.Args[2])

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
			path = cmd.Args[3]
		}

		if found < 0 {
			panic(errors.New("Backup id " + cmd.Args[3] + " not found in dataset " + cmd.Args[2]))
		}

		timestamp := binary.BigEndian.Uint64(list.States[found].StateID[:])
		date := time.Unix(0, int64(timestamp))
		fmt.Printf("Restoring %x (%s) to path %s\n", list.States[found].StateID, date.Format(time.RFC3339), path)

		backup := BackupSession{Client: client, Start: time.Now(), State: &list.States[found]}
		backup.ShowProgress = cmd.Option["progress"].Value.(bool)
		backup.Verbose = cmd.Option["v"].Value.(bool)

		if err := os.MkdirAll(path, 0777); err != nil {
			panic(err)
		}

		if err := backup.restoreDir(list.States[found].BlockID, path); err != nil {
			panic(err)
		}
		backup.PrintRestoreProgress()
	})

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
