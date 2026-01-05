//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package storagedb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fredli74/hashbox/pkg/core"
)

//***********************************************************************//
//                       Storage database handling                       //
//***********************************************************************//

type _storageFileTypeInfo struct {
	Type       uint32
	Extension  string
	BufferSize int
}

const (
	StorageFileTypeIndex = iota
	StorageFileTypeMeta
	StorageFileTypeData
)

const (
	storageFileTypeIndex = StorageFileTypeIndex
	storageFileTypeMeta  = StorageFileTypeMeta
	storageFileTypeData  = StorageFileTypeData
)

var storageFileTypeInfo []_storageFileTypeInfo = []_storageFileTypeInfo{ // datamarker, extension, buffersize
	_storageFileTypeInfo{0x48534958, ".idx", 2048},  // "HSIX" Hashbox Storage Index
	_storageFileTypeInfo{0x48534D44, ".meta", 1024}, // "HSMD" Hashbox Storage Meta
	_storageFileTypeInfo{0x48534442, ".dat", 3072},  // "HSDB" Hashbox Storage Data
}

const (
	storageVersion             uint32 = 1
	storageFileDeadspaceOffset int64  = 8  // 8 bytes (filetype + version)
	storageFileHeaderSize      int64  = 16 // 16 bytes (filetype + version + deadspace)

	storageDataMarker  uint32 = 0x68626C6B    // "hblk"
	storageOffsetLimit int64  = (1 << 34) - 1 // 0x3ffffffff ~= 16 GiB data and metadata files
)

const ( // 2 bytes, max 16 flags
	entryFlagExists  = 1 << iota // index entry exists
	entryFlagNoLinks             // index entry has no links
	entryFlagMarked              // sweep marker
	entryFlagInvalid             // entry is invalid
)

type storageFileHeader struct {
	filetype  uint32
	version   uint32
	deadspace int64
}

func (h *storageFileHeader) Serialize(w io.Writer) (size int) {
	size += core.WriteUint32(w, h.filetype)
	size += core.WriteUint32(w, h.version)
	size += core.WriteInt64(w, h.deadspace)
	return
}
func (h *storageFileHeader) Unserialize(r io.Reader) (size int) {
	size += core.ReadUint32(r, &h.filetype)
	size += core.ReadUint32(r, &h.version)
	if h.version != storageVersion {
		core.Abort("Invalid version in dbFileHeader")
	}
	size += core.ReadInt64(r, &h.deadspace)
	return
}

// sixByteLocation uses 6 bytes (48 bit) to reference a file number and file offset
// 14 bit for filenumber = 16384 files   (int64(x) >> 34)
// 34 bit for filesize = 16 GiB files    (int64(x) & 0x3ffffffff)
// total addressable storage = 256TiB
type sixByteLocation [6]byte

func (b *sixByteLocation) Set(File int32, Offset int64) {
	core.ASSERT(File >= 0 && File <= 0x3fff, File)
	core.ASSERT(Offset >= 0 && Offset <= 0x3ffffffff, Offset)

	var l int64 = int64(File)<<34 | (Offset & 0x3ffffffff)
	b[0] = byte(l >> 40)
	b[1] = byte(l >> 32)
	b[2] = byte(l >> 24)
	b[3] = byte(l >> 16)
	b[4] = byte(l >> 8)
	b[5] = byte(l)
}
func (b sixByteLocation) Get() (File int32, Offset int64) {
	var l int64 = int64(b[5]) | (int64(b[4]) << 8) | (int64(b[3]) << 16) | (int64(b[2]) << 24) | (int64(b[1]) << 32) | (int64(b[0]) << 40)
	return int32(l >> 34), (l & 0x3ffffffff)
}
func (b sixByteLocation) Serialize(w io.Writer) (size int) {
	return core.WriteBytes(w, b[:])
}
func (b *sixByteLocation) Unserialize(r io.Reader) (size int) {
	return core.ReadBytes(r, b[:])
}

//***********************************************************************//
//                         storage file handling                         //
//***********************************************************************//

func (store *Store) getNumberedName(fileType int, fileNumber int32) string {
	return fmt.Sprintf("%.8X%s", fileNumber, storageFileTypeInfo[fileType].Extension)
}
func (store *Store) getNumberedFileName(fileType int, fileNumber int32) string {
	var path string
	switch fileType {
	case storageFileTypeData:
		path = store.DataDir
	case storageFileTypeIndex:
		path = store.IndexDir
	case storageFileTypeMeta:
		path = store.IndexDir
	}
	return filepath.Join(path, store.getNumberedName(fileType, fileNumber))
}
func (store *Store) getNumberedFile(fileType int, fileNumber int32, create bool) *core.BufferedFile {
	name := store.getNumberedName(fileType, fileNumber)
	if store.filepool[name] == nil {
		filename := store.getNumberedFileName(fileType, fileNumber)

		flag := 0
		if create {
			flag |= os.O_CREATE
		}

		f, err := core.OpenBufferedFile(filename, storageFileTypeInfo[fileType].BufferSize, flag, 0666)
		if err != nil {
			if create {
				core.Abort("%v", err)
			} else {
				return nil
			}
		}
		var header storageFileHeader
		if f.Size() == 0 { // New file, write a header
			header.filetype = storageFileTypeInfo[fileType].Type
			header.version = storageVersion
			header.Serialize(f.Writer)
			store.filedeadspace[name] = 0
		} else {
			header.Unserialize(f.Reader)
			if header.filetype != storageFileTypeInfo[fileType].Type {
				core.Abort("Trying to read storage file %s with the wrong file type header: %x (was expecting %x)", filename, header.filetype, storageFileTypeInfo[fileType].Type)
			}
			store.filedeadspace[name] = header.deadspace
		}
		core.Log(core.LogInfo, "Opening file: %s", filename)
		store.filepool[name] = f
	}
	return store.filepool[name]
}

// getNumberedFileSize is used to read the dead space value from a meta or data file header
func (store *Store) getNumberedFileSize(fileType int, fileNumber int32) (size int64, deadspace int64, err error) {
	file := store.getNumberedFile(fileType, fileNumber, false)
	if file == nil {
		return 0, 0, fmt.Errorf("Trying to read free space from %.8X%s which does not exist", fileNumber, storageFileTypeInfo[fileType].Extension)
	}
	name := store.getNumberedName(fileType, fileNumber)
	return file.Size(), store.filedeadspace[name], nil
}

func (store *Store) findFreeOffset(fileType int, ignore int32) (freeFileNum int32, freeOffset int64, freeFile *core.BufferedFile) {
	for {
		if store.topFileNumber[fileType] != ignore {
			freeFile = store.getNumberedFile(fileType, store.topFileNumber[fileType], true)
			freeOffset, _ = freeFile.Writer.Seek(0, io.SeekEnd)
			if freeOffset <= storageOffsetLimit {
				break
			}
		}
		store.topFileNumber[fileType]++
	}
	return store.topFileNumber[fileType], freeOffset, freeFile
}

// setDeadSpace is used to mark the amount of dead space in a meta or data file
func (store *Store) setDeadSpace(fileType int, fileNumber int32, size int64, add bool) {
	file := store.getNumberedFile(fileType, fileNumber, false)
	if file == nil {
		core.Abort("Trying to mark free space in %.8X%s which does not exist", fileNumber, storageFileTypeInfo[fileType].Extension)
	}

	// Update the cached entry
	name := store.getNumberedName(fileType, fileNumber)
	if add {
		store.filedeadspace[name] += size
	} else {
		store.filedeadspace[name] = size
	}

	// Write new deadspace to file
	file.Writer.Seek(storageFileDeadspaceOffset, io.SeekStart)
	core.WriteInt64(file.Writer, store.filedeadspace[name])
}

func (store *Store) ShowStorageDeadSpace() {
	for datFileNumber := int32(0); ; datFileNumber++ {
		var datFile = store.getNumberedFile(storageFileTypeData, datFileNumber, false)
		if datFile == nil {
			break // no more data
		}
		fileSize, deadSpace, err := store.getNumberedFileSize(storageFileTypeData, datFileNumber)
		core.AbortOn(err)
		core.Log(core.LogInfo, "File %s, %s (est. dead data %s)", datFile.Path, core.HumanSize(fileSize), core.HumanSize(deadSpace))
	}
}

//***********************************************************************//
//                                 Store                                 //
//***********************************************************************//

type Store struct {
	filepool      map[string]*core.BufferedFile
	filedeadspace map[string]int64
	topFileNumber []int32

	DataDir  string
	IndexDir string
}

func NewStore(datDir, idxDir string) *Store {
	return &Store{
		filepool:      make(map[string]*core.BufferedFile),
		filedeadspace: make(map[string]int64),
		topFileNumber: []int32{0, 0, 0},
		DataDir:       datDir,
		IndexDir:      idxDir,
	}
}

func (store *Store) SyncAll() {
	for s, f := range store.filepool {
		core.Log(core.LogDebug, "Syncing %s", s)
		f.Sync()
	}
}

func (store *Store) Close() {
	for s, f := range store.filepool {
		core.Log(core.LogInfo, "Closing %s", s)
		f.Close()
	}
}

func (store *Store) DoesBlockExist(blockID core.Byte128) bool {
	_, _, _, err := store.readIXEntry(blockID)
	return err == nil
}

// ReadBlockMeta returns metadata for a block without loading its data payload.
func (store *Store) ReadBlockMeta(blockID core.Byte128) *StorageMetaEntry {
	ixEntry, _, _, err := store.readIXEntry(blockID)
	if err != nil {
		return nil
	}
	metaFileNumber, metaOffset := ixEntry.location.Get()
	metaEntry, err := store.readMetaEntry(metaFileNumber, metaOffset)
	if err != nil {
		return nil
	}
	return metaEntry
}

func (store *Store) ReadBlock(blockID core.Byte128) *core.HashboxBlock {
	block, _ := store.readBlockFile(blockID)
	return block
}

func (store *Store) WriteBlock(block *core.HashboxBlock) bool {
	return store.writeBlockFile(block)
}

const MINIMUM_IX_FREE = int64(storageIXFileSize + storageIXFileSize/20) // 105% of an IX file because we must be able to create a new one
const MINIMUM_DAT_FREE = int64(1 << 26)                                 // 64 MB minimum free space

func (store *Store) CheckFree(size int64) bool {
	if free, _ := core.FreeSpace(store.IndexDir); free < MINIMUM_IX_FREE {
		core.Log(core.LogWarning, "Storage rejected because free space on index path has dropped below %d", MINIMUM_IX_FREE)
		return false
	}
	if free, _ := core.FreeSpace(store.DataDir); free < size+MINIMUM_DAT_FREE {
		core.Log(core.LogWarning, "Storage rejected because free space on data path has dropped below %d", size+MINIMUM_DAT_FREE)
		return false
	}
	return true
}
