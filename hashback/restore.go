//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package main

import (
	"github.com/fredli74/hashbox/pkg/core"

	"bytes"
	_ "crypto/aes"
	_ "crypto/cipher"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
)

func (session *BackupSession) PrintRestoreProgress() {
	var compression float64
	if session.WriteData > 0 {
		compression = 100.0 * (float64(session.WriteData) - float64(session.ReadData)) / float64(session.WriteData)
	}
	var failed string
	if session.FailedEntries > 0 {
		failed = fmt.Sprintf(", %d failed", session.FailedEntries)
	}
	session.Log(">>> %.1f min, read: %s (%.0f%% compr), written: %s, %d folders, %d/%d files restored%s",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), compression, core.HumanSize(session.WriteData), session.Directories, session.Files-session.UnchangedFiles, session.Files, failed)

	//fmt.Println(core.MemoryStats())
}

func (session *BackupSession) PrintDiffProgress() {
	var compression float64
	if session.WriteData > 0 {
		compression = 100.0 * (float64(session.WriteData) - float64(session.ReadData)) / float64(session.WriteData)
	}
	session.Log(">>> %.1f min, read: %s (%.0f%% compr), compared: %s, %d folders, %d files",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), compression, core.HumanSize(session.WriteData), session.Directories, session.Files)

	//fmt.Println(core.MemoryStats())
}

// Classifies a recovered block read panic. A dead client or a server that
// refuses the block is not damage to the backup, salvaging past it would
// mislabel every remaining entry and end in a success exit code. A runtime
// fault or a non-error value is a bug and still crashes.
func salvageBlockError(r interface{}) error {
	err, ok := r.(error)
	var fault runtime.Error
	if !ok || errors.As(err, &fault) {
		panic(r)
	}
	var remote core.RemoteError
	if errors.As(err, &remote) {
		panic(exitError{fmt.Errorf("restore aborted, %s", err), 2})
	}
	return fmt.Errorf("%w, %v", errUnreadableBackup, err)
}

// Reads a verified block and hands it to use. Damage to the backup is a panic
// from deep inside ReadBlock, and -salvage turns exactly that into an error so
// the readable part of the dataset can still be recovered. The recover is
// scoped to the read on purpose, a wider one would also swallow assertions and
// contract violations, which are never expected.
func (session *BackupSession) readBlock(blockID core.Byte128, use func(block *core.HashboxBlock, r io.Reader) error) (err error) {
	if session.Salvage {
		defer func() {
			if r := recover(); r != nil {
				err = salvageBlockError(r)
			}
		}()
	}
	block := session.Client.ReadBlock(blockID)
	defer block.Release() // before VerifyBlock, a recovered panic must not leak the slab

	// VerifyBlock only proves the block matches its own ID
	if block.BlockID != blockID || !block.VerifyBlock() {
		panic(fmt.Errorf("block %x corrupted, hash does not match", blockID))
	}
	return use(block, &block.Data)
}

// Metadata is not counted as read data, it never was.
func (session *BackupSession) readMetadataBlock(blockID core.Byte128, into core.Unserializer) error {
	return session.readBlock(blockID, func(_ *core.HashboxBlock, r io.Reader) error {
		into.Unserialize(r)
		return nil
	})
}

func (session *BackupSession) restoreFileData(w io.Writer, blockID core.Byte128, DecryptKey core.Byte128) error {
	if session.ShowProgress && time.Now().After(session.Progress) {
		session.PrintRestoreProgress()
		session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	}
	return session.readBlock(blockID, func(block *core.HashboxBlock, r io.Reader) error {
		session.ReadData += int64(block.CompressedSize)
		// TODO: Decrypt block
		n, err := io.Copy(w, r)
		if err != nil {
			return err
		}
		session.WriteData += n
		return nil
	})
}

// Mode is not compared, restoreEntry writes ModTime last so a matching one marks
// a completed entry, move that Chtimes and this stops being a safe check.
func isEntryUpToDate(e *FileEntry, localinfo os.FileInfo, localpath string) bool {
	core.ASSERT(!e.IsDir(), "isEntryUpToDate called for a directory entry")
	core.ASSERT(localinfo != nil, "isEntryUpToDate called without a stat")
	if e.IsSymlink() {
		if localinfo.Mode()&os.ModeSymlink == 0 {
			return false
		}
		// Windows stores the target with backslashes
		link, err := os.Readlink(localpath)
		return err == nil && link == filepath.FromSlash(string(e.FileLink))
	}
	// compare with second precision because of Dropbox Online Only files
	return isRestorableFile(localinfo) && localinfo.Size() == e.FileSize &&
		localinfo.ModTime().UnixNano()/1e9 == e.ModTime/1e9
}

// A placeholder (OneDrive/Dropbox on demand) is reported irregular but is still a
// file. Checked narrowly, a fast symlink also has no blocks and would match.
func isRestorableFile(localinfo os.FileInfo) bool {
	return localinfo.Mode().IsRegular() ||
		(localinfo.Mode()&os.ModeType == os.ModeIrregular && isOfflineFile(localinfo))
}

// Clears whatever blocks the destination, reporting whether anything is still
// there afterwards.
func prepareDestination(e *FileEntry, localinfo os.FileInfo, localpath string) (occupied bool, err error) {
	if localinfo == nil {
		return false, nil
	}
	if e.IsSymlink() {
		// os.Symlink cannot overwrite, and a restore replaces what it finds
		return false, os.Remove(localpath)
	}
	if e.IsDir() {
		// MkdirAll resolves a symlink to a directory and would write the whole
		// subtree into its target, so only a real directory is kept
		if localinfo.IsDir() {
			if localinfo.Mode().Perm()&0300 != 0300 {
				// best effort, chmod needs ownership but the children may still be
				// writable through group or other, so a failure must not skip them
				_ = os.Chmod(localpath, localinfo.Mode()|0300) // Perm() would clear setgid
			}
			return true, nil
		}
	} else if isRestorableFile(localinfo) {
		// a read-only or unwritable file is dealt with by reopenForWrite, guessing
		// from the mode bits is wrong for anything we do not own. A cloud
		// placeholder is overwritten in place, removing it rehydrates the tree.
		return true, nil
	}
	// a symlink would be followed by O_TRUNC, anything else is the wrong type
	return false, os.Remove(localpath)
}

// Opens the destination on the first write, so an existing file is only
// truncated once the entry's first block has been read. A one block file is
// fully read by then, a longer one can still fail part way.
type lazyFile struct {
	open func() (*os.File, error)
	f    *os.File
}

func (l *lazyFile) Write(p []byte) (n int, err error) {
	if l.f == nil {
		if l.f, err = l.open(); err != nil {
			return 0, err
		}
	}
	return l.f.Write(p)
}

// Recreates the destination after an open failed on a file that is there but
// cannot be reopened, which is what a previous restore leaves behind when the
// stored mode is read-only. Returns the original error if it cannot be removed.
func reopenForWrite(localpath string, filemode os.FileMode, openErr error, occupied bool) (*os.File, error) {
	// only a mode problem is fixed by recreating. Unlinking on anything else
	// destroys a good file, and a cloud placeholder deleted here syncs upstream
	if !occupied || !errors.Is(openErr, fs.ErrPermission) {
		return nil, openErr
	}
	if err := os.Remove(localpath); err != nil {
		return nil, openErr
	}
	// the file is gone, so the create error is the one that counts (ENOSPC)
	return os.OpenFile(localpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, filemode)
}

func (session *BackupSession) logRestoreError(err error) {
	session.FailedEntries++
	session.errorCounts[errorCause(err)]++
	session.Log("FAILED %s", err.Error()) // .Error() so core.Log escapes it
}

func (session *BackupSession) PrintRestoreErrors() {
	classes := slices.SortedFunc(maps.Keys(session.errorCounts), func(a, b string) int {
		if n := session.errorCounts[b] - session.errorCounts[a]; n != 0 {
			return n
		}
		return strings.Compare(a, b)
	})
	for _, c := range classes {
		session.Log("    %6d  %s", session.errorCounts[c], c)
	}
}

var (
	errLostWithParent   = errors.New("lost with parent directory")
	errUnreadableBackup = errors.New("unreadable backup data")
	errUnsafeName       = errors.New("unsafe file name in backup")
	errNameCollision    = errors.New("same local name as an entry restored earlier")
)

// counted classes that carry no errno, see errorCause
var errorClasses = []error{errLostWithParent, errUnreadableBackup, errUnsafeName, errNameCollision}

// The class an error is counted under. Without this the summary degrades to one
// line per path, which is what it exists to avoid. Sentinels first, they carry
// no errno; the fallback is the whole message and is the case to keep rare.
func errorCause(err error) string {
	for _, sentinel := range errorClasses {
		if errors.Is(err, sentinel) {
			return sentinel.Error()
		}
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	return err.Error()
}

// Counts and logs every entry below a failed directory. Reads blocks, writes nothing.
func (session *BackupSession) reportLostSubtree(e *FileEntry, localpath string) {
	if e.ContentType != ContentTypeDirectory {
		return
	}
	var dir DirectoryBlock
	if err := session.readMetadataBlock(e.ContentBlockID, &dir); err != nil {
		session.logRestoreError(fmt.Errorf("%w: %s", err, localpath))
		return
	}

	for _, child := range dir.File {
		childpath := filepath.Join(localpath, platformSafeFilename(string(child.FileName)))
		session.logRestoreError(fmt.Errorf("%w: %s", errLostWithParent, childpath))
		session.reportLostSubtree(child, childpath)
	}
}

// Carries a directory failure that happened before its children were walked,
// so the subtree can be accounted for once the run is known to continue.
type lostSubtreeError struct {
	error
	localpath string
}

func (e lostSubtreeError) Unwrap() error { return e.error }

// Only returned errors are tolerated, a panic still aborts. Damage to the
// backup is turned into an error by readMetadataBlock/restoreFileData under
// -salvage, so nothing here has to guess what a panic meant.
func (session *BackupSession) tryRestoreEntry(e *FileEntry, path string, restored restoredNames) {
	localname := platformSafeFilename(string(e.FileName))
	if localname != string(e.FileName) {
		session.Log("Platform restricted characters substituted in filename %s", string(e.FileName))
	}
	localpath := filepath.Join(path, localname)
	key := strings.ToLower(localname)
	err := session.restoreEntry(e, localname, localpath, restored[key])
	now, _ := os.Lstat(localpath) // whatever is there now, failed or not
	restored[key] = append(restored[key], now)
	if err == nil {
		return
	}
	session.logRestoreError(err)
	if session.StopOnError || isFatalRestoreError(err) {
		panic(exitError{fmt.Errorf("restore aborted, %s", err), 2})
	}
	// only now that the run continues is counting the subtree worth the blocks
	var lost lostSubtreeError
	if errors.As(err, &lost) {
		session.reportLostSubtree(e, lost.localpath)
	}
}

// Siblings restored so far by case folded local name. A fold match is only a
// candidate, os.SameFile decides, so case sensitive targets keep both names.
// All of them are kept, foo FOO foo must not let the second foo pass.
type restoredNames map[string][]os.FileInfo

// siblings are what earlier entries with the same folded name left behind.
func (session *BackupSession) restoreEntry(e *FileEntry, localname string, localpath string, siblings []os.FileInfo) error {
	// the name is server input, only a plain name may be joined to the destination.
	// Store names a "/" source "/", its tree is restored into the destination itself
	isRoot := localname == "/" && e.IsDir()
	if !isRoot && (localname == "." || filepath.Base(localname) != localname || !filepath.IsLocal(localname)) {
		return lostSubtreeError{fmt.Errorf("%w: %q", errUnsafeName, localname), localpath}
	}
	localinfo, _ := os.Lstat(localpath)
	// a restore replaces what it finds, but never what this run just restored
	if slices.ContainsFunc(siblings, func(s os.FileInfo) bool { return os.SameFile(s, localinfo) }) {
		return lostSubtreeError{fmt.Errorf("%w: %s", errNameCollision, localpath), localpath}
	}

	// a directory is never up to date, its contents still have to be walked
	if !e.IsDir() && session.Resume && localinfo != nil && isEntryUpToDate(e, localinfo, localpath) {
		if !e.IsSymlink() {
			session.Files++
			session.UnchangedFiles++
		}
		session.LogVerbose("UNCHANGED %s", localpath)
		return nil
	}

	occupied, err := prepareDestination(e, localinfo, localpath)
	if err != nil {
		return lostSubtreeError{err, localpath}
	}

	if e.IsSymlink() {
		session.LogVerbose("SYMLINK %s -> %s", localpath, string(e.FileLink))
		switch e.ContentType {
		case ContentTypeSymLink:
			if err := os.Symlink(string(e.FileLink), localpath); err != nil {
				return err
			}
		default:
			panic((fmt.Errorf("invalid ContentType for a symlink (%d)", e.ContentType)))
		}
	} else if e.IsDir() {
		session.LogVerbose("DIR %s", localpath)
		// the parent is always restored first, so MkdirAll and its extra stat
		// would only repeat work already done. ErrExist covers what occupied did
		if err := os.Mkdir(localpath, 0777); err != nil && !errors.Is(err, fs.ErrExist) {
			return lostSubtreeError{err, localpath}
		}

		switch e.ContentType {
		case ContentTypeDirectory:
			if err := session.restoreDir(e.ContentBlockID, localpath); err != nil {
				_ = os.Chmod(localpath, os.FileMode(e.FileMode)) // best effort, we are already failing
				return fmt.Errorf("%w: %s", err, localpath)
			}
		case ContentTypeEmpty:
		default:
			panic((fmt.Errorf("invalid ContentType for a directory (%d)", e.ContentType)))
		}
		if err := os.Chmod(localpath, os.FileMode(e.FileMode) /*&os.ModePerm*/); err != nil {
			return err
		}
		session.Directories++
	} else {
		if e.ContentType != ContentTypeFileChain && e.ContentType != ContentTypeFileData && e.ContentType != ContentTypeEmpty {
			panic((fmt.Errorf("invalid ContentType for a file (%d)", e.ContentType)))
		}
		session.LogVerbose("%s", localpath)
		// the full mode, open applies setuid/setgid/sticky on create and masking
		// them here would silently drop them from every restored file. Not as
		// root: the backup holds no owner, so the file would be root's.
		filemode := os.FileMode(e.FileMode)
		if filemode&(os.ModeSetuid|os.ModeSetgid) != 0 && os.Geteuid() == 0 {
			filemode &^= os.ModeSetuid | os.ModeSetgid
			session.Log("Setuid/setgid dropped, the backup holds no owner: %s", localpath)
		}
		// block errors do not carry the path
		writeContent := func(w io.Writer) (err error) {
			defer func() {
				if err != nil {
					err = fmt.Errorf("%w: %s", err, localpath)
				}
			}()
			switch e.ContentType {
			case ContentTypeFileChain:
				var chain FileChainBlock
				if err := session.readMetadataBlock(e.ContentBlockID, &chain); err != nil {
					return err
				}
				for i := range chain.ChainBlocks {
					if err := session.restoreFileData(w, chain.ChainBlocks[i], chain.DecryptKeys[i]); err != nil {
						return err
					}
				}
			case ContentTypeFileData:
				return session.restoreFileData(w, e.ContentBlockID, e.DecryptKey)
			}
			return nil
		}
		err := func() (err error) {
			dst := &lazyFile{open: func() (*os.File, error) {
				fil, err := os.OpenFile(localpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, filemode)
				if err != nil {
					return reopenForWrite(localpath, filemode, err, occupied)
				}
				return fil, nil
			}}
			defer func() {
				if dst.f != nil {
					err = errors.Join(err, dst.f.Close())
				}
			}()
			if err := writeContent(dst); err != nil {
				return err
			}
			if _, err := dst.Write(nil); err != nil { // opens an empty file
				return err
			}
			// writing clears setuid/setgid unless we hold CAP_FSETID, so put them
			// back on the handle. Skipped for ordinary modes, a chmod that a
			// filesystem rejects (vfat, smb) would fail an otherwise good entry.
			if filemode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
				return dst.f.Chmod(filemode)
			}
			return nil
		}()
		if err != nil {
			return err
		}
		// last, so an interrupted entry never carries a matching mtime
		if err := os.Chtimes(localpath, time.Now(), time.Unix(0, e.ModTime)); err != nil {
			return err
		}
		session.Files++
	}
	return nil
}

// Returns an error only when the listing itself cannot be read, a failed child
// is handled by tryRestoreEntry.
func (session *BackupSession) restoreDir(blockID core.Byte128, path string) error {
	var dir DirectoryBlock

	if err := session.readMetadataBlock(blockID, &dir); err != nil {
		return err
	}

	restored := restoredNames{}
	for _, e := range dir.File {
		session.tryRestoreEntry(e, path, restored)
	}
	return nil
}

func (session *BackupSession) changeDir(blockID core.Byte128, pathList []string) (*DirectoryBlock, int, error) {
	var dir DirectoryBlock
	if err := session.readMetadataBlock(blockID, &dir); err != nil {
		return nil, len(pathList), err
	}

	if len(pathList) > 0 {
		for _, f := range dir.File {
			if string(f.FileName) == pathList[0] && f.ContentType == ContentTypeDirectory {
				return session.changeDir(f.ContentBlockID, pathList[1:])
			}
		}
		return &dir, len(pathList), errors.New("path not found")
	}
	return &dir, len(pathList), nil
}

func (session *BackupSession) Restore(rootBlockID core.Byte128, restorepath string, restorelist ...string) {
	if err := os.MkdirAll(restorepath, 0777); err != nil {
		core.AbortOnError(err)
	}
	defer session.PrintRestoreErrors()
	defer session.PrintRestoreProgress()

	if len(restorelist) > 0 {
		restored := restoredNames{}     // every pattern lands in restorepath
		matched := map[FileEntry]bool{} // an entry two patterns match is restored once
		for _, r := range restorelist {
			list, err := session.findPathMatch(rootBlockID, r)
			if err != nil {
				// the sibling branch aborts the same way, this one stack traced
				panic(exitError{fmt.Errorf("restore aborted, %s", err), 2})
			}
			for _, e := range list {
				if !matched[*e] {
					matched[*e] = true
					session.tryRestoreEntry(e, restorepath, restored)
				}
			}
		}
	} else if err := session.restoreDir(rootBlockID, restorepath); err != nil {
		// nothing was restored and nothing can be, not even under -salvage
		panic(exitError{fmt.Errorf("restore aborted, %s: %s", err, restorepath), 2})
	}

	if session.FailedEntries > 0 {
		panic(exitError{fmt.Errorf("restore completed with %d skipped entries, including everything below a skipped directory", session.FailedEntries), 1})
	}
}

/*********************** DIFF *******************/

func hexdumpLine(offset int64, buffer []byte) (line string) {
	line = fmt.Sprintf("%08x  ", offset)
	for i, b := range buffer {
		line += fmt.Sprintf("%02x ", b)
		if i%8 == 7 {
			line += " "
		}
	}
	line += "|"
	for _, b := range buffer {
		if b < 32 || b > 126 {
			line += "."
		} else {
			line += string(b)
		}
	}
	line += "|"
	return line
}
func (session *BackupSession) diffAndPrint(startOffset int64, A io.Reader, B io.Reader) bool {
	bufA := make([]byte, 16)
	bufB := make([]byte, 16)
	for offset := startOffset; ; {
		nA, errA := A.Read(bufA)
		if errA != nil && errA != io.EOF {
			core.AbortOnError(errA)
		}
		if nA == 0 {
			return true
		}
		nB, errB := B.Read(bufB[:nA])
		if errB != nil && errB != io.EOF {
			core.AbortOnError(errB)
		}
		if nA != nB {
			panic(errors.New("unable to read local file"))
		}

		if !bytes.Equal(bufA[:nA], bufB[:nA]) {
			session.Log("local:  %s", hexdumpLine(offset, bufA[:nA]))
			session.Log("remote: %s", hexdumpLine(offset, bufB[:nB]))
			return false
		}
		offset += int64(nA)
	}
}

func (session *BackupSession) diffFileData(f *os.File, blockID core.Byte128, DecryptKey core.Byte128) bool {
	if session.ShowProgress && time.Now().After(session.Progress) {
		session.PrintDiffProgress()
		session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	}

	same := true
	core.AbortOnError(session.readBlock(blockID, func(block *core.HashboxBlock, r io.Reader) error {
		session.ReadData += int64(block.CompressedSize)
		// TODO: Decrypt block

		offset, err := f.Seek(0, io.SeekCurrent)
		core.AbortOnError(err)
		if same = session.diffAndPrint(offset, r, f); same {
			session.WriteData += int64(block.Data.Len())
		}
		return nil
	}))
	return same
}

func (session *BackupSession) diffEntry(e *FileEntry, path string) (same bool, err error) {
	localname := filepath.Join(path, platformSafeFilename(string(e.FileName)))
	localinfo, err := os.Lstat(localname)
	if os.IsNotExist(err) {
		return false, errors.New("local path does not exist")
	}
	same = true

	if e.IsSymlink() {
		switch e.ContentType {
		case ContentTypeSymLink:
			if localinfo.Mode()&os.ModeSymlink != os.ModeSymlink {
				return false, errors.New("local path is not a symlink")
			}
			link, err := os.Readlink(localname)
			if err != nil {
				return false, err
			}
			if link != filepath.FromSlash(string(e.FileLink)) {
				return false, fmt.Errorf("symlink points to %s and not %s", link, string(e.FileLink))
			}

		default:
			panic((fmt.Errorf("invalid ContentType for a symlink (%d)", e.ContentType)))
		}
	} else if e.IsDir() {
		if !localinfo.IsDir() {
			return false, errors.New("local path is not a directory")
		}

		switch e.ContentType {
		case ContentTypeDirectory:
			session.diffDir(e.ContentBlockID, localname)
		case ContentTypeEmpty:
		default:
			panic((fmt.Errorf("invalid ContentType for a directory (%d)", e.ContentType)))
		}
		session.Directories++
	} else {
		if e.ContentType != ContentTypeFileChain && e.ContentType != ContentTypeFileData && e.ContentType != ContentTypeEmpty {
			panic((fmt.Errorf("invalid ContentType for a file (%d)", e.ContentType)))
		}
		same, err = func() (bool, error) {
			if e.FileSize != int64(localinfo.Size()) {
				return false, fmt.Errorf("local file size %d and remote size %d are different", e.FileSize, int64(localinfo.Size()))
			}
			if e.FileMode&uint32(os.ModePerm) != uint32(localinfo.Mode())&uint32(os.ModePerm) {
				session.Log("%s local permissions %s and remote %s are different", localname, localinfo.Mode(), os.FileMode(e.FileMode))
				same = false
			}
			if e.ModTime/1e9 != localinfo.ModTime().UnixNano()/1e9 { // compare with second precision because of Dropbox Online Only files
				session.Log("%s local modification time %s and remote %s are different", localname, localinfo.ModTime().Format(time.RFC3339), time.Unix(0, int64(e.ModTime)).Format(time.RFC3339))
				same = false
			}

			fil, err := os.OpenFile(localname, os.O_RDONLY, 0)
			if err != nil {
				return false, err
			}
			defer func() {
				core.AbortOnError(fil.Close())
			}()

			switch e.ContentType {
			case ContentTypeEmpty:
				// No data
			case ContentTypeFileChain:
				var chain FileChainBlock

				core.AbortOnError(session.readMetadataBlock(e.ContentBlockID, &chain))

				for i := range chain.ChainBlocks {
					same = session.diffFileData(fil, chain.ChainBlocks[i], chain.DecryptKeys[i])
					if !same {
						break
					}
				}
			case ContentTypeFileData:
				same = session.diffFileData(fil, e.ContentBlockID, e.DecryptKey)
			}
			return same, err
		}()
		if err != nil {
			return false, err
		}
		session.Files++
	}
	return same, err
}

// Todo: diffDir does not work with platform filename substitution
func (session *BackupSession) diffDir(blockID core.Byte128, path string) {
	var dir DirectoryBlock
	core.AbortOnError(session.readMetadataBlock(blockID, &dir))

	fil, err := os.Open(path)
	core.AbortOnError(err)
	defer func() {
		core.AbortOnError(fil.Close())
	}()

	fl, err := fil.Readdir(-1)
	core.AbortOnError(err)
	sort.Sort(FileInfoSlice(fl))

	iL, iR := 0, 0
	for iL < len(fl) && iR < len(dir.File) {
		if iL >= len(fl) || (iR < len(dir.File) && fl[iL].Name() > string(dir.File[iR].FileName)) {
			session.Log("%s local path does not exist", filepath.Join(path, string(dir.File[iR].FileName)))
			iR++
		} else if iR >= len(dir.File) || (iL < len(fl) && fl[iL].Name() < string(dir.File[iR].FileName)) {
			session.Log("%s remote path does not exist", filepath.Join(path, fl[iL].Name()))
			iL++
		} else {
			e := dir.File[iR]
			same, err := session.diffEntry(e, path)
			if !same {
				session.DifferentFiles++
				if err != nil {
					session.Log("%s %s", filepath.Join(path, string(e.FileName)), err.Error())
				} else {
					session.Log("%s data is different", filepath.Join(path, string(e.FileName)))
				}
			} else {
				session.LogVerbose("%s is the same", filepath.Join(path, string(e.FileName)))
			}
			iL++
			iR++
		}
	}
}

func (session *BackupSession) DiffRestore(rootBlockID core.Byte128, restorepath string, restorelist ...string) {
	info, err := os.Stat(restorepath)
	core.AbortOnError(err)

	if !info.IsDir() {
		panic(fmt.Errorf("%s is not a directory", restorepath))
	}

	if len(restorelist) > 0 {
		for _, r := range restorelist {
			list, err := session.findPathMatch(rootBlockID, r)
			core.AbortOnError(err)
			for _, e := range list {
				same, err := session.diffEntry(e, restorepath)
				if !same {
					session.DifferentFiles++
					if err != nil {
						session.Log("%s %s", err.Error(), filepath.Join(restorepath, string(e.FileName)))
					} else {
						session.Log("%s data is different", filepath.Join(restorepath, string(e.FileName)))
					}
				} else {
					session.LogVerbose("%s is the same", filepath.Join(restorepath, string(e.FileName)))
				}
			}
		}
	} else {
		session.diffDir(rootBlockID, restorepath)
	}
	session.PrintDiffProgress()
}
