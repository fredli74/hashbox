//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2018
//	+---+´

package main

import (
	"github.com/fredli74/hashbox/core"

	"bytes"
	_ "crypto/aes"
	_ "crypto/cipher"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func (session *BackupSession) PrintRestoreProgress() {
	var compression float64
	if session.WriteData > 0 {
		compression = 100.0 * (float64(session.WriteData) - float64(session.ReadData)) / float64(session.WriteData)
	}
	session.Log(">>> %.1f min, read: %s (%.0f%% compr), written: %s, %d folders, %d files",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), compression, core.HumanSize(session.WriteData), session.Directories, session.Files)

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

	d := block.Data
	d.ReadSeek(0, os.SEEK_SET)
	core.CopyOrPanic(f, &d)
	session.WriteData += int64(d.Len())
	d.Release()
	return nil
}

func (session *BackupSession) restoreEntry(e *FileEntry, path string) error {
	localname := platformSafeFilename(string(e.FileName))
	if localname != string(e.FileName) {
		session.Log("Platform restricted characters substituted in filename %s", string(e.FileName))
	}
	localpath := filepath.Join(path, localname)
	session.LogVerbose(localpath)

	if e.FileMode&uint32(os.ModeSymlink) > 0 {
		switch e.ContentType {
		case ContentTypeSymLink:
			if err := os.Symlink(string(e.FileLink), localpath); err != nil {
				return err
			}
		default:
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a Symlink (%d)", e.ContentType)))
		}
	} else if e.FileMode&uint32(os.ModeDir) > 0 {
		if err := os.MkdirAll(localpath, 0777); err != nil {
			return err
		}

		switch e.ContentType {
		case ContentTypeDirectory:
			if err := session.restoreDir(e.ContentBlockID, localpath); err != nil {
				return err
			}
		case ContentTypeEmpty:
		default:
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a directory (%d)", e.ContentType)))
		}
		if err := os.Chmod(localpath, os.FileMode(e.FileMode) /*&os.ModePerm*/); err != nil {
			return err
		}
		session.Directories++
	} else {
		if e.ContentType != ContentTypeFileChain && e.ContentType != ContentTypeFileData && e.ContentType != ContentTypeEmpty {
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a file (%d)", e.ContentType)))
		}
		err := func() error {
			fil, err := os.OpenFile(localpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(e.FileMode) /*&os.ModePerm*/)
			if err != nil {
				return err
			}
			defer fil.Close()

			switch e.ContentType {
			case ContentTypeEmpty:
				// No data
			case ContentTypeFileChain:
				var chain FileChainBlock

				blockData := session.Client.ReadBlock(e.ContentBlockID).Data
				chain.Unserialize(&blockData)
				blockData.Release()

				for i := range chain.ChainBlocks {
					if err := session.restoreFileData(fil, chain.ChainBlocks[i], chain.DecryptKeys[i]); err != nil {
						return err
					}
				}
			case ContentTypeFileData:
				if err := session.restoreFileData(fil, e.ContentBlockID, e.DecryptKey); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
		if err := os.Chtimes(localpath, time.Now(), time.Unix(0, e.ModTime)); err != nil {
			return err
		}
		session.Files++
	}
	return nil
}

func (session *BackupSession) restoreDir(blockID core.Byte128, path string) error {
	var dir DirectoryBlock

	blockData := session.Client.ReadBlock(blockID).Data
	dir.Unserialize(&blockData)
	blockData.Release()

	for _, e := range dir.File {
		if err := session.restoreEntry(e, path); err != nil {
			return err
		}
	}
	return nil
}

func (session *BackupSession) changeDir(blockID core.Byte128, pathList []string) (*DirectoryBlock, int, error) {
	var dir DirectoryBlock
	blockData := session.Client.ReadBlock(blockID).Data
	dir.Unserialize(&blockData)
	blockData.Release()

	if len(pathList) > 0 {
		for _, f := range dir.File {
			if string(f.FileName) == pathList[0] && f.ContentType == ContentTypeDirectory {
				return session.changeDir(f.ContentBlockID, pathList[1:])
			}
		}
		return &dir, len(pathList), errors.New("Path not found")
	}
	return &dir, len(pathList), nil
}

func (session *BackupSession) Restore(rootBlockID core.Byte128, restorepath string, restorelist ...string) {
	if err := os.MkdirAll(restorepath, 0777); err != nil {
		panic(err)
	}

	if len(restorelist) > 0 {
		for _, r := range restorelist {
			list, err := session.findPathMatch(rootBlockID, r)
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
		if err := session.restoreDir(rootBlockID, restorepath); err != nil {
			panic(err)
		}
	}
	session.PrintRestoreProgress()
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
		nA, err := A.Read(bufA)
		if nA == 0 {
			return true
		}
		nB, err := B.Read(bufB[:nA])
		if nA != nB {
			PanicOn(err)
			panic(errors.New("Unable to read local file"))
		}

		if bytes.Compare(bufA[:nA], bufB[:nA]) != 0 {
			session.Log("local:  %s", hexdumpLine(offset, bufA[:nA]))
			session.Log("remote: %s", hexdumpLine(offset, bufB[:nB]))
			return false
		}
		offset += int64(nA)
	}
	return true
}

func (session *BackupSession) diffFileData(f *os.File, blockID core.Byte128, DecryptKey core.Byte128) bool {
	if session.ShowProgress && time.Now().After(session.Progress) {
		session.PrintDiffProgress()
		session.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	}

	block := session.Client.ReadBlock(blockID)
	if !block.VerifyBlock() {
		panic(errors.New(fmt.Sprintf("Block %x corrupted, hash does not match")))
	}

	session.ReadData += int64(block.CompressedSize)
	// TODO: Decrypt block

	d := block.Data
	defer d.Release()

	d.ReadSeek(0, os.SEEK_SET)
	offset, _ := f.Seek(0, os.SEEK_CUR)

	if !session.diffAndPrint(offset, &d, f) {
		return false
	}

	session.WriteData += int64(d.Len())
	return true
}

func (session *BackupSession) diffEntry(e *FileEntry, path string) (same bool, err error) {
	localname := filepath.Join(path, platformSafeFilename(string(e.FileName)))
	localinfo, err := os.Lstat(localname)
	if os.IsNotExist(err) {
		return false, errors.New("local path does not exist")
	}
	same = true

	if e.FileMode&uint32(os.ModeSymlink) > 0 {
		switch e.ContentType {
		case ContentTypeSymLink:
			if localinfo.Mode()&os.ModeSymlink != os.ModeSymlink {
				return false, errors.New("local path is not a symlink")
			} else {
				link, err := os.Readlink(localname)
				if err != nil {
					return false, err
				} else if link != string(e.FileLink) {
					return false, errors.New(fmt.Sprintf("symlink points to %s and not %s", link, string(e.FileLink)))
				}
			}
		default:
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a Symlink (%d)", e.ContentType)))
		}
	} else if e.FileMode&uint32(os.ModeDir) > 0 {
		if !localinfo.IsDir() {
			return false, errors.New("local path is not a directory")
		}

		switch e.ContentType {
		case ContentTypeDirectory:
			session.diffDir(e.ContentBlockID, localname)
		case ContentTypeEmpty:
		default:
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a directory (%d)", e.ContentType)))
		}
		session.Directories++
	} else {
		if e.ContentType != ContentTypeFileChain && e.ContentType != ContentTypeFileData && e.ContentType != ContentTypeEmpty {
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a file (%d)", e.ContentType)))
		}
		same, err = func() (bool, error) {
			if e.FileSize != int64(localinfo.Size()) {
				return false, errors.New(fmt.Sprintf("local file size %d and remote size %d are different", e.FileSize, int64(localinfo.Size())))
			}
			if e.FileMode&uint32(os.ModePerm) != uint32(localinfo.Mode())&uint32(os.ModePerm) {
				session.Log("%s local permissions %s and remote %s are different", localname, localinfo.Mode(), os.FileMode(e.FileMode))
				same = false
			}
			if e.ModTime != localinfo.ModTime().UnixNano() {
				session.Log("%s local modification time %s and remote %s are different", localname, localinfo.ModTime().Format(time.RFC3339), time.Unix(0, int64(e.ModTime)).Format(time.RFC3339))
				same = false
			}

			fil, err := os.OpenFile(localname, os.O_RDONLY, 0)
			if err != nil {
				return false, err
			}
			defer fil.Close()

			switch e.ContentType {
			case ContentTypeEmpty:
				// No data
			case ContentTypeFileChain:
				var chain FileChainBlock

				blockData := session.Client.ReadBlock(e.ContentBlockID).Data
				chain.Unserialize(&blockData)
				blockData.Release()

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

	blockData := session.Client.ReadBlock(blockID).Data
	dir.Unserialize(&blockData)
	blockData.Release()

	fil, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fil.Close()

	fl, err := fil.Readdir(-1)
	if err != nil {
		panic(err)
	}
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
	PanicOn(err)

	if !info.IsDir() {
		panic(errors.New(fmt.Sprintf("%s is not a directory", restorepath)))
	}

	if len(restorelist) > 0 {
		for _, r := range restorelist {
			list, err := session.findPathMatch(rootBlockID, r)
			if err != nil {
				panic(err)
			}
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
