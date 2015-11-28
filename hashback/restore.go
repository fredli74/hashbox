//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/hashbox/core"

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
	fmt.Printf(">>> %.1f min, read: %s (%.0f%% compr), write: %s, %d folders, %d files\n",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), compression, core.HumanSize(session.WriteData), session.Directories, session.Files)

	fmt.Println(core.MemoryStats())
}

func (session *BackupSession) PrintDiffProgress() {
	var compression float64
	if session.WriteData > 0 {
		compression = 100.0 * (float64(session.WriteData) - float64(session.ReadData)) / float64(session.WriteData)
	}
	fmt.Printf(">>> %.1f min, read: %s (%.0f%% compr), compared: %s, %d folders, %d files\n",
		time.Since(session.Start).Minutes(), core.HumanSize(session.ReadData), compression, core.HumanSize(session.WriteData), session.Directories, session.Files)

	fmt.Println(core.MemoryStats())
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

func (backup *BackupSession) Restore(rootBlockID core.Byte128, restorepath string, restorelist ...string) {
	if err := os.MkdirAll(restorepath, 0777); err != nil {
		panic(err)
	}

	if len(restorelist) > 0 {
		for _, r := range restorelist {
			list, err := backup.findPathMatch(rootBlockID, r)
			if err != nil {
				panic(err)
			}
			for _, e := range list {
				if err := backup.restoreEntry(e, restorepath); err != nil {
					panic(err)
				}
			}
		}
	} else {
		if err := backup.restoreDir(rootBlockID, restorepath); err != nil {
			panic(err)
		}
	}
	backup.PrintRestoreProgress()
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
func (backup *BackupSession) diffAndPrint(startOffset int64, A io.Reader, B io.Reader) bool {
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
			backup.Important("local: ", hexdumpLine(offset, bufA[:nA]))
			backup.Important("remote:", hexdumpLine(offset, bufB[:nB]))
			return false
		}
		offset += int64(nA)
	}
	return true
}

func (backup *BackupSession) diffFileData(f *os.File, blockID core.Byte128, DecryptKey core.Byte128) bool {
	if backup.ShowProgress && time.Now().After(backup.Progress) {
		backup.PrintDiffProgress()
		backup.Progress = time.Now().Add(PROGRESS_INTERVAL_SECS)
	}

	block := backup.Client.ReadBlock(blockID)
	if !block.VerifyBlock() {
		panic(errors.New(fmt.Sprintf("Block %x corrupted, hash does not match")))
	}

	backup.ReadData += int64(block.CompressedSize)
	// TODO: Decrypt block

	d := block.Data
	defer d.Release()

	d.ReadSeek(0, os.SEEK_SET)
	offset, _ := f.Seek(0, os.SEEK_CUR)

	if !backup.diffAndPrint(offset, &d, f) {
		return false
	}

	backup.WriteData += int64(d.Len())
	return true
}

func (backup *BackupSession) diffEntry(e *FileEntry, path string) (same bool, err error) {
	localname := filepath.Join(path, string(e.FileName))
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
			backup.diffDir(e.ContentBlockID, localname)
		case ContentTypeEmpty:
		default:
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a directory (%d)", e.ContentType)))
		}
		backup.Directories++
	} else {
		if e.ContentType != ContentTypeFileChain && e.ContentType != ContentTypeFileData && e.ContentType != ContentTypeEmpty {
			panic(errors.New(fmt.Sprintf("Invalid ContentType for a file (%d)", e.ContentType)))
		}
		same, err = func() (bool, error) {
			if e.FileSize != int64(localinfo.Size()) {
				return false, errors.New(fmt.Sprintf("local file size %d and remote size %d are different", e.FileSize, int64(localinfo.Size())))
			}
			if e.FileMode&uint32(os.ModePerm) != uint32(localinfo.Mode())&uint32(os.ModePerm) {
				backup.Important(fmt.Sprintf("%s local permissions %s and remote %s are different", localname, localinfo.Mode(), os.FileMode(e.FileMode)))
				same = false
			}
			if e.ModTime != localinfo.ModTime().UnixNano() {
				backup.Important(fmt.Sprintf("%s local modification time %s and remote %s are different", localname, localinfo.ModTime().Format(time.RFC3339), time.Unix(0, int64(e.ModTime)).Format(time.RFC3339)))
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

				blockData := backup.Client.ReadBlock(e.ContentBlockID).Data
				chain.Unserialize(&blockData)
				blockData.Release()

				for i := range chain.ChainBlocks {
					same = backup.diffFileData(fil, chain.ChainBlocks[i], chain.DecryptKeys[i])
					if !same {
						break
					}
				}
			case ContentTypeFileData:
				same = backup.diffFileData(fil, e.ContentBlockID, e.DecryptKey)
			}
			return same, err
		}()
		if err != nil {
			return false, err
		}
		backup.Files++
	}
	return same, err
}

func (backup *BackupSession) diffDir(blockID core.Byte128, path string) {
	var dir DirectoryBlock

	blockData := backup.Client.ReadBlock(blockID).Data
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
			backup.Important(filepath.Join(path, string(dir.File[iR].FileName)), "local path does not exist")
			iR++
		} else if iR >= len(dir.File) || (iL < len(fl) && fl[iL].Name() < string(dir.File[iR].FileName)) {
			backup.Important(filepath.Join(path, fl[iL].Name()), "remote path does not exist")
			iL++
		} else {
			e := dir.File[iR]
			same, err := backup.diffEntry(e, path)
			if !same {
				backup.DifferentFiles++
				if err != nil {
					backup.Important(filepath.Join(path, string(e.FileName)), err.Error())
				} else {
					backup.Important(filepath.Join(path, string(e.FileName)), "data is different")
				}
			} else {
				backup.Log(filepath.Join(path, string(e.FileName)), "is the same")
			}
			iL++
			iR++
		}
	}
}

func (backup *BackupSession) DiffRestore(rootBlockID core.Byte128, restorepath string, restorelist ...string) {
	info, err := os.Stat(restorepath)
	PanicOn(err)

	if !info.IsDir() {
		panic(errors.New(fmt.Sprintf("%s is not a directory", restorepath)))
	}

	if len(restorelist) > 0 {
		for _, r := range restorelist {
			list, err := backup.findPathMatch(rootBlockID, r)
			if err != nil {
				panic(err)
			}
			for _, e := range list {
				same, err := backup.diffEntry(e, restorepath)
				if !same {
					backup.DifferentFiles++
					if err != nil {
						backup.Important(filepath.Join(restorepath, string(e.FileName)), err.Error())
					} else {
						backup.Important(filepath.Join(restorepath, string(e.FileName)), "data is different")
					}
				} else {
					backup.Log(filepath.Join(restorepath, string(e.FileName)), "is the same")
				}
			}
		}
	} else {
		backup.diffDir(rootBlockID, restorepath)
	}
	backup.PrintDiffProgress()
}
