//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

package main

import (
	"bitbucket.org/fredli74/hashbox/core"

	_ "crypto/aes"
	_ "crypto/cipher"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (session *BackupSession) PrintRestoreProgress() {
	var compression float64
	if session.WriteData > 0 {
		compression = 100.0 * (float64(session.WriteData) - float64(session.ReadData)) / float64(session.WriteData)
	}
	fmt.Printf("*** %.1f min, read: %s (%.0f%% compr), write: %s, %d folders, %d files\n",
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

	if !block.VerifyBlock() {
		panic(errors.New("Unable to verify block content, data is corrupted"))
	}
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
