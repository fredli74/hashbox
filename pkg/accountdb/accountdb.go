//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package accountdb

import (
	"encoding/base64"
	"io"
	"os"
	"path/filepath"

	"github.com/fredli74/hashbox/pkg/core"
)

const (
	DbVersion                  uint32 = 1
	DbFileTypeTransaction      uint32 = 0x48415458 // "HATX" Hashbox Account Transaction
	DbFileExtensionTransaction string = ".trn"
	DbFileTypeDatabase         uint32 = 0x48414442 // "HADB" Hashbox Account Database
	DbFileExtensionDatabase    string = ".db"
	DbTxTypeAdd                uint32 = 0x2B414444 // "+ADD"
	DbTxTypeDel                uint32 = 0x2D44454C // "-DEL"
)

type dbFileHeader struct {
	filetype    uint32
	version     uint32
	datasetName core.String
}

func (h *dbFileHeader) Serialize(w io.Writer) {
	core.WriteUint32(w, h.filetype)
	core.WriteUint32(w, h.version)
	h.datasetName.Serialize(w)
}
func (h *dbFileHeader) Unserialize(r io.Reader) {
	core.ReadUint32(r, &h.filetype)
	core.ReadUint32(r, &h.version)
	if h.version != DbVersion {
		core.Abort("Invalid version in dbFileHeader")
	}
	h.datasetName.Unserialize(r)
}

type Store struct {
	DataDir string
}

func NewStore(datDir string) *Store {
	return &Store{DataDir: datDir}
}

func base64filename(d []byte) string {
	return base64.RawURLEncoding.EncodeToString(d)
}

func (fs *Store) ensureAccountDir() {
	err := os.MkdirAll(filepath.Join(fs.DataDir, "account"), 0o755)
	core.AbortOnError(err, "mkdir %s/account: %v", fs.DataDir, err)
}

func (fs *Store) accountFilepath(nameHash core.Byte128) string {
	name := base64filename(nameHash[:])
	return filepath.Join(fs.DataDir, "account", name)
}

func DatasetFilename(aH core.Byte128, dName core.String) string {
	dNameH := core.Hash([]byte(dName))
	return base64filename(aH[:]) + "." + base64filename(dNameH[:])
}

func (fs *Store) DatasetFilepath(aH core.Byte128, dName core.String) string {
	return filepath.Join(fs.DataDir, "account", DatasetFilename(aH, dName))
}
