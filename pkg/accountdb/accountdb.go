//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package accountdb

import (
	"encoding/base64"
	"io"
	"path/filepath"

	"github.com/fredli74/hashbox/pkg/core"
)

const (
	dbVersion                  uint32 = 1
	dbFileTypeTransaction      uint32 = 0x48415458 // "HATX" Hashbox Account Transaction
	dbFileExtensionTransaction string = ".trn"
	dbFileTypeDatabase         uint32 = 0x48414442 // "HADB" Hashbox Account Database
	dbFileExtensionDatabase    string = ".db"
	dbTxTypeAdd                uint32 = 0x2B414444 // "+ADD"
	dbTxTypeDel                uint32 = 0x2D44454C // "-DEL"
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
	if h.version != dbVersion {
		core.Abort("Invalid version in dbFileHeader")
	}
	h.datasetName.Unserialize(r)
}

type Store struct {
	DataDir string
}

func NewStore(dataDir string) *Store {
	return &Store{DataDir: dataDir}
}

func base64filename(d []byte) string {
	return base64.RawURLEncoding.EncodeToString(d)
}

func (fs *Store) accountFilename(nameHash core.Byte128) string {
	name := base64filename(nameHash[:])
	return filepath.Join(fs.DataDir, "account", name)
}

func (fs *Store) datasetFilename(aH core.Byte128, dName string) string {
	dNameH := core.Hash([]byte(dName))
	return fs.accountFilename(aH) + "." + base64filename(dNameH[:])
}
