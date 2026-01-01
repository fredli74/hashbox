//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package accountdb

import (
	"crypto/md5"
	"io"
	"os"
	"sort"

	"github.com/fredli74/hashbox/pkg/core"
)

// DBStateCollection mirrors the cached .db representation.
type DBStateCollection struct {
	Size   int64        // Size of all data referenced by this dataset
	ListH  core.Byte128 // = md5(States)
	States core.DatasetStateArray
}

func (c *DBStateCollection) Serialize(w io.Writer) {
	c.States.Serialize(w)
}
func (c *DBStateCollection) Unserialize(r io.Reader) {
	c.States.Unserialize(r)
	{
		c.Size = 0
		maxSize := int64(0)
		hash := md5.New()
		for _, s := range c.States {
			s.Serialize(hash)
			if s.State.Size > maxSize {
				maxSize = s.State.Size
			}
			c.Size += s.State.UniqueSize // TODO: this calculation is wrong the moment you start deleting stuff, it needs to be reworked
		}
		copy(c.ListH[:], hash.Sum(nil)[:16])
		c.Size += maxSize
	}
}

// ReadDBFile reads a .db cache file.
func (fs *Store) ReadDBFile(accountNameH core.Byte128, datasetName core.String) *DBStateCollection {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + DbFileExtensionDatabase
	file, err := os.Open(filename)
	if err != nil {
		return nil
	}
	defer file.Close()
	var header dbFileHeader
	header.Unserialize(file)
	if header.filetype != DbFileTypeDatabase {
		core.Abort("%s is not a valid db file", filename)
	}
	if header.version != DbVersion {
		core.Abort("Unsupported db file version in %s", filename)
	}
	var c DBStateCollection
	c.Unserialize(file)
	return &c
}

// WriteDBFile writes a .db cache file and updates the .info dataset list.
func (fs *Store) WriteDBFile(accountNameH core.Byte128, datasetName core.String, c *DBStateCollection) {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + DbFileExtensionDatabase
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	core.AbortOn(err)
	defer file.Close()

	header := dbFileHeader{filetype: DbFileTypeDatabase, version: DbVersion, datasetName: datasetName}
	header.Serialize(file)
	c.Serialize(file)

	fs.updateInfoFile(accountNameH, datasetName)
}

// RebuildDB regenerates the cached DB file for a dataset from its transaction log.
func (fs *Store) RebuildDB(accountNameH core.Byte128, datasetName core.String) core.DatasetStateArray {
	states := fs.StateArrayFromTransactions(accountNameH, datasetName)
	sort.Sort(states)
	fs.WriteDBFile(accountNameH, datasetName, &DBStateCollection{States: states})
	return states
}
