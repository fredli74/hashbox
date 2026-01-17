//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package accountdb

import (
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/hashbox/pkg/lockablefile"
)

type DbTx struct {
	Timestamp int64
	TxType    uint32
	Data      interface{}
}

func (t *DbTx) Serialize(w io.Writer) {
	core.WriteInt64(w, t.Timestamp)
	core.WriteUint32(w, t.TxType)
	t.Data.(core.Serializer).Serialize(w)
}
func (t *DbTx) Unserialize(r io.Reader) {
	core.ReadInt64(r, &t.Timestamp)
	core.ReadUint32(r, &t.TxType)
	switch t.TxType {
	case DbTxTypeAdd:
		var s core.DatasetState
		s.Unserialize(r)
		t.Data = s
	case DbTxTypeDel:
		var s core.Byte128
		s.Unserialize(r)
		t.Data = s
	default:
		core.Abort("Corrupt transaction file")
	}
}

// AppendTx appends a transaction to the dataset log.
// Panics on unexpected IO/lock errors.
func (fs *Store) AppendTx(accountNameH core.Byte128, datasetName core.String, tx DbTx) {
	fs.ensureAccountDir()
	filename := fs.DatasetFilename(accountNameH, datasetName) + DbFileExtensionTransaction
	lock, err := lockablefile.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	core.AbortOn(err)
	defer lock.Close()
	lock.Lock()
	defer lock.Unlock()

	pos, err := lock.Seek(0, io.SeekEnd)
	core.AbortOn(err)
	if pos == 0 { // New file, write the header
		header := dbFileHeader{filetype: DbFileTypeTransaction, version: DbVersion, datasetName: datasetName}
		header.Serialize(lock)
	}
	tx.Serialize(lock)
	// Explicit fsync to persist the append before releasing the lock; Close alone
	// does not guarantee durability.
	core.AbortOn(lock.Sync())
}

// AppendAddState appends an add tx with current timestamp.
func (fs *Store) AppendAddState(accountNameH core.Byte128, datasetName core.String, state core.DatasetState) {
	fs.AppendTx(accountNameH, datasetName, DbTx{Timestamp: time.Now().UnixNano(), TxType: DbTxTypeAdd, Data: state})
}

// AppendDelState appends a delete tx with current timestamp.
func (fs *Store) AppendDelState(accountNameH core.Byte128, datasetName core.String, stateID core.Byte128) {
	fs.AppendTx(accountNameH, datasetName, DbTx{Timestamp: time.Now().UnixNano(), TxType: DbTxTypeDel, Data: stateID})
}

// StateArrayFromTransactions returns current live states by replaying the transaction log.
func (fs *Store) StateArrayFromTransactions(accountNameH core.Byte128, datasetName core.String) (states core.DatasetStateArray) {
	filename := fs.DatasetFilename(accountNameH, datasetName) + DbFileExtensionTransaction
	reader, err := fs.NewTxReader(accountNameH, datasetName)
	core.AbortOn(err)
	defer reader.Close()

	stateMap := make(map[core.Byte128]core.DatasetState)
	// Go through transaction history and populate the stateMap
	var pointInHistory int64
	for {
		tx := reader.Next()
		if tx == nil {
			break
		}
		if tx.Timestamp < pointInHistory {
			core.Abort("%s is corrupt, timestamp check failed (%x < %x)", filename, tx.Timestamp, pointInHistory)
		}
		pointInHistory = tx.Timestamp
		switch tx.TxType {
		case DbTxTypeAdd:
			state := tx.Data.(core.DatasetState)
			stateMap[state.StateID] = state
		case DbTxTypeDel:
			stateID := tx.Data.(core.Byte128)
			delete(stateMap, stateID)
		default:
			core.Abort("%s is corrupt, invalid transaction type found: %x", filename, tx.TxType)
		}
	}

	for _, s := range stateMap {
		states = append(states, core.DatasetStateEntry{State: s})
	}
	return states
}

// ReadTrnFile reads and validates the transaction log for a dataset.
func (fs *Store) ReadTrnFile(accountNameH core.Byte128, datasetName core.String) []DbTx {
	reader, err := fs.NewTxReader(accountNameH, datasetName)
	core.AbortOn(err)
	defer reader.Close()

	var out []DbTx
	for {
		tx := reader.Next()
		if tx == nil {
			break
		}
		out = append(out, *tx)
	}
	return out
}

// WriteTrnFile truncates and writes a transaction log exclusively.
func (fs *Store) WriteTrnFile(accountNameH core.Byte128, datasetName core.String, txs []DbTx) {
	fs.ensureAccountDir()
	filename := fs.DatasetFilename(accountNameH, datasetName) + DbFileExtensionTransaction
	lock, err := lockablefile.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	core.AbortOn(err)
	defer lock.Close()
	lock.Lock()
	defer lock.Unlock()

	header := dbFileHeader{filetype: DbFileTypeTransaction, version: DbVersion, datasetName: datasetName}
	header.Serialize(lock)
	for _, tx := range txs {
		tx.Serialize(lock)
	}
	// Explicit fsync to persist the rewritten log before releasing the lock.
	core.AbortOn(lock.Sync())
}

// TxReader is a tolerant streaming reader over a .trn file.
type TxReader struct {
	fh *lockablefile.LockableFile // locked file handle
}

// NewTxReader opens and validates a .trn file and returns a reader.
func (fs *Store) NewTxReader(accountNameH core.Byte128, datasetName core.String) (*TxReader, error) {
	filename := fs.DatasetFilename(accountNameH, datasetName) + DbFileExtensionTransaction
	lock, err := lockablefile.Open(filename)
	if err != nil {
		return nil, err
	}

	locked := false
	defer func() {
		if rec := recover(); rec != nil {
			if locked {
				lock.Unlock()
			}
			lock.Close()
			panic(rec)
		}
	}()

	lock.LockShared()
	locked = true

	f := lock
	var header dbFileHeader
	header.Unserialize(f)
	if header.filetype != DbFileTypeTransaction {
		core.Abort("File %s is not a valid transaction file", filename)
	}
	if header.version != DbVersion {
		core.Abort("Unsupported trn file version %x in %s", header.version, filename)
	}
	if header.datasetName != datasetName {
		core.Abort("%s dataset name mismatch in header", filename)
	}
	return &TxReader{fh: lock}, nil
}

// Close closes the reader.
func (r *TxReader) Close() {
	core.ASSERT(r != nil && r.fh != nil, "TxReader.Close on nil handle")
	r.fh.Unlock()
	r.fh.Close()
	r.fh = nil
}

// Seek sets the underlying reader position.
func (r *TxReader) Seek(offset int64, whence int) (int64, error) {
	return r.fh.Seek(offset, whence)
}

// Next returns the next tx or nil on short read/EOF.
func (r *TxReader) Next() *DbTx {
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); !ok || !(errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF)) {
				panic(rec)
			}
		}
	}()
	var tx DbTx
	tx.Unserialize(r.fh)
	return &tx
}

// Pos returns the current offset.
func (r *TxReader) Pos() (int64, error) {
	return r.fh.Seek(0, io.SeekCurrent)
}

// GetDatasetNameFromTransactionFile returns the dataset name and hash stored in a .trn file header.
func (fs *Store) GetDatasetNameFromTransactionFile(filename string) (core.String, core.Byte128) {
	fullpath := filepath.Join(fs.DataDir, "account", filename)
	_, err := os.Stat(fullpath)
	core.AbortOn(err)
	// Open the file, read and check the file headers
	lock, err := lockablefile.Open(fullpath)
	core.AbortOn(err)
	defer lock.Close()
	lock.LockShared()
	defer lock.Unlock()

	var header dbFileHeader
	header.Unserialize(lock)
	if header.filetype != DbFileTypeTransaction {
		core.Abort("File %s is not a valid transaction file", filename)
	}

	datasetName := header.datasetName
	var datasetNameH core.Byte128
	{
		decoded, err := base64.RawURLEncoding.DecodeString(filename[23:45])
		core.AbortOn(err)
		datasetNameH.Set(decoded)
	}
	datasetHashB := core.Hash([]byte(datasetName))
	if datasetHashB.Compare(datasetNameH) != 0 {
		core.Abort("Header for %s does not contain the correct dataset name", filename)
	}
	return datasetName, datasetNameH
}
