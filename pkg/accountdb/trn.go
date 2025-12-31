//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
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

type DBTx struct {
	Timestamp int64
	TxType    uint32
	Data      interface{}
}

func (t *DBTx) Serialize(w io.Writer) {
	core.WriteInt64(w, t.Timestamp)
	core.WriteUint32(w, t.TxType)
	t.Data.(core.Serializer).Serialize(w)
}
func (t *DBTx) Unserialize(r io.Reader) {
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
func (fs *Store) AppendTx(accountNameH core.Byte128, datasetName core.String, tx DBTx) {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
	lock, err := lockablefile.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	core.AbortOn(err)
	defer lock.Close()
	lock.Lock()
	defer lock.Unlock()

	file := lock.File()

	pos, err := file.Seek(0, io.SeekEnd)
	core.AbortOn(err)
	if pos == 0 { // New file, write the header
		header := dbFileHeader{filetype: dbFileTypeTransaction, version: dbVersion, datasetName: datasetName}
		header.Serialize(file)
	}
	tx.Serialize(file)
	// Explicit fsync to persist the append before releasing the lock; Close alone
	// does not guarantee durability.
	core.AbortOn(file.Sync())
}

// AppendAddState appends an add tx with current timestamp.
func (fs *Store) AppendAddState(accountNameH core.Byte128, datasetName core.String, state core.DatasetState) {
	fs.AppendTx(accountNameH, datasetName, DBTx{Timestamp: time.Now().UnixNano(), TxType: DbTxTypeAdd, Data: state})
}

// AppendDelState appends a delete tx with current timestamp.
func (fs *Store) AppendDelState(accountNameH core.Byte128, datasetName core.String, stateID core.Byte128) {
	fs.AppendTx(accountNameH, datasetName, DBTx{Timestamp: time.Now().UnixNano(), TxType: DbTxTypeDel, Data: stateID})
}

// StateArrayFromTransactions returns current live states by replaying the transaction log.
func (fs *Store) StateArrayFromTransactions(accountNameH core.Byte128, datasetName core.String) (states core.DatasetStateArray) {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
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

// ReadTRNFile reads and validates the transaction log for a dataset.
func (fs *Store) ReadTRNFile(accountNameH core.Byte128, datasetName core.String) []DBTx {
	reader, err := fs.NewTxReader(accountNameH, datasetName)
	core.AbortOn(err)
	defer reader.Close()

	var out []DBTx
	for {
		tx := reader.Next()
		if tx == nil {
			break
		}
		out = append(out, *tx)
	}
	return out
}

// WriteTRNFile truncates and writes a transaction log exclusively.
func (fs *Store) WriteTRNFile(accountNameH core.Byte128, datasetName core.String, txs []DBTx) {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
	lock, err := lockablefile.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	core.AbortOn(err)
	defer lock.Close()
	lock.Lock()
	defer lock.Unlock()

	f := lock.File()
	header := dbFileHeader{filetype: dbFileTypeTransaction, version: dbVersion, datasetName: datasetName}
	header.Serialize(f)
	for _, tx := range txs {
		tx.Serialize(f)
	}
	// Explicit fsync to persist the rewritten log before releasing the lock.
	core.AbortOn(f.Sync())
}

// TxReader is a tolerant streaming reader over a .trn file.
type TxReader struct {
	fh *lockablefile.LockableFile // locked file handle
}

// NewTxReader opens and validates a .trn file and returns a reader.
func (fs *Store) NewTxReader(accountNameH core.Byte128, datasetName core.String) (*TxReader, error) {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
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

	f := lock.File()
	var header dbFileHeader
	header.Unserialize(f)
	if header.filetype != dbFileTypeTransaction {
		core.Abort("File %s is not a valid transaction file", filename)
	}
	if header.version != dbVersion {
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
	return r.fh.File().Seek(offset, whence)
}

// Next returns the next tx or nil on short read/EOF.
func (r *TxReader) Next() *DBTx {
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); !ok || !(errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF)) {
				panic(rec)
			}
		}
	}()
	var tx DBTx
	tx.Unserialize(r.fh.File())
	return &tx
}

// Pos returns the current offset.
func (r *TxReader) Pos() (int64, error) {
	return r.fh.File().Seek(0, io.SeekCurrent)
}

// GetDatasetNameFromTransactionFile returns the dataset name stored in a .trn file header.
func (fs *Store) GetDatasetNameFromTransactionFile(filename string) core.String {
	fullpath := filepath.Join(fs.DataDir, "account", filename)
	_, err := os.Stat(fullpath)
	core.AbortOn(err)
	// Open the file, read and check the file headers
	lock, err := lockablefile.Open(fullpath)
	core.AbortOn(err)
	defer lock.Close()
	lock.LockShared()
	defer lock.Unlock()
	f := lock.File()

	var header dbFileHeader
	header.Unserialize(f)
	if header.filetype != dbFileTypeTransaction {
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
	return datasetName
}
