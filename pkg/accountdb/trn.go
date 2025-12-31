//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2025
//	+---+´

package accountdb

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/hashbox/pkg/lockablefile"
)

type DBTx struct {
	timestamp int64
	txType    uint32
	data      interface{}
}

func (t *DBTx) Serialize(w io.Writer) {
	core.WriteInt64(w, t.timestamp)
	core.WriteUint32(w, t.txType)
	t.data.(core.Serializer).Serialize(w)
}
func (t *DBTx) Unserialize(r io.Reader) {
	core.ReadInt64(r, &t.timestamp)
	core.ReadUint32(r, &t.txType)
	switch t.txType {
	case dbTxTypeAdd:
		var s core.DatasetState
		s.Unserialize(r)
		t.data = s
	case dbTxTypeDel:
		var s core.Byte128
		s.Unserialize(r)
		t.data = s
	default:
		core.Abort("Corrupt transaction file")
	}
}

// AppendTx appends a transaction to the dataset log.
func (fs *Store) AppendTx(accountNameH core.Byte128, datasetName core.String, tx DBTx) error {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
	lock, err := lockablefile.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	core.AbortOn(err)
	defer lock.Close()
	core.AbortOn(lock.Lock())
	defer lock.Unlock()

	file := lock.File()

	pos, _ := file.Seek(0, 2)
	if pos == 0 { // New file, write the header
		header := dbFileHeader{filetype: dbFileTypeTransaction, version: dbVersion, datasetName: datasetName}
		header.Serialize(file)
	}
	tx.Serialize(file)
	return nil
}

// AppendAddState appends an add tx with current timestamp.
func (fs *Store) AppendAddState(accountNameH core.Byte128, datasetName core.String, state core.DatasetState) error {
	return fs.AppendTx(accountNameH, datasetName, DBTx{timestamp: time.Now().UnixNano(), txType: dbTxTypeAdd, data: state})
}

// AppendDelState appends a delete tx with current timestamp.
func (fs *Store) AppendDelState(accountNameH core.Byte128, datasetName core.String, stateID core.Byte128) error {
	return fs.AppendTx(accountNameH, datasetName, DBTx{timestamp: time.Now().UnixNano(), txType: dbTxTypeDel, data: stateID})
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
		if tx.timestamp < pointInHistory {
			core.Abort("%s is corrupt, timestamp check failed (%x < %x)", filename, tx.timestamp, pointInHistory)
		}
		pointInHistory = tx.timestamp
		switch tx.txType {
		case dbTxTypeAdd:
			stateMap[tx.data.(core.DatasetState).StateID] = tx.data.(core.DatasetState)
		case dbTxTypeDel:
			delete(stateMap, tx.data.(core.Byte128))
		default:
			core.Abort("%s is corrupt, invalid transaction type found: %x", filename, tx.txType)
		}
	}

	for _, s := range stateMap {
		states = append(states, core.DatasetStateEntry{State: s})
	}
	return states
}

// TxReader is a tolerant streaming reader over a .trn file.
type TxReader struct {
	fh *lockablefile.LockableFile // locked file handle
}

// NewTxReader opens and validates a .trn file and returns a reader.
func (fs *Store) NewTxReader(accountNameH core.Byte128, datasetName core.String) (*TxReader, error) {
	filename := fs.datasetFilename(accountNameH, string(datasetName)) + dbFileExtensionTransaction
	if _, err := os.Stat(filename); err != nil {
		return nil, err
	}
	lock, err := lockablefile.Open(filename)
	if err != nil {
		return nil, err
	}
	if err := lock.LockShared(); err != nil {
		lock.Close()
		return nil, err
	}
	file := lock.File()
	var header dbFileHeader
	header.Unserialize(file)
	if header.filetype != dbFileTypeTransaction {
		lock.Unlock()
		lock.Close()
		return nil, fmt.Errorf("%s is not a valid transaction file", filename)
	}
	if header.version != dbVersion {
		lock.Unlock()
		lock.Close()
		return nil, fmt.Errorf("%s has unsupported version %d (expected %d)", filename, header.version, dbVersion)
	}
	if header.datasetName != datasetName {
		lock.Unlock()
		lock.Close()
		return nil, fmt.Errorf("%s dataset name mismatch in header", filename)
	}
	return &TxReader{fh: lock}, nil
}

// Close closes the reader.
func (r *TxReader) Close() error {
	if r == nil || r.fh == nil {
		return nil
	}
	r.fh.Unlock()
	err := r.fh.Close()
	r.fh = nil
	return err
}

// Seek sets the underlying reader position.
func (r *TxReader) Seek(offset int64, whence int) (int64, error) {
	return r.fh.File().Seek(offset, whence)
}

// Pos returns the current reader offset.
func (r *TxReader) Pos() (int64, error) {
	return r.fh.File().Seek(0, io.SeekCurrent)
}

// Next returns the next tx or nil on short read/EOF.
func (r *TxReader) Next() *DBTx {
	var tx DBTx
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); !ok || !(errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF)) {
				panic(rec)
			}
		}
	}()
	tx.Unserialize(r.fh.File())
	return &tx
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
	core.AbortOn(lock.LockShared())
	defer lock.Unlock()
	fil := lock.File()

	var header dbFileHeader
	header.Unserialize(fil)
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
