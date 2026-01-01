//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package accountdb

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/fredli74/hashbox/pkg/core"
)

func newTestStore(t *testing.T) (*Store, core.Byte128) {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "account"), 0o755); err != nil {
		t.Fatalf("create account dir: %v", err)
	}
	return NewStore(dir), core.Hash([]byte("account"))
}

func makeState(label string, size, unique int64) core.DatasetState {
	return core.DatasetState{
		StateID:    core.Hash([]byte("state-" + label)),
		BlockID:    core.Hash([]byte("block-" + label)),
		Size:       size,
		UniqueSize: unique,
	}
}

// populateCollectionMeta mirrors DBStateCollection.Unserialize side effects so we can
// reuse the computed Size and ListH in tests that write .db files.
func populateCollectionMeta(c *DBStateCollection) {
	var buf bytes.Buffer
	c.Serialize(&buf)
	var tmp DBStateCollection
	tmp.Unserialize(&buf)
	c.Size = tmp.Size
	c.ListH = tmp.ListH
}

func TestStateArrayFromTransactions(t *testing.T) {
	store, account := newTestStore(t)
	dataset := core.String("photos")

	state1 := makeState("one", 10, 4)
	state2 := makeState("two", 20, 8)

	store.AppendTx(account, dataset, DbTx{Timestamp: 1, TxType: DbTxTypeAdd, Data: state1})
	store.AppendTx(account, dataset, DbTx{Timestamp: 2, TxType: DbTxTypeAdd, Data: state2})
	store.AppendTx(account, dataset, DbTx{Timestamp: 3, TxType: DbTxTypeDel, Data: state1.StateID})

	states := store.StateArrayFromTransactions(account, dataset)
	if len(states) != 1 {
		t.Fatalf("expected 1 state after replay, got %d", len(states))
	}
	if got := states[0].State; got != state2 {
		t.Fatalf("unexpected state: got %+v want %+v", got, state2)
	}
}

func TestTxReaderStopsOnTruncatedEntry(t *testing.T) {
	store, account := newTestStore(t)
	dataset := core.String("docs")

	state1 := makeState("first", 5, 5)
	state2 := makeState("second", 6, 6)

	store.AppendTx(account, dataset, DbTx{Timestamp: 1, TxType: DbTxTypeAdd, Data: state1})
	store.AppendTx(account, dataset, DbTx{Timestamp: 2, TxType: DbTxTypeAdd, Data: state2})

	filename := store.datasetFilename(account, string(dataset)) + DbFileExtensionTransaction
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("stat tx file: %v", err)
	}
	if err := os.Truncate(filename, info.Size()-4); err != nil { // chop a few bytes off the last tx
		t.Fatalf("truncate: %v", err)
	}

	r, err := store.NewTxReader(account, dataset)
	if err != nil {
		t.Fatalf("NewTxReader: %v", err)
	}
	defer r.Close()

	tx := r.Next()
	if tx == nil || tx.TxType != DbTxTypeAdd {
		t.Fatalf("unexpected first tx: %+v", tx)
	}
	if s, ok := tx.Data.(core.DatasetState); !ok || s != state1 {
		t.Fatalf("unexpected first tx: %+v", tx)
	}

	// Second transaction is truncated; reader should stop without error.
	tx = r.Next()
	if tx != nil {
		t.Fatalf("expected nil on truncated tx, got %+v", tx)
	}
}

func TestWriteDBFileUpdatesInfoAndRoundTrips(t *testing.T) {
	store, account := newTestStore(t)
	dataset := core.String("photos")

	// Seed info file with a placeholder dataset to ensure sorting/replacement works.
	seedInfo := AccountInfo{
		AccountName: core.String("alice"),
		AccessKey:   core.Hash([]byte("key")),
		Datasets: core.DatasetArray{
			{Name: core.String("archive"), Size: 123, ListH: core.Hash([]byte("archive"))},
			{Name: dataset, Size: 1, ListH: core.Hash([]byte("stale"))},
		},
	}
	store.WriteInfoFile(account, seedInfo)

	collection := &DBStateCollection{
		States: core.DatasetStateArray{
			{State: makeState("a", 10, 4)},
			{State: makeState("b", 20, 5)},
		},
	}
	populateCollectionMeta(collection)

	store.WriteDBFile(account, dataset, collection)

	readCollection := store.ReadDBFile(account, dataset)
	if readCollection == nil {
		t.Fatalf("ReadDBFile: got nil")
	}
	if len(readCollection.States) != len(collection.States) {
		t.Fatalf("state count mismatch: got %d want %d", len(readCollection.States), len(collection.States))
	}
	for i := range collection.States {
		if readCollection.States[i].State != collection.States[i].State {
			t.Fatalf("state[%d] mismatch: got %+v want %+v", i, readCollection.States[i].State, collection.States[i].State)
		}
	}
	if readCollection.Size != collection.Size || readCollection.ListH != collection.ListH {
		t.Fatalf("collection meta mismatch: got size=%d listH=%x, want size=%d listH=%x", readCollection.Size, readCollection.ListH, collection.Size, collection.ListH)
	}

	info := store.ReadInfoFile(account)
	if info == nil {
		t.Fatalf("ReadInfoFile: got nil")
	}
	if len(info.Datasets) != 2 {
		t.Fatalf("expected 2 datasets in info, got %d", len(info.Datasets))
	}
	var archiveSeen, photosSeen bool
	for _, ds := range info.Datasets {
		switch ds.Name {
		case core.String("archive"):
			archiveSeen = true
		case dataset:
			photosSeen = true
			if ds.Size != collection.Size || ds.ListH != collection.ListH {
				t.Fatalf("info dataset meta mismatch: got size=%d listH=%x, want size=%d listH=%x", ds.Size, ds.ListH, collection.Size, collection.ListH)
			}
		}
	}
	if !archiveSeen || !photosSeen {
		t.Fatalf("unexpected datasets in info: %+v", info.Datasets)
	}
}

func TestWriteDBFileRemovesDatasetFromInfo(t *testing.T) {
	store, account := newTestStore(t)
	dataset := core.String("empty")

	info := AccountInfo{
		AccountName: core.String("bob"),
		AccessKey:   core.Byte128{},
		Datasets: core.DatasetArray{
			{Name: dataset, Size: 10, ListH: core.Hash([]byte("old"))},
			{Name: core.String("keep"), Size: 5, ListH: core.Hash([]byte("keep"))},
		},
	}
	store.WriteInfoFile(account, info)

	store.WriteDBFile(account, dataset, &DBStateCollection{})

	updated := store.ReadInfoFile(account)
	if updated == nil {
		t.Fatalf("ReadInfoFile: got nil")
	}
	if len(updated.Datasets) != 1 || updated.Datasets[0].Name != core.String("keep") {
		t.Fatalf("expected only remaining dataset 'keep', got %+v", updated.Datasets)
	}
}
