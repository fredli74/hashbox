package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fredli74/bytearray"
	"github.com/fredli74/hashbox/pkg/accountdb"
	"github.com/fredli74/hashbox/pkg/core"
	"github.com/fredli74/hashbox/pkg/storagedb"
)

func TestListAccountsIncludesHashAndName(t *testing.T) {
	tmp := t.TempDir()

	accName := core.String("alice")
	accHash := core.Hash([]byte(accName))
	store := accountdb.NewStore(tmp)
	store.WriteInfoFile(accHash, accountdb.AccountInfo{AccountName: accName})

	cmds := newCommandSet(tmp, filepath.Join(tmp, "index"))
	out := captureOutput(t, func() {
		cmds.listAccounts()
	})

	if !strings.Contains(out, "["+formatHash(accHash)+"]") || !strings.Contains(out, string(accName)) {
		t.Fatalf("output missing account hash/name: %s", out)
	}
}

func TestListDatasetsAndStates(t *testing.T) {
	tmp := t.TempDir()

	accName := core.String("alice")
	accHash := core.Hash([]byte(accName))
	store := accountdb.NewStore(tmp)
	store.WriteInfoFile(accHash, accountdb.AccountInfo{AccountName: accName})

	dataset := core.String("photos")
	state1 := core.DatasetState{
		StateID:    core.Hash([]byte("state-a")),
		BlockID:    core.Hash([]byte("block-a")),
		Size:       10,
		UniqueSize: 5,
	}
	state2 := core.DatasetState{
		StateID:    core.Hash([]byte("state-b")),
		BlockID:    core.Hash([]byte("block-b")),
		Size:       20,
		UniqueSize: 8,
	}

	appendState(t, store, accHash, dataset, state1)
	appendState(t, store, accHash, dataset, state2)
	store.RebuildDB(accHash, dataset)

	cmds := newCommandSet(tmp, filepath.Join(tmp, "index"))
	out := captureOutput(t, func() {
		cmds.listDatasets(string(accName))
	})
	if !strings.Contains(out, "states=2") || !strings.Contains(out, "size=33B") {
		t.Fatalf("unexpected dataset output: %s", out)
	}
	if !strings.Contains(out, "["+formatHash(core.Hash([]byte(dataset)))+"]") {
		t.Fatalf("missing dataset hash: %s", out)
	}

	stateOut := captureOutput(t, func() {
		cmds.listStates(string(accName), string(dataset))
	})
	if !strings.Contains(stateOut, "size=10B") || !strings.Contains(stateOut, "size=20B") {
		t.Fatalf("unexpected state output: %s", stateOut)
	}
	if !strings.Contains(stateOut, fmt.Sprintf("%x", state1.BlockID[:])) || !strings.Contains(stateOut, fmt.Sprintf("%x", state2.BlockID[:])) {
		t.Fatalf("missing roots in state output: %s", stateOut)
	}
}

func TestShowBlock(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	idxDir := filepath.Join(t.TempDir(), "index")
	requireDirs(t, dataDir)
	requireDirs(t, idxDir)

	store := storagedb.NewStore(dataDir, idxDir)
	var payload bytes.Buffer
	payload.WriteString("hello")
	block := core.NewHashboxBlock(core.BlockDataTypeRaw, bytearrayFromBytes(t, payload.Bytes()), nil)
	if ok := store.WriteBlock(block); !ok {
		t.Fatalf("failed to write block")
	}
	store.SyncAll()

	cmds := newCommandSet(dataDir, idxDir)
	out := captureOutput(t, func() {
		cmds.showBlock(fmtHash(block.BlockID[:]), false)
	})
	if !strings.Contains(out, "Size: 5B (raw data)") {
		t.Fatalf("unexpected show-block output: %s", out)
	}

	verifyOut := captureOutput(t, func() {
		cmds.showBlock(fmtHash(block.BlockID[:]), true)
	})
	if !strings.Contains(verifyOut, "Verify: OK") {
		t.Fatalf("unexpected show-block verify output: %s", verifyOut)
	}
}

// Helpers
func captureOutput(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	fn()
	w.Close()
	os.Stdout = orig
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	return buf.String()
}

func requireDirs(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func appendState(t *testing.T, store *accountdb.Store, acc core.Byte128, dataset core.String, state core.DatasetState) {
	t.Helper()
	store.AppendAddState(acc, dataset, state)
}

func bytearrayFromBytes(t *testing.T, b []byte) bytearray.ByteArray {
	t.Helper()
	var ba bytearray.ByteArray
	if _, err := ba.Write(b); err != nil {
		t.Fatal(err)
	}
	return ba
}

func fmtHash(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
