//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2016
//	+---+´

package main

import (
	"github.com/fredli74/bytearray"
	"github.com/fredli74/hashbox/core"

	"bytes"
	"crypto/md5"
	_ "encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"testing"
	"time"
)

type FauxServer struct {
	ServerWriter io.Writer
	ServerReader io.Reader
}

func (c FauxServer) Close() error {
	return nil
}
func (c FauxServer) Read(data []byte) (n int, err error)  { return c.ServerReader.Read(data) }
func (c FauxServer) Write(data []byte) (n int, err error) { return c.ServerWriter.Write(data) }

func TestCreatingTestServer(t *testing.T) {
	datDirectory = "./test" // YES! It is a global, and it is ugly, but so are yoU!
	idxDirectory = "./test"
	os.Mkdir("./test", 0777)
	os.Mkdir("./test/account", 0777)
	accountHandler = NewAccountHandler()
	storageHandler = NewStorageHandler()
	/*
		k := core.Hash([]byte(""))
		a := AccountInfo{AccountName: "test", AccountKey: k[:]}
		J, err := json.MarshalIndent(a, "", "\t")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(J))
	*/
	go func() {
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{Port: 1248})
		if err != nil {
			panic(err)
		}
		defer listener.Close()

		connectionListener(listener)
	}()
}

func TestServerTestAccount(t *testing.T) {
	A := AccountInfo{
		AccountName: "test account",
		AccessKey:   core.DeepHmac(20000, append([]byte("test account"), []byte("*ACCESS*KEY*PAD*")...), core.Hash([]byte("password"))),
		Datasets:    core.DatasetArray{},
	}
	accountHandler.SetInfo(A)
	JA, _ := json.Marshal(A)

	B := accountHandler.GetInfo(core.Hash([]byte(A.AccountName)))
	JB, _ := json.Marshal(*B)

	if B == nil {
		t.Error("Account not found")
	} else if string(JA) != string(JB) {
		fmt.Println(string(JA))
		fmt.Println(string(JB))
		t.Error("Accounts not equal")
	}

	C := accountHandler.GetInfo(core.Hash([]byte("")))
	if C != nil {
		t.Error("Account without name found")
	}
}

func TestClientServerAuthentication(t *testing.T) {
	var err error

	conn, err := net.Dial("tcp", "127.0.0.1:1248")
	if err != nil {
		panic(err)
	}

	conn.SetDeadline(time.Now().Add(15 * time.Second))
	client := core.NewClient(conn, "test account", core.DeepHmac(20000, append([]byte("test account"), []byte("*ACCESS*KEY*PAD*")...), core.Hash([]byte("password"))))
	defer client.Close(true)
}

func randomByte128() (b core.Byte128) {
	for i := 0; i < 16; i++ {
		b[i] = byte(rand.Uint32())
	}
	return
}

func TestClientServerDataset(t *testing.T) {
	var err error

	conn, err := net.Dial("tcp", "127.0.0.1:1248")
	if err != nil {
		panic(err)
	}

	conn.SetDeadline(time.Now().Add(10 * time.Minute))
	client := core.NewClient(conn, "test account", core.DeepHmac(20000, append([]byte("test account"), []byte("*ACCESS*KEY*PAD*")...), core.Hash([]byte("password"))))
	defer client.Close(true)

	// First make sure there is some data to reference
	var data bytearray.ByteArray
	block := core.NewHashboxBlock(core.BlockDataTypeZlib, data, nil)
	var blockID core.Byte128
	blockID = client.StoreBlock(block)
	client.Commit()

	var array core.DatasetStateArray
	var stateID core.Byte128 // randomByte128()

	copy(stateID[:], []byte("testC"))
	keepstate := core.DatasetState{StateID: stateID, BlockID: blockID, Size: 5, UniqueSize: 42}

	client.AddDatasetState("testset", keepstate)
	array = append(array, core.DatasetStateEntry{State: keepstate})
	copy(stateID[:], []byte("testD"))
	keepstate = core.DatasetState{StateID: stateID, BlockID: blockID, Size: 125, UniqueSize: 33342}
	client.AddDatasetState("testset", keepstate)
	array = append(array, core.DatasetStateEntry{State: keepstate})

	copy(stateID[:], []byte("testA"))
	client.AddDatasetState("testset", core.DatasetState{StateID: stateID, BlockID: blockID, Size: 5, UniqueSize: 42})
	copy(stateID[:], []byte("testB"))
	client.AddDatasetState("testset", core.DatasetState{StateID: stateID, BlockID: blockID, Size: 5, UniqueSize: 42})
	copy(stateID[:], []byte("testB"))
	client.RemoveDatasetState("testset", stateID)
	copy(stateID[:], []byte("testA"))
	client.RemoveDatasetState("testset", stateID)
	client.RemoveDatasetState("testset", stateID)

	hash := md5.New()
	array[0].Serialize(hash)
	array[1].Serialize(hash)
	var localHash core.Byte128
	copy(localHash[:], hash.Sum(nil)[:16])

	info := client.GetAccountInfo()
	if reflect.TypeOf(info).String() != "*core.MsgServerAccountInfo" {
		t.Error("Invalid response to client.GetAccountInfo")
	}
	if len(info.DatasetList) != 1 {
		t.Error("More than one dataset was found under test account")
	} else {
		if info.DatasetList[0].Name != "testset" {
			t.Error("First dataset is not named \"testset\"")
		}
		if fmt.Sprintf("%x", info.DatasetList[0].ListH) != fmt.Sprintf("%x", localHash) {
			t.Error(fmt.Sprintf("%x != %x", info.DatasetList[0].ListH, localHash))
			t.Error("List hash could not be verified")
		}
		if info.DatasetList[0].Size != 33509 {
			t.Error(fmt.Sprintf("Wrong total size for the dataset %d != %d", info.DatasetList[0].Size, 33509))
		}
	}

	list := client.ListDataset("testset")
	if list == nil {
		t.Error("Dataset \"testset\" was not found")
	} else if !reflect.DeepEqual(array, list.States) {
		fmt.Println(array)
		fmt.Println(list.States)
		t.Error("Local list and remote list not equal")
	}
}

func TestClientServerHashboxBlocks(t *testing.T) {
	var err error

	conn, err := net.Dial("tcp", "127.0.0.1:1248")
	if err != nil {
		panic(err)
	}

	conn.SetDeadline(time.Now().Add(10 * time.Minute))
	client := core.NewClient(conn, "test account", core.DeepHmac(20000, append([]byte("test account"), []byte("*ACCESS*KEY*PAD*")...), core.Hash([]byte("password"))))
	defer client.Close(true)

	{
		var data bytearray.ByteArray
		data.Write([]byte("HELLO!"))
		blockID := client.StoreData(core.BlockDataTypeRaw, data, nil)
		client.Commit()
		block := client.ReadBlock(blockID)
		if !reflect.DeepEqual(blockID, block.BlockID) {
			t.Error("Received block has wrong ID")
		}

		rdata, _ := block.Data.ReadSlice()
		if !bytes.Equal(rdata, []byte("HELLO!")) {
			t.Error("Block contains wrong data, expected \"HELLO!\" received \"" + string(rdata) + "\"")
		}
	}

	{
		var data bytearray.ByteArray
		data.Write([]byte("Bet it all on black?"))
		blockID := client.StoreData(core.BlockDataTypeRaw, data, nil)
		client.Commit()
		block := client.ReadBlock(blockID)
		if !reflect.DeepEqual(blockID, block.BlockID) {
			t.Error("Received block has wrong ID")
		}
		rdata, _ := block.Data.ReadSlice()
		if !bytes.Equal(rdata, []byte("Bet it all on black?")) {
			t.Error("Block contains wrong data, expected \"Bet it all on black?\" received \"" + string(rdata) + "\"")
		}
	}
}

func TestClosingTestServer(t *testing.T) {
	accountHandler.Close()
	storageHandler.Close()
}
