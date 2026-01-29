//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	_ "io"
	"math/big"
	_ "os"
	"reflect"
	"testing"
)

func randomUint32() Uint32 {
	var b [4]byte
	_, err := rand.Read(b[:])
	AbortOnError(err, "rand.Read: %v", err)
	return Uint32(binary.BigEndian.Uint32(b[:]))
}
func randomInt63() int64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	AbortOnError(err, "rand.Read: %v", err)
	v := int64(binary.BigEndian.Uint64(b[:]) & ^(uint64(1) << 63))
	return v
}
func randomUint8() uint8 {
	var b [1]byte
	_, err := rand.Read(b[:])
	AbortOnError(err, "rand.Read: %v", err)
	return b[0]
}
func randomByte128() (b Byte128) {
	_, err := rand.Read(b[:])
	AbortOnError(err, "rand.Read: %v", err)
	return
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomMsgString() String {
	n, err := rand.Int(rand.Reader, big.NewInt(32))
	AbortOnError(err, "rand.Int: %v", err)
	b := make([]byte, int(n.Int64())+8)
	for i := range b {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(letterBytes))))
		AbortOnError(err, "rand.Int: %v", err)
		b[i] = letterBytes[idx.Int64()]
	}
	return String("RandomString>" + string(b) + "<")
}

func randomHashboxBlock() (b HashboxBlock) {
	b.DataType = BlockDataTypeRaw
	_, err := b.Data.Write([]byte(randomMsgString()))
	AbortOnError(err, "b.Data.Write: %v", err)
	b.Links = append(b.Links, randomByte128())
	b.UncompressedSize = -1
	b.BlockID = b.HashData()
	return b
}
func protocolPipeCompare(msgType uint32, msgData interface{}) (isEqual bool, dump string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			isEqual = false // this actually sets the return value named isEqual
		}
	}()
	buf := new(bytes.Buffer)
	num, err := rand.Int(rand.Reader, big.NewInt(1<<16))
	AbortOnError(err, "rand.Int: %v", err)
	msg := &ProtocolMessage{Num: uint16(num.Int64()), Type: msgType, Data: msgData}
	WriteMessage(buf, msg)
	dump = hex.Dump(buf.Bytes())
	R := ReadMessage(buf)
	switch d := R.Data.(type) {
	case *MsgClientWriteBlock:
		isEqual = reflect.TypeOf(msg) == reflect.TypeOf(R) && d.Block.VerifyBlock()
	case *MsgServerWriteBlock:
		isEqual = reflect.TypeOf(msg) == reflect.TypeOf(R) && d.Block.VerifyBlock()
	default:
		isEqual = reflect.TypeOf(msg) == reflect.TypeOf(R) && reflect.DeepEqual(msg, reflect.ValueOf(R).Interface())
	}

	if !isEqual {
		J1, _ := json.Marshal(msg)
		dump += ">" + reflect.TypeOf(msg).String() + ":" + string(J1)
		J2, _ := json.Marshal(reflect.ValueOf(R).Interface())
		dump += "\n<" + reflect.TypeOf(R).String() + ":" + string(J2)
	}
	return isEqual, dump
}

func TestMessageSerialization(t *testing.T) {
	if ok, dump := protocolPipeCompare(MsgTypeGreeting, &MsgClientGreeting{randomUint32()}); !ok {
		t.Errorf("MsgClientGreeting did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientGreeting passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeGreeting&MsgTypeServerMask, &MsgServerGreeting{randomByte128()}); !ok {
		t.Errorf("MsgServerGreeting did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerGreeting passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAuthenticate, &MsgClientAuthenticate{randomByte128(), randomByte128()}); !ok {
		t.Errorf("MsgClientAuthenticate did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientAuthenticate passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAuthenticate&MsgTypeServerMask, nil); !ok {
		t.Errorf("MsgServerAuthenticate did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerAuthenticate passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeGoodbye, nil); !ok {
		t.Errorf("MsgClientGoodbye did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientGoodbye passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeGoodbye&MsgTypeServerMask, nil); !ok {
		t.Errorf("MsgServerGoodbye did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerGoodbye passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeError&MsgTypeServerMask, &MsgServerError{randomMsgString()}); !ok {
		t.Errorf("MsgServerError did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerError passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAllocateBlock, &MsgClientAllocateBlock{randomByte128()}); !ok {
		t.Errorf("MsgClientAllocateBlock did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientAllocateBlock passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAcknowledgeBlock&MsgTypeServerMask, &MsgServerAcknowledgeBlock{randomByte128()}); !ok {
		t.Errorf("MsgServerAcknowledgeBlock did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerAcknowledgeBlock passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeReadBlock&MsgTypeServerMask, &MsgServerReadBlock{randomByte128()}); !ok {
		t.Errorf("MsgServerReadBlock did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerReadBlock passed:\n%s", dump)
		}
	}
	block := randomHashboxBlock()
	if ok, dump := protocolPipeCompare(MsgTypeWriteBlock, &MsgClientWriteBlock{&block}); !ok {
		t.Errorf("MsgClientWriteBlock did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientWriteBlock passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeReadBlock, &MsgClientReadBlock{randomByte128()}); !ok {
		t.Errorf("MsgClientReadBlock did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientReadBlock passed:\n%s", dump)
		}
	}

	block = randomHashboxBlock()
	if ok, dump := protocolPipeCompare(MsgTypeWriteBlock&MsgTypeServerMask, &MsgServerWriteBlock{&block}); !ok {
		t.Errorf("MsgServerWriteBlock did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerWriteBlock passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAccountInfo, &MsgClientAccountInfo{randomByte128()}); !ok {
		t.Errorf("MsgClientAccountInfo did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientAccountInfo passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAccountInfo&MsgTypeServerMask, &MsgServerAccountInfo{[]Dataset{Dataset{Name: randomMsgString(), Size: randomInt63(), ListH: randomByte128()}}}); !ok {
		t.Errorf("MsgServerAccountInfo did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerAccountInfo passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeListDataset, &MsgClientListDataset{AccountNameH: randomByte128(), DatasetName: randomMsgString()}); !ok {
		t.Errorf("MsgClientListDataset did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientListDataset passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeListDataset&MsgTypeServerMask, &MsgServerListDataset{
		States: DatasetStateArray{
			DatasetStateEntry{
				StateFlags: randomUint8(),
				State: DatasetState{
					StateID:    randomByte128(),
					BlockID:    randomByte128(),
					Size:       randomInt63(),
					UniqueSize: randomInt63(),
				},
			},
		},
		ListH: randomByte128()}); !ok {
		t.Errorf("MsgServerListDataset did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerListDataset passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAddDatasetState, &MsgClientAddDatasetState{AccountNameH: randomByte128(), DatasetName: randomMsgString(), State: DatasetState{StateID: randomByte128(), BlockID: randomByte128(), Size: randomInt63(), UniqueSize: randomInt63()}}); !ok {
		t.Errorf("MsgClientAddDatasetState did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientAddDatasetState passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeAddDatasetState&MsgTypeServerMask, nil); !ok {
		t.Errorf("MsgServerAddDatasetState did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerAddDatasetState passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeRemoveDatasetState, &MsgClientRemoveDatasetState{AccountNameH: randomByte128(), DatasetName: randomMsgString(), StateID: randomByte128()}); !ok {
		t.Errorf("MsgClientRemoveDatasetState did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgClientRemoveDatasetState passed:\n%s", dump)
		}
	}

	if ok, dump := protocolPipeCompare(MsgTypeRemoveDatasetState&MsgTypeServerMask, nil); !ok {
		t.Errorf("MsgServerRemoveDatasetState did not pass serialization / deserialization test:\n%s", dump)
	} else {
		if testing.Verbose() {
			t.Logf("MsgServerRemoveDatasetState passed:\n%s", dump)
		}
	}

}
