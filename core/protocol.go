//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015
//	+---+´

// Hashbox core client routines
package core

//
// protocol.go source contains client-server protocol definitions, all message types can be found here
//

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"time"
)

type Session struct {
	AccountNameH Byte128 // = md5( accountName )
	SessionNonce Byte128 // Unique nonce for the session, based on uint64(servertime) + uint64(random)
	SessionKey   Byte128 // = hmac^20000( AccountNameH SessionNonce, AccessKey)
}

func (s *Session) GenerateSessionKey(AccessKey Byte128) {
	s.SessionKey = DeepHmac(20000, append(s.AccountNameH[:], s.SessionNonce[:]...), AccessKey)
}

// Hashbox Protocol
// ================
//
//  ->  (Sent to server)
//  <-  (Replied by server)
//

const (
	MsgTypeGreeting     uint32 = 0x686F6C61 // = "hola"
	MsgTypeAuthenticate uint32 = 0x61757468 // = "auth"
	MsgTypeGoodbye      uint32 = 0x71756974 // = "quit"

	MsgTypeAllocateBlock    uint32 = 0x616C6C6F // = "allo"
	MsgTypeReadBlock        uint32 = 0x72656164 // = "read"
	MsgTypeWriteBlock       uint32 = 0x77726974 // = "writ"
	MsgTypeAcknowledgeBlock uint32 = 0x61636B6E // = "ackn"

	MsgTypeAccountInfo        uint32 = 0x696E666F // = "info"
	MsgTypeAddDatasetState    uint32 = 0x61646473 // = "adds"
	MsgTypeListDataset        uint32 = 0x6C697374 // = "list"
	MsgTypeRemoveDatasetState uint32 = 0x64656C73 // = "dels"

	MsgTypeError uint32 = 0x65727273 // = "ERRS"
)

// We use the same function for reading server and client messages but they do have different payloads
// so we need to differentiate between the two message types by using  lowercase / uppercase.
// We do this simply by removing the lowercase bit in the ASCII table by using AND on 0xDf
const MsgTypeServerMask uint32 = 0xDFDFDFDF // MsgType & MsgTypeClientMask

// If we would have needed to convert a server message type to client message type, we could OR the bit in again
const MsgTypeClientMask uint32 = 0x20202020 // MsgType | MsgTypeClientMask

//
// Unauthenticated commands
// ------------------------
//

// MsgClientAuthenticate sent from the client to authenticate the session.
type MsgClientAuthenticate struct {
	AccountNameH    Byte128 // = md5(accountName)
	AuthenticationH Byte128 // = hmac(AccountNameH, Session.SessionKey)
}

// MsgServerGreeting sent from server in response to client greeting.
type MsgServerGreeting struct { // <- "HOLA"
	SessionNonce Byte128 // Unique nonce for the session
}

// MsgServerError is sent on all kind of severe protocol / server errors. The ErrorMessage contains more details regarding the error.
type MsgServerError struct { // <- "ERR!"
	ErrorMessage String // Error message
}

//
// Authenticated Commands
// ----------------------
// All of these returns MsgTypeServerError and disconnects if not authenticated
//

// MsgClientAllocateBlock allocates a new Block on the server. The server responds with a
// MsgServerAcknowledgeBlock if the Block already exists or a
// MsgServerReadBlock if the server needs to read the data from the client.
type MsgClientAllocateBlock struct { // -> "allo"
	BlockID Byte128 // ID of Block to allocate
}

// MsgServerAcknowledgeBlock sent as a confirmation that the server has the Block.
type MsgServerAcknowledgeBlock struct { // <- "ACKN"
	BlockID Byte128 // ID of Block that exists
}

// MsgServerReadBlock sent from the server to retreive Block data from client.
type MsgServerReadBlock struct { // <- "READ"
	BlockID Byte128 // ID of Block to read
}

// MsgClientWriteBlock sent to the server to write a Block. Server calculates its own BlockID from the block to verify its integrity.
// Server responds with MsgServerAcknowledgeBlock if the Block was written successfully.
type MsgClientWriteBlock struct { // -> "writ"
	Block *HashboxBlock // Block to be sent
}

// MsgClientReadBlock sent to the server to read a Block. Server responds with a MsgServerWriteBlock if the Block exists.
type MsgClientReadBlock struct { // -> "read"
	BlockID Byte128 // ID of Block to read
}

// MsgServerWriteBlock
type MsgServerWriteBlock struct { // <- "WRIT"
	Block *HashboxBlock // Block to be sent
}

//
// AccountCommands
// ---------------
//

// MsgClientAccountInfo is sent from the client to retrieve quota information and a list of all Datasets under the account.
type MsgClientAccountInfo struct { // -> "info"
	AccountNameH Byte128 // = md5(accountName)
}

// MsgServerAccountInfo sent from the server as a response to MsgClientAccountInfo.
type MsgServerAccountInfo struct { // <- "INFO"
	// TODO: Add quota stuff
	DatasetList DatasetArray // List of all Datasets under the account
}

// MsgClientListDataset sent from the client to retrieve a list of all states for a Dataset
type MsgClientListDataset struct { // -> "list"
	AccountNameH Byte128 // = md5(accountName)
	DatasetName  String  // Name of the Dataset you wish to list
}

// MsgServerListDataset sent from the server as a response to MsgClientListDataset. ListH is used so that the client can make sure it has the correct listing
type MsgServerListDataset struct { // <- "LIST"
	States DatasetStateArray // Array of all states under the Dataset
	ListH  Byte128           // = md5(States)
}

// MsgClientAddDatasetState sent to the server to add a Dataset or a Dataset state.
// Server returns MsgServerError if the DatasetState refers to a BlockID that does not exist.
type MsgClientAddDatasetState struct { // -> "+set"
	AccountNameH Byte128      // = md5(accountName)
	DatasetName  String       // Name of the Dataset to store under
	State        DatasetState // Dataset state to add
}

// MsgClientRemoveDatasetState sent by the client to remove a Dataset state.
type MsgClientRemoveDatasetState struct { // -> "-set"
	AccountNameH Byte128 // = md5(accountName)
	DatasetName  String  // Name of the Dataset to remove under
	StateID      Byte128 // ID of the state to remove
}

type ProtocolMessage struct {
	Num  uint16
	Type uint32
	Data interface{}
}

func (m ProtocolMessage) Serialize(w io.Writer) {
	WriteOrPanic(w, m.Num)
	WriteOrPanic(w, m.Type)

	if m.Data != nil {
		mv := reflect.ValueOf(m.Data).Elem()
		mt := reflect.TypeOf(m.Data).Elem()

		for i := 0; i < mt.NumField(); i++ {
			if mv.Field(i).Kind() == reflect.Ptr {
				mv.Field(i).Elem().Interface().(Serializer).Serialize(w)
			} else {
				mv.Field(i).Interface().(Serializer).Serialize(w)
			}
		}
	}
}
func (m *ProtocolMessage) Unserialize(r io.Reader) {
	ReadOrPanic(r, &m.Num)
	ReadOrPanic(r, &m.Type)

	switch m.Type {
	case MsgTypeListDataset | MsgTypeClientMask:
		m.Data = new(MsgClientListDataset)
	case MsgTypeAuthenticate | MsgTypeClientMask:
		m.Data = new(MsgClientAuthenticate)
	case MsgTypeGoodbye | MsgTypeClientMask: // no data
	case MsgTypeGreeting | MsgTypeClientMask: // no data
	case MsgTypeReadBlock | MsgTypeClientMask:
		m.Data = new(MsgClientReadBlock)
	case MsgTypeWriteBlock | MsgTypeClientMask:
		m.Data = new(MsgClientWriteBlock)
	case MsgTypeAllocateBlock | MsgTypeClientMask:
		m.Data = new(MsgClientAllocateBlock)
	case MsgTypeAccountInfo | MsgTypeClientMask:
		m.Data = new(MsgClientAccountInfo)
	case MsgTypeAddDatasetState | MsgTypeClientMask:
		m.Data = new(MsgClientAddDatasetState)
	case MsgTypeRemoveDatasetState | MsgTypeClientMask:
		m.Data = new(MsgClientRemoveDatasetState)

	case MsgTypeAuthenticate & MsgTypeServerMask:
	case MsgTypeGoodbye & MsgTypeServerMask: // no data
	case MsgTypeGreeting & MsgTypeServerMask:
		m.Data = new(MsgServerGreeting)
	case MsgTypeAcknowledgeBlock & MsgTypeServerMask:
		m.Data = new(MsgServerAcknowledgeBlock)
	case MsgTypeReadBlock & MsgTypeServerMask:
		m.Data = new(MsgServerReadBlock)
	case MsgTypeWriteBlock & MsgTypeServerMask:
		m.Data = new(MsgServerWriteBlock)
	case MsgTypeAccountInfo & MsgTypeServerMask:
		m.Data = new(MsgServerAccountInfo)
	case MsgTypeAddDatasetState & MsgTypeServerMask:
	case MsgTypeListDataset & MsgTypeServerMask:
		m.Data = new(MsgServerListDataset)
	case MsgTypeRemoveDatasetState & MsgTypeServerMask: // no data
	case MsgTypeError & MsgTypeServerMask:
		m.Data = new(MsgServerError)
	default:
		// return nil instead if needed, then the type will not match anything reasonable for the caller
		panic(errors.New("Invalid protocol message received \"" + string(m.Type) + "\" (connection corrupted?)"))
	}

	if m.Data != nil {
		mv := reflect.ValueOf(m.Data).Elem()
		mt := reflect.TypeOf(m.Data).Elem()

		for i := 0; i < mt.NumField(); i++ {
			if mv.Field(i).Kind() == reflect.Ptr { // pointer value, need to create the underlying type
				mv.Field(i).Set(reflect.New(mv.Field(i).Type().Elem()))
				mv.Field(i).Interface().(Unserializer).Unserialize(r)
			} else {
				mv.Field(i).Addr().Interface().(Unserializer).Unserialize(r)
			}
		}
	}
}
func (m ProtocolMessage) String() string {
	var b [4]byte
	b[0] = byte(m.Type >> 24)
	b[1] = byte(m.Type >> 16)
	b[2] = byte(m.Type >> 8)
	b[3] = byte(m.Type)
	return string(b[:])
}
func (m ProtocolMessage) Details() string {
	switch t := m.Data.(type) {
	case (*MsgClientAuthenticate):
		return fmt.Sprintf("%x %x", t.AccountNameH, t.AuthenticationH)
	case (*MsgClientAccountInfo):
		return fmt.Sprintf("%x", t.AccountNameH)
	case (*MsgClientListDataset):
		return fmt.Sprintf("%x %s", t.AccountNameH, t.DatasetName)
	case (*MsgClientAddDatasetState):
		return fmt.Sprintf("%x %s.%x", t.AccountNameH, t.DatasetName, t.State.StateID)
	case (*MsgClientRemoveDatasetState):
		return fmt.Sprintf("%x %s.%x", t.AccountNameH, t.DatasetName, t.StateID)
	case (*MsgClientAllocateBlock):
		return fmt.Sprintf("%x", t.BlockID)
	case (*MsgClientReadBlock):
		return fmt.Sprintf("%x", t.BlockID)
	case (*MsgClientWriteBlock):
		return fmt.Sprintf("%x", t.Block.BlockID)
	case (*MsgServerGreeting):
		return fmt.Sprintf("%x", t.SessionNonce)
	case (*MsgServerAccountInfo):
		return fmt.Sprintf("[%d]Dataset", len(t.DatasetList))
	case (*MsgServerListDataset):
		return fmt.Sprintf("%x", t.ListH)
	case (*MsgServerAcknowledgeBlock):
		return fmt.Sprintf("%x", t.BlockID)
	case (*MsgServerReadBlock):
		return fmt.Sprintf("%x", t.BlockID)
	case (*MsgServerWriteBlock):
		return fmt.Sprintf("%x", t.Block.BlockID)
	case (*MsgServerError):
		return string(t.ErrorMessage)
	case nil:
		return ""
	default:
		panic(errors.New("ASSERT: Panic much! Should not reach here"))
	}
	return ""
}

func ReadMessage(r io.Reader) *ProtocolMessage {
	var msg ProtocolMessage
	msg.Unserialize(r)
	return &msg
}
func WriteMessage(w io.Writer, msg *ProtocolMessage) {
	msg.Serialize(w)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
