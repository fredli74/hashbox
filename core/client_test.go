//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2016
//	+---+´

// Hashbox core, version 0.1
package core

import (
	_ "bytes"
	"encoding/hex"
	_ "fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

type FauxServer struct {
	ServerWriter io.WriteCloser
	ServerReader io.ReadCloser
}

func (c FauxServer) Close() error {
	c.ServerReader.Close()
	c.ServerWriter.Close()
	return nil
}
func (c FauxServer) Read(data []byte) (n int, err error)  { return c.ServerReader.Read(data) }
func (c FauxServer) Write(data []byte) (n int, err error) { return c.ServerWriter.Write(data) }
func (c FauxServer) LocalAddr() net.Addr                  { return nil }
func (c FauxServer) RemoteAddr() net.Addr                 { return nil }
func (c FauxServer) SetDeadline(t time.Time) error        { return nil }
func (c FauxServer) SetReadDeadline(t time.Time) error    { return nil }
func (c FauxServer) SetWriteDeadline(t time.Time) error   { return nil }

func TestClientInit(t *testing.T) {
	hdump := hex.Dumper(os.Stdout)

	pr, pw := io.Pipe()
	go func(w io.Writer) {
		time.Sleep(2 * time.Second)
		WriteMessage(w, &ProtocolMessage{Num: 0, Type: MsgTypeGreeting & MsgTypeServerMask, Data: &MsgServerGreeting{Hash([]byte("testing"))}})
		time.Sleep(2 * time.Second)
		WriteMessage(w, &ProtocolMessage{Num: 1, Type: MsgTypeAuthenticate & MsgTypeServerMask})
		time.Sleep(2 * time.Second)
		WriteMessage(w, &ProtocolMessage{Num: 2, Type: MsgTypeGoodbye & MsgTypeServerMask})
	}(pw)

	c := FauxServer{hdump, pr}
	client := NewClient(c, "account name", Hash([]byte("password")))
	time.Sleep(1 * time.Second)
	client.Close()

	hdump.Close()
	_ = client
}
