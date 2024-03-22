//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2024
//	+---+´

// Hashbox core, version 0.1
package core

import (
	"net"
	"time"
)

type TimeoutConn struct {
	conn    net.Conn
	timeout time.Duration
}

func NewTimeoutConn(c net.Conn, t time.Duration) *TimeoutConn {
	return &TimeoutConn{c, t}
}

func (t *TimeoutConn) Close() error {
	return t.conn.Close()
}

func (t *TimeoutConn) Read(b []byte) (n int, err error) {
	t.conn.SetReadDeadline(time.Now().Add(t.timeout))
	return t.conn.Read(b)
}

func (t *TimeoutConn) Write(b []byte) (n int, err error) {
	t.conn.SetWriteDeadline(time.Now().Add(t.timeout))
	return t.conn.Write(b)
}
