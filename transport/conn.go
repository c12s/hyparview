package transport

import "github.com/c12s/hyparview/data"

type Conn interface {
	GetAddress() string
	Send(msg data.Message)
	onReceive(handler func(msgBytes []byte))
	Disconnect() error
	onDisconnect(handler func())
}
