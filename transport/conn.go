package transport

import "github.com/c12s/hyparview/data"

type Conn interface {
	GetAddress() string
	Send(msg data.Message) error
	onReceive(handler func(msg data.Message))
	disconnect() error
	onDisconnect(handler func())
}
