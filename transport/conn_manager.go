package transport

import (
	"github.com/c12s/hyparview/data"
)

type ConnManager struct {
	newConnFn          func(address string) (Conn, error)
	acceptConnsFn      func(nodeID string, stopCh chan struct{}, handler func(conn Conn)) error
	stopAcceptingConns chan struct{}
	connDown           chan Conn
	messages           chan MsgReceived
}

func NewConnManager(newConnFn func(address string) (Conn, error), acceptConnsFn func(nodeID string, stopCh chan struct{}, handler func(conn Conn)) error) ConnManager {
	return ConnManager{
		newConnFn:          newConnFn,
		acceptConnsFn:      acceptConnsFn,
		stopAcceptingConns: make(chan struct{}),
		connDown:           make(chan Conn),
		messages:           make(chan MsgReceived),
	}
}

func (cm *ConnManager) StartAcceptingConns(nodeID string) error {
	return cm.acceptConnsFn(nodeID, cm.stopAcceptingConns, func(conn Conn) {
		cm.registerConnHandlers(conn)
	})
}

func (cm *ConnManager) StopAcceptingConns() {
	cm.stopAcceptingConns <- struct{}{}
}

func (cm *ConnManager) Connect(address string) (Conn, error) {
	conn, err := cm.newConnFn(address)
	if err != nil {
		return nil, err
	}
	cm.registerConnHandlers(conn)
	return conn, nil
}

func (cm *ConnManager) Disconnect(conn Conn) error {
	if conn != nil {
		return conn.disconnect()
	}
	return nil
}

func (cm *ConnManager) OnConnDown(handler func(conn Conn)) Subscription {
	return Subscribe(cm.connDown, handler)
}

func (cm *ConnManager) OnReceive(handler func(msg MsgReceived)) Subscription {
	return Subscribe(cm.messages, handler)
}

func (cm *ConnManager) registerConnHandlers(conn Conn) {
	conn.onReceive(func(msg data.Message, msgBytes []byte) {
		cm.messages <- MsgReceived{Msg: msg, Sender: conn, MsgBytes: msgBytes}
	})
	conn.onDisconnect(func() {
		cm.connDown <- conn
	})
}

type MsgReceived struct {
	Msg      data.Message
	MsgBytes []byte
	Sender   Conn
}
