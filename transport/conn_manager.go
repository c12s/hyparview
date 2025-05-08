package transport

import (
	"time"

	"github.com/c12s/hyparview/data"
)

type ConnManager struct {
	newConnFn          func(address string) (Conn, error)
	acceptConnsFn      func(stopCh chan struct{}, handler func(conn Conn)) error
	stopAcceptingConns chan struct{}
	connUp             chan Conn
	connDown           chan Conn
	messages           chan MsgReceived
}

func NewConnManager(newConnFn func(address string) (Conn, error), acceptConnsFn func(stopCh chan struct{}, handler func(conn Conn)) error) ConnManager {
	return ConnManager{
		newConnFn:     newConnFn,
		acceptConnsFn: acceptConnsFn,
		connUp:        make(chan Conn),
		connDown:      make(chan Conn),
		messages:      make(chan MsgReceived),
	}
}

func (cm *ConnManager) StartAcceptingConns() error {
	return cm.acceptConnsFn(cm.stopAcceptingConns, func(conn Conn) {
		cm.addConn(conn)
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
	cm.addConn(conn)
	select {
	case cm.connUp <- conn:
	case <-time.After(100 * time.Millisecond):
	}
	return conn, nil
}

func (cm *ConnManager) Disconnect(conn Conn) error {
	return conn.disconnect()
}

func (cm *ConnManager) OnConnUp(handler func(conn Conn)) Subscription {
	return Subscribe(cm.connUp, handler)
}

func (cm *ConnManager) OnConnDown(handler func(conn Conn)) Subscription {
	return Subscribe(cm.connDown, handler)
}

func (cm *ConnManager) OnReceive(handler func(msg MsgReceived)) Subscription {
	return Subscribe(cm.messages, handler)
}

func (cm *ConnManager) addConn(conn Conn) {
	conn.onReceive(func(msg data.Message, msgBytes []byte) {
		cm.messages <- MsgReceived{Msg: msg, Sender: conn, MsgBytes: msgBytes}
	})
	conn.onDisconnect(func() {
		select {
		case cm.connDown <- conn:
		case <-time.After(100 * time.Millisecond):
		}
	})
}

type MsgReceived struct {
	Msg      data.Message
	MsgBytes []byte
	Sender   Conn
}
