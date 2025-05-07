package transport

import (
	"errors"
	"log"
	"slices"

	"github.com/c12s/hyparview/data"
)

type ConnManager struct {
	conns              []Conn
	newConnFn          func(address string) (Conn, error)
	acceptConnsFn      func(stopCh chan struct{}, handler func(conn Conn)) error
	stopAcceptingConns chan struct{}
	connUp             chan Conn
	connDown           chan Conn
	messages           chan MsgReceived
}

func NewConnManager(newConnFn func(address string) (Conn, error), acceptConnsFn func(stopCh chan struct{}, handler func(conn Conn)) error) ConnManager {
	return ConnManager{
		conns:         make([]Conn, 0),
		newConnFn:     newConnFn,
		acceptConnsFn: acceptConnsFn,
		connUp:        make(chan Conn),
		connDown:      make(chan Conn),
		messages:      make(chan MsgReceived),
	}
}

func (cm *ConnManager) StartAcceptingConns() error {
	return cm.acceptConnsFn(cm.stopAcceptingConns, func(conn Conn) {
		log.Printf("new connection accepted %s\n", conn.GetAddress())
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
	cm.connUp <- conn
	return conn, nil
}

func (cm *ConnManager) Disconnect(conn Conn) error {
	index := slices.Index(cm.conns, conn)
	if index == -1 {
		return errors.New("conn not found")
	}
	cm.conns = slices.Delete(cm.conns, index, index+1)
	err := conn.disconnect()
	cm.connDown <- conn
	return err
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
	conn.onReceive(func(msg data.Message) {
		log.Println("message received %v\n", msg)
		cm.messages <- MsgReceived{Msg: msg, Sender: conn}
	})
	cm.conns = append(cm.conns, conn)
	log.Printf("connection added %s\n", conn.GetAddress())
}

type MsgReceived struct {
	Msg    data.Message
	Sender Conn
}
