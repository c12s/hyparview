package transport

import "log"

type ConnManager struct {
	newConnFn          func(address string, logger *log.Logger) (Conn, error)
	acceptConnsFn      func(stopCh chan struct{}, handler func(conn Conn), logger *log.Logger) error
	stopAcceptingConns chan struct{}
	connDown           chan Conn
	messages           chan MsgReceived
}

func NewConnManager(newConnFn func(address string, logger *log.Logger) (Conn, error), acceptConnsFn func(stopCh chan struct{}, handler func(conn Conn), logger *log.Logger) error) ConnManager {
	return ConnManager{
		newConnFn:          newConnFn,
		acceptConnsFn:      acceptConnsFn,
		stopAcceptingConns: make(chan struct{}),
		connDown:           make(chan Conn),
		messages:           make(chan MsgReceived),
	}
}

func (cm *ConnManager) StartAcceptingConns(logger *log.Logger) error {
	return cm.acceptConnsFn(cm.stopAcceptingConns, func(conn Conn) {
		cm.registerConnHandlers(conn)
	}, logger)
}

func (cm *ConnManager) StopAcceptingConns() {
	cm.stopAcceptingConns <- struct{}{}
}

func (cm *ConnManager) Connect(address string, logger *log.Logger) (Conn, error) {
	conn, err := cm.newConnFn(address, logger)
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
	conn.onReceive(func(msgBytes []byte) {
		cm.messages <- MsgReceived{Sender: conn, MsgBytes: msgBytes}
	})
	conn.onDisconnect(func() {
		cm.connDown <- conn
	})
}

type MsgReceived struct {
	MsgBytes []byte
	Sender   Conn
}
