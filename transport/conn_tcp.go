package transport

import (
	"encoding/binary"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/c12s/hyparview/data"
)

var (
	MessagesSent     = 0
	MessagesRcvd     = 0
	MessagesSentLock = new(sync.Mutex)
	MessagesRcvdLock = new(sync.Mutex)
)

type TCPConn struct {
	address           string
	conn              *net.TCPConn
	msgCh             chan []byte
	disconnectCh      chan struct{}
	disconnectHandler func()
	logger            *log.Logger
}

func NewTCPConn(address string, logger *log.Logger) (Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return MakeTCPConn(conn.(*net.TCPConn), logger)
}

func MakeTCPConn(conn *net.TCPConn, logger *log.Logger) (Conn, error) {
	tcpConn := &TCPConn{
		address:      conn.RemoteAddr().String(),
		conn:         conn,
		msgCh:        make(chan []byte),
		disconnectCh: make(chan struct{}),
		logger:       logger,
	}
	tcpConn.conn.SetLinger(0)
	tcpConn.conn.SetKeepAlive(true)
	go func() {
		for range tcpConn.disconnectCh {
			if tcpConn.disconnectHandler != nil {
				tcpConn.disconnectHandler()
			}
		}
	}()
	tcpConn.read()
	return tcpConn, nil
}

func (t *TCPConn) GetAddress() string {
	return t.address
}

func (t *TCPConn) Send(msg data.Message) error {
	payload, err := Serialize(msg)
	if err != nil {
		return err
	}
	payloadSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(payloadSize, uint32(len(payload)))
	msgSerialized := append(payloadSize, payload...)
	_, err = t.conn.Write(msgSerialized)
	if err != nil {
		t.logger.Println(err)
	}
	if t.isClosed(err) {
		go func() {
			t.disconnectCh <- struct{}{}
		}()
	}
	if err == nil {
		MessagesSentLock.Lock()
		MessagesSent++
		MessagesSentLock.Unlock()
	}
	return err
}

func (t *TCPConn) Disconnect() error {
	return t.conn.Close()
}

func (t *TCPConn) onDisconnect(handler func()) {
	t.disconnectHandler = handler
}

func (t *TCPConn) onReceive(handler func(msgBytes []byte)) {
	go func() {
		for msgBytes := range t.msgCh {
			handler(msgBytes)
		}
	}()
}

func (t *TCPConn) read() {
	go func() {
		header := make([]byte, 4)
		for {
			_, err := t.conn.Read(header)
			MessagesRcvdLock.Lock()
			MessagesRcvd++
			MessagesRcvdLock.Unlock()
			if err != nil {
				t.logger.Println("tcp read error:", err)
				go func() {
					t.disconnectCh <- struct{}{}
				}()
				return
			}
			payloadSize := binary.LittleEndian.Uint32(header)
			payload := make([]byte, payloadSize)
			_, err = t.conn.Read(payload)
			if err != nil {
				t.logger.Println("tcp read error:", err)
				go func() {
					t.disconnectCh <- struct{}{}
				}()
				return
			}
			t.msgCh <- payload
		}
	}()
}

func (t *TCPConn) isClosed(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "EOF")
}

func AcceptTcpConnsFn(address string) func(stopCh chan struct{}, handler func(conn Conn), logger *log.Logger) error {
	return func(stopCh chan struct{}, handler func(conn Conn), logger *log.Logger) error {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		logger.Printf("Server listening on %s\n", address)
		var conns []net.Conn

		go func(listener net.Listener) {
			for {
				conn, err := listener.Accept()
				if err != nil {
					logger.Println("Connection error:", err)
					return
				}
				logger.Println("new TCP connection", conn.RemoteAddr().String())
				conns = append(conns, conn)
				tcpConn, err := MakeTCPConn(conn.(*net.TCPConn), logger)
				if err != nil {
					logger.Println(err)
					continue
				}
				go handler(tcpConn)
			}
		}(listener)
		go func(stopCh chan struct{}, listener net.Listener) {
			<-stopCh
			logger.Println("received signal to stop accepting TCP connections")
			err := listener.Close()
			if err != nil {
				logger.Println(err)
			}
			for _, conn := range conns {
				conn.Close()
			}
		}(stopCh, listener)
		return nil
	}
}
