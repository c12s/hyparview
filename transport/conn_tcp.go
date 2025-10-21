package transport

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
)

var (
	MessagesSent     = 0
	MessagesSentSub  = 0
	MessagesRcvd     = 0
	MessagesRcvdSub  = 0
	MessagesSentLock = new(sync.Mutex)
	MessagesRcvdLock = new(sync.Mutex)
)

type TCPConn struct {
	address           string
	conn              *net.TCPConn
	msgCh             chan []byte
	disconnectCh      chan struct{}
	disconnectHandler func()
	readTimeout       bool
	logger            *log.Logger
}

func NewTCPConn(address string, readTimeout bool, logger *log.Logger) (Conn, error) {
	// todo: tmp
	localIP := strings.Split(os.Getenv("RN_LISTEN_ADDR"), ":")[0]
	if localIP == "" {
		localIP = strings.Split(os.Getenv("LISTEN_ADDR"), ":")[0]
	}
	localAddr := &net.TCPAddr{IP: net.ParseIP(localIP), Port: 0}
	dialer := net.Dialer{
		LocalAddr: localAddr,
		Timeout:   5 * time.Second,
	}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return MakeTCPConn(conn.(*net.TCPConn), readTimeout, logger)
}

func MakeTCPConn(conn *net.TCPConn, readTimeout bool, logger *log.Logger) (Conn, error) {
	tcpConn := &TCPConn{
		address:      conn.RemoteAddr().String(),
		conn:         conn,
		msgCh:        make(chan []byte),
		disconnectCh: make(chan struct{}, 1),
		logger:       logger,
		readTimeout:  readTimeout,
	}
	// tcpConn.conn.SetLinger(0)
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

func (t *TCPConn) Send(msg data.Message) {
	go func() {
		payload, err := Serialize(msg)
		if err != nil {
			// return err
		}
		size := uint32(len(payload))
		t.logger.Println("SENT PAYLOAD SIZE", size)
		payloadSize := make([]byte, 4)
		binary.LittleEndian.PutUint32(payloadSize, size)
		msgSerialized := append(payloadSize, payload...)
		// t.conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
		_, err = t.conn.Write(msgSerialized)
		if err != nil {
			t.logger.Println(err)
		}
		if t.isClosed(err) || os.IsTimeout(err) {
			// go func() {
			t.disconnectCh <- struct{}{}
			// }()
		}
		// if err == nil && msg.Type == data.CUSTOM {
		MessagesSentLock.Lock()
		MessagesSent++
		MessagesSentLock.Unlock()
		// }
		// return err
	}()
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
			// if t.readTimeout {
			// 	t.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			// }
			// _, err := t.conn.Read(header)
			_, err := io.ReadFull(t.conn, header)
			if err != nil {
				t.logger.Println("tcp read error:", err)
				// go func() {
				t.disconnectCh <- struct{}{}
				// }()
				return
			}
			payloadSize := binary.LittleEndian.Uint32(header)
			t.logger.Println("PAYLOAD SIZE", payloadSize)
			if payloadSize > 50000 {
				t.logger.Println("payload too large")
				t.disconnectCh <- struct{}{}
				return
			}
			payload := make([]byte, payloadSize)
			// _, err = t.conn.Read(payload)
			_, err = io.ReadFull(t.conn, payload)
			if err != nil {
				t.logger.Println("tcp read error:", err)
				// go func() {
				t.disconnectCh <- struct{}{}
				// }()
				return
			}
			// if payload[0] == byte(data.CUSTOM) {
			MessagesRcvdLock.Lock()
			MessagesRcvd++
			MessagesRcvdLock.Unlock()
			// }
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

func AcceptTcpConnsFn(address string, readTimeout bool) func(stopCh chan struct{}, handler func(conn Conn), logger *log.Logger) error {
	return func(stopCh chan struct{}, handler func(conn Conn), logger *log.Logger) error {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		// logger.Printf("Server listening on %s\n", address)
		var conns []net.Conn

		go func(listener net.Listener) {
			for {
				conn, err := listener.Accept()
				if err != nil {
					logger.Println("Connection error:", err)
					return
				}
				// logger.Println("new TCP connection", conn.RemoteAddr().String())
				conns = append(conns, conn)
				tcpConn, err := MakeTCPConn(conn.(*net.TCPConn), readTimeout, logger)
				if err != nil {
					logger.Println(err)
					continue
				}
				go handler(tcpConn)
			}
		}(listener)
		go func(stopCh chan struct{}, listener net.Listener) {
			<-stopCh
			// logger.Println("received signal to stop accepting TCP connections")
			err := listener.Close()
			if err != nil {
				// logger.Println(err)
			}
			for _, conn := range conns {
				conn.Close()
			}
		}(stopCh, listener)
		return nil
	}
}
