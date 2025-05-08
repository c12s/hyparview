package transport

import (
	"encoding/binary"
	"log"
	"net"
	"strings"

	"github.com/c12s/hyparview/data"
)

type TCPConn struct {
	address      string
	conn         net.Conn
	msgCh        chan []byte
	disconnectCh chan struct{}
}

func NewTCPConn(address string) (Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return MakeTCPConn(conn)
}

func MakeTCPConn(conn net.Conn) (Conn, error) {
	tcpConn := TCPConn{
		address:      conn.RemoteAddr().String(),
		conn:         conn,
		msgCh:        make(chan []byte),
		disconnectCh: make(chan struct{}),
	}
	tcpConn.read()
	return tcpConn, nil
}

func (t TCPConn) GetAddress() string {
	return t.address
}

func (t TCPConn) Send(msg data.Message) error {
	payload, err := serialize(msg)
	if err != nil {
		return err
	}
	payloadSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(payloadSize, uint32(len(payload)))
	msgSerialized := append(payloadSize, payload...)
	_, err = t.conn.Write(msgSerialized)
	if t.isClosed(err) {
		t.disconnectCh <- struct{}{}
	}
	return err
}

func (t TCPConn) disconnect() error {
	err := t.conn.Close()
	if err != nil {
		return err
	}
	t.disconnectCh <- struct{}{}
	return nil
}

func (t TCPConn) onDisconnect(handler func()) {
	go func() {
		for range t.disconnectCh {
			handler()
		}
	}()
}

func (t TCPConn) onReceive(handler func(msg data.Message, msgBytes []byte)) {
	go func() {
		for msgBytes := range t.msgCh {
			msg, err := Deserialize(msgBytes, nil)
			if err != nil {
				log.Println(err)
				continue
			}
			handler(msg, msgBytes)
		}
	}()
}

func (t TCPConn) read() {
	go func() {
		header := make([]byte, 4)
		for {
			_, err := t.conn.Read(header)
			if err != nil {
				t.handleError(err)
				break
			}
			payloadSize := binary.LittleEndian.Uint32(header)
			payload := make([]byte, payloadSize)
			_, err = t.conn.Read(payload)
			if err != nil {
				t.handleError(err)
				break
			}
			// msg, err := Deserialize(payload, nil)
			// if err != nil {
			// 	log.Println(err)
			// 	continue
			// }
			t.msgCh <- payload
		}
	}()
}

func (t TCPConn) handleError(err error) {
	if err == nil {
		return
	}
	log.Println(err)
	t.disconnectCh <- struct{}{}
}

func (t TCPConn) isClosed(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "EOF")
}

func AcceptTcpConnsFn(address string) func(stopCh chan struct{}, handler func(conn Conn)) error {
	return func(stopCh chan struct{}, handler func(conn Conn)) error {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		// log.Printf("Server listening on %s\n", address)

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					log.Println("Connection error:", err)
					continue
				}
				// log.Printf("new TCP connection %s\n", conn.RemoteAddr().String())
				tcpConn, err := MakeTCPConn(conn)
				if err != nil {
					log.Println(err)
					continue
				}
				handler(tcpConn)
			}
		}()
		return nil
	}
}
