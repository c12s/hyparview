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
	conn         *net.TCPConn
	msgCh        chan []byte
	disconnectCh chan struct{}
}

func NewTCPConn(address string) (Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return MakeTCPConn(conn.(*net.TCPConn))
}

func MakeTCPConn(conn *net.TCPConn) (Conn, error) {
	tcpConn := TCPConn{
		address:      conn.RemoteAddr().String(),
		conn:         conn,
		msgCh:        make(chan []byte),
		disconnectCh: make(chan struct{}),
	}
	tcpConn.conn.SetLinger(0)
	tcpConn.conn.SetKeepAlive(true)
	tcpConn.read()
	return tcpConn, nil
}

func (t TCPConn) GetAddress() string {
	return t.address
}

func (t TCPConn) Send(msg data.Message) error {
	payload, err := Serialize(msg)
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
	return t.conn.Close()
}

func (t TCPConn) onDisconnect(handler func()) {
	go func() {
		for range t.disconnectCh {
			handler()
		}
	}()
}

func (t TCPConn) onReceive(handler func(msgBytes []byte)) {
	go func() {
		for msgBytes := range t.msgCh {
			// msg, err := Deserialize(msgBytes, )
			// if err != nil {
			// 	log.Println(err)
			// 	continue
			// }
			handler(msgBytes)
		}
	}()
}

func (t TCPConn) read() {
	go func() {
		header := make([]byte, 4)
		for {
			_, err := t.conn.Read(header)
			if err != nil {
				log.Println("tcp read error:", err)
				t.disconnectCh <- struct{}{}
				return
			}
			payloadSize := binary.LittleEndian.Uint32(header)
			payload := make([]byte, payloadSize)
			_, err = t.conn.Read(payload)
			if err != nil {
				log.Println("tcp read error:", err)
				t.disconnectCh <- struct{}{}
				return
			}
			t.msgCh <- payload
		}
	}()
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

func AcceptTcpConnsFn(address string) func(nodeID string, stopCh chan struct{}, handler func(conn Conn)) error {
	return func(nodeID string, stopCh chan struct{}, handler func(conn Conn)) error {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		log.Printf("Server listening on %s\n", address)

		go func(listener net.Listener) {
			for {
				conn, err := listener.Accept()
				if err != nil {
					log.Println("Connection error:", err)
					return
				}
				log.Println(nodeID, "new TCP connection", conn.RemoteAddr().String())
				tcpConn, err := MakeTCPConn(conn.(*net.TCPConn))
				if err != nil {
					log.Println(err)
					continue
				}
				go handler(tcpConn)
			}
		}(listener)
		go func(stopCh chan struct{}, listener net.Listener) {
			<-stopCh
			log.Println("received signal to stop accepting TCP connections")
			err := listener.Close()
			if err != nil {
				log.Println(err)
			}
		}(stopCh, listener)
		return nil
	}
}
