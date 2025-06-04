package hyparview

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
	"github.com/natefinch/lumberjack"
)

func TestHyparview(t *testing.T) {
	const numNodes = 20
	var nodes []*HyParView
	port := 8000
	config := Config{
		HyParViewConfig: HyParViewConfig{
			Fanout:          2,
			PassiveViewSize: 5,
			ARWL:            2,
			PRWL:            2,
			ShuffleInterval: 10,
			Ka:              2,
			Kp:              2,
		},
	}

	for i := 0; i < numNodes; i++ {
		port = port + 1
		config.ContactNodeID = config.NodeID
		config.ContactNodeAddress = config.ListenAddress
		config.NodeID = fmt.Sprintf("node%d", i+1)
		config.ListenAddress = fmt.Sprintf("127.0.0.1:%d", port)
		self := data.Node{
			ID:            config.NodeID,
			ListenAddress: config.ListenAddress,
		}
		connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
		logger := log.New(&lumberjack.Logger{
			Filename: fmt.Sprintf("log/%s.log", config.NodeID),
		}, config.NodeID, log.LstdFlags|log.Lshortfile)
		node, err := NewHyParView(config.HyParViewConfig, self, connManager, logger)
		if err != nil {
			log.Println(err)
		}
		nodes = append(nodes, node)
		time.Sleep(1 * time.Second)
		err = node.Join(config.ContactNodeID, config.ContactNodeAddress)
		if err != nil {
			log.Println(err)
		}
	}

	time.Sleep(10 * time.Second)
	// for _, n := range nodes {
	// 	n.Leave()
	// }

	time.Sleep(5 * time.Second)
	for _, n := range nodes {
		log.Println("********************")
		log.Println(n.self.ID)
		log.Println("active view")
		for _, p := range n.activeView.peers {
			log.Println(p)
		}
		log.Println("passive view")
		for _, p := range n.passiveView.peers {
			log.Println(p)
		}
		log.Println("********************")
	}
}
