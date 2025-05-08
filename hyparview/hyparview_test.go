package hyparview

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

func TestHyparview(t *testing.T) {
	const numNodes = 50
	var nodes []*HyParView
	port := 8000
	config := Config{
		HyParViewConfig: HyParViewConfig{
			Fanout:          2,
			PassiveViewSize: 5,
			ARWL:            0,
			PRWL:            0,
			ShuffleInterval: 20,
			Ka:              1,
			Kp:              1,
		},
	}

	for i := 0; i < numNodes; i++ {
		port = port + 1
		config.ContactNodeID = config.NodeID
		config.ContactNodeAddress = config.ListenAddress
		config.NodeID = fmt.Sprintf("node%d", i+1)
		config.ListenAddress = fmt.Sprintf("localhost:%d", port)
		self := data.Node{
			ID:            config.NodeID,
			ListenAddress: config.ListenAddress,
		}
		connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
		node, err := NewHyParView(config.HyParViewConfig, self, connManager)
		if err != nil {
			log.Println(err)
		}
		nodes = append(nodes, node)
		time.Sleep(3 * time.Second)
		err = node.Join(config.ContactNodeID, config.ContactNodeAddress)
		if err != nil {
			log.Println(err)
		}
	}

	time.Sleep(120 * time.Second)

	for _, n := range nodes {
		log.Println("********************")
		log.Println(n.self.ID)
		log.Println("active view")
		// log.Println(n.activeView.peers)
		for _, p := range n.activeView.peers {
			log.Println(p)
		}
		log.Println("passive view")
		// log.Println(n.passiveView.peers)
		for _, p := range n.passiveView.peers {
			log.Println(p)
		}
		log.Println("********************")
	}

	for _, n := range nodes {
		n.Leave()
	}
	time.Sleep(3 * time.Second)
}
