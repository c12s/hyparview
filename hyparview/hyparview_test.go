package hyparview

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

func TestHyparview(t *testing.T) {
	const numNodes = 20
	var nodes []*HyParView

	port := 8000
	config := Config{
		Fanout:          2,
		PassiveViewSize: 5,
		ARWL:            2,
		PRWL:            2,
		ShuffleInterval: 10,
		Ka:              2,
		Kp:              2,
	}
	nodeID := fmt.Sprintf("node%d", 1)
	listenAddress := fmt.Sprintf("127.0.0.1:%d", port+1)

	for i := 0; i < numNodes; i++ {
		contactNodeID := nodeID
		contactNodeAddress := listenAddress
		nodeID = fmt.Sprintf("node%d", i+1)
		listenAddress = fmt.Sprintf("127.0.0.1:%d", port+i+1)

		self := data.Node{
			ID:            nodeID,
			ListenAddress: listenAddress,
		}
		connManager := transport.NewConnManager(false, transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress, false))
		hvLogFile, err := os.Create(fmt.Sprintf("log/hv_%s.log", nodeID))
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer hvLogFile.Close()
		logger := log.New(hvLogFile, "", log.LstdFlags|log.Lshortfile)
		node, err := NewHyParView(config, self, connManager, logger)
		if err != nil {
			log.Println(err)
		}

		nodes = append(nodes, node)
		time.Sleep(1 * time.Second)
		err = node.Join(contactNodeID, contactNodeAddress)
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
