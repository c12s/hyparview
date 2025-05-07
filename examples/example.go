package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	id := os.Args[1]
	address := os.Args[2]
	contactNodeAddress := os.Args[3]
	config := hyparview.Config{
		NodeID:             id,
		ListenAddress:      address,
		ContactNodeAddress: contactNodeAddress,
		HyParViewConfig: hyparview.HyParViewConfig{
			Fanout:          2,
			PassiveViewSize: 10,
			ARWL:            3,
			PRWL:            2,
			ShuffleInterval: 10,
			Ka:              1,
			Kp:              1,
		},
	}
	self := data.Node{
		ID:            config.NodeID,
		ListenAddress: config.ListenAddress,
	}
	connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
	hv, err := hyparview.NewHyParView(config.HyParViewConfig, self, connManager)
	if err != nil {
		log.Fatal(err)
	}
	// time.Sleep(10 * time.Second)
	err = hv.Join(contactNodeAddress)
	if err != nil {
		log.Println(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Waiting for exit signal...")
	sig := <-sigs
	log.Println("Received signal:", sig)
}
