package hyparview

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

// events

type Cmd struct {
	Error chan error
}

type JoinCmd struct {
	Cmd
	ContactID, ContactAddr string
}

type LeaveCmd struct {
	Cmd
}

type MsgEvent struct {
	MsgBytes []byte
	Sender   transport.Conn
}

type ConnDownEvent struct {
	Conn transport.Conn
}

type ShuffleEvent struct{}

type PeerUpEvent struct {
	Peer Peer
}

type PeerDownEvent struct {
	Peer Peer
}

// event loop

func (h *HyParView) processEvents() {
	for {
		select {
		case cmd := <-h.joinCh:
			cmd.Error <- h.join(cmd.ContactID, cmd.ContactAddr)
		case cmd := <-h.leaveCh:
			h.leave()
			cmd.Error <- nil
		case e := <-h.msgCh:
			h.processMsg(e.MsgBytes, e.Sender)
		case e := <-h.connDownCh:
			h.processConnDown(e.Conn)
		case <-h.shuffleCh:
			h.shuffle()
		case e := <-h.peerUpCh:
			h.peerUpHandler(e.Peer)
		case e := <-h.peerDownCh:
			h.peerDownHandler(e.Peer)
		}
	}
}

// event processors

func (h *HyParView) join(contactNodeID string, contactNodeAddress string) error {
	h.logger.Printf("%s attempting to join via %s (%s)", h.self.ID, contactNodeID, contactNodeAddress)

	if peer, err := h.activeView.getById(contactNodeID); err == nil {
		return fmt.Errorf("peer %s already in active view", peer.Node.ID)
	}

	if h.left {
		err := h.connManager.StartAcceptingConns(h.logger)
		if err != nil {
			return err
		}
		h.rcvSub = h.connManager.OnReceive(h.triggerMsgRcvd)
		_ = h.connManager.OnConnDown(h.triggerConnDown)
		// go h.shuffle()
		go h.triggerShuffle()
	}
	h.left = false

	if contactNodeAddress == "" || contactNodeAddress == "x" || contactNodeID == h.self.ID {
		return nil
	}
	conn, err := h.connManager.Connect(contactNodeAddress, h.logger)
	if err != nil {
		h.logger.Printf("%s failed to connect to %s: %v", h.self.ID, contactNodeAddress, err)
		return err
	}

	msg := data.Message{
		Type: data.JOIN,
		Payload: data.Join{
			ListenAddress: h.self.ListenAddress,
			NodeID:        h.self.ID,
		},
	}

	conn.Send(msg)
	// err = conn.Send(msg)
	// if err != nil {
	// 	h.logger.Printf("%s failed to send JOIN message to %s: %v", h.self.ID, contactNodeID, err)
	// 	return err
	// }

	newPeer := Peer{
		Node: data.Node{
			ID:            contactNodeID,
			ListenAddress: contactNodeAddress,
		},
		Conn: conn,
	}
	h.activeView.add(newPeer)
	h.triggerPeerUp(newPeer)
	h.logger.Printf("%s successfully connected to %s", h.self.ID, contactNodeID)
	return nil
}

func (h *HyParView) leave() {
	if h.left {
		h.logger.Printf("%s already left the network", h.self.ID)
		return
	}
	h.logger.Printf("%s is leaving the network", h.self.ID)
	h.left = true
	h.connManager.StopAcceptingConns()
	h.rcvSub.Unsubscribe()
	// _ = h.connManager.OnReceive(nil)
	h.stopShuffleCh <- ShuffleEvent{}
	for _, peer := range h.activeView.peers {
		err := h.connManager.Disconnect(peer.Conn)
		if err != nil {
			h.logger.Println(err)
		}
	}
	h.activeView.peers = make([]Peer, 0)
	h.passiveView.peers = make([]Peer, 0)
	h.activeNeightbor = make(map[string]int)
}

func (h *HyParView) processMsg(msgBytes []byte, sender transport.Conn) {
	if h.left {
		return
	}
	msgType := transport.GetMsgType(msgBytes)
	payload, err := transport.GetPayload(msgBytes)
	if err != nil {
		h.logger.Println(err)
		return
	}
	if slices.Contains(data.InternalMsgTypes(), msgType) {
		handler := h.msgHandlers[msgType]
		if handler == nil {
			h.logger.Printf("no handler found for message type %v", msgType)
			return
		}
		err = handler(payload, sender)
		if err != nil {
			h.logger.Println(err)
		}
	} else {
		peer, err := h.activeView.getByConn(sender)
		if err != nil {
			h.logger.Println(err)
			if !h.AllowAny {
				return
			}
		}
		handler := h.clientMsgHandlers[msgType]
		if handler == nil {
			h.logger.Printf("no handler found for message type %v", msgType)
			return
		}
		handler(payload, peer)
	}
}

func (h *HyParView) shuffle() {
	activeViewMaxIndex := int(math.Min(float64(h.config.Ka), float64(len(h.activeView.peers))))
	passiveViewMaxIndex := int(math.Min(float64(h.config.Kp), float64(len(h.passiveView.peers))))
	activePeers := make([]Peer, activeViewMaxIndex)
	passivePeers := make([]Peer, passiveViewMaxIndex)
	copy(activePeers, h.activeView.peers)
	copy(passivePeers, h.passiveView.peers)
	peers := append(activePeers, passivePeers...)
	nodes := make([]data.Node, len(peers))
	for i, peer := range peers {
		nodes[i] = peer.Node
	}
	shuffleMsg := data.Message{
		Type: data.SHUFFLE,
		Payload: data.Shuffle{
			NodeID:        h.self.ID,
			ListenAddress: h.self.ListenAddress,
			Nodes:         nodes,
			TTL:           h.config.ARWL,
		},
	}
	peer, err := h.activeView.selectRandom([]string{})
	if err != nil {
		h.replacePeer([]string{}, 2)
		h.logger.Println("no peers in active view to perform shuffle")
		return
	}
	peer.Conn.Send(shuffleMsg)
	// err = peer.Conn.Send(shuffleMsg)
	// if err != nil {
	// 	h.logger.Println(err)
	// }
}

func (h *HyParView) processConnDown(conn transport.Conn) {
	if peer, err := h.activeView.getByConn(conn); err == nil {
		if !h.activeView.full() && !h.left {
			h.replacePeer([]string{}, 2)
		}
		// h.logger.Printf("%s - peer %s down", h.self.ID, peer.Node.ID)
		h.activeView.delete(peer)
		h.triggerPeerDown(peer)
	}
}

// event generators

func (h *HyParView) Join(contactNodeID string, contactNodeAddress string) error {
	errCh := make(chan error)
	h.joinCh <- JoinCmd{
		ContactID:   contactNodeID,
		ContactAddr: contactNodeAddress,
		Cmd:         Cmd{Error: errCh},
	}
	return <-errCh
}

func (h *HyParView) Leave() {
	errCh := make(chan error)
	h.leaveCh <- LeaveCmd{Cmd: Cmd{Error: errCh}}
	<-errCh
}

func (h *HyParView) triggerShuffle() {
	ticker := time.NewTicker(time.Duration(h.config.ShuffleInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			h.shuffleCh <- ShuffleEvent{}
		case <-h.stopShuffleCh:
			h.logger.Println("stop triggering the shuffle process")
			return
		}
	}
}

func (h *HyParView) triggerMsgRcvd(rcvd transport.MsgReceived) {
	if h.left {
		return
	}
	h.msgCh <- MsgEvent{MsgBytes: rcvd.MsgBytes, Sender: rcvd.Sender}
}

func (h *HyParView) triggerConnDown(conn transport.Conn) {
	h.connDownCh <- ConnDownEvent{Conn: conn}
}

func (h *HyParView) triggerPeerUp(peer Peer) {
	h.peerUpCh <- PeerUpEvent{Peer: peer}
}

func (h *HyParView) triggerPeerDown(peer Peer) {
	h.peerDownCh <- PeerDownEvent{Peer: peer}
}
