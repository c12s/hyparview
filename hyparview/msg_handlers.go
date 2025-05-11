package hyparview

import (
	"fmt"
	"log"
	"math"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

func (h *HyParView) onJoin(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.Join{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onJoin %v\n", h.self.ID, msg)
	if h.activeView.full() {
		err := h.disconnectRandomPeer()
		if err != nil {
			return err
		}
	}
	newPeer := Peer{
		Node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		Conn: sender,
	}
	h.activeView.add(newPeer, true)
	log.Printf("peer [ID=%s, address=%s] added to active view\n", newPeer.Node.ID, newPeer.Conn.GetAddress())
	forwardJoinMsg := data.Message{
		Type: data.FORWARD_JOIN,
		Payload: data.ForwardJoin{
			ListenAddress: msg.ListenAddress,
			NodeID:        msg.NodeID,
			TTL:           h.config.ARWL,
		},
	}
	for _, peer := range h.activeView.peers {
		if peer.Node.ID == newPeer.Node.ID {
			continue
		}
		err := peer.Conn.Send(forwardJoinMsg)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

func (h *HyParView) onDisconnect(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.Disconnect{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onDisconnect %v\n", h.self.ID, msg)
	peer, err := h.activeView.getById(msg.NodeID)
	if err != nil {
		return fmt.Errorf("peer %s not in active view\n", msg.NodeID)
	}
	h.activeView.delete(peer)
	return h.connManager.Disconnect(peer.Conn)
}

func (h *HyParView) onForwardJoin(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.ForwardJoin{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onForwardJoin %v\n", h.self.ID, msg)
	newPeer := Peer{
		Node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		Conn: nil,
	}
	addedToActiveView := false
	if msg.TTL == 0 || len(h.activeView.peers) == 1 {
		conn, err := h.connManager.Connect(msg.ListenAddress)
		if err != nil {
			return err
		}
		forwardJoinAcceptMsg := data.Message{
			Type: data.FORWARD_JOIN_ACCEPT,
			Payload: data.ForwardJoinAccept{
				NodeID:        h.self.ID,
				ListenAddress: h.self.ListenAddress,
			},
		}
		err = conn.Send(forwardJoinAcceptMsg)
		if err != nil {
			log.Println(err)
		} else {
			newPeer.Conn = conn
			// if h.activeView.full() {
			// 	err = h.disconnectRandomPeer()
			// 	if err != nil {
			// 		log.Println(err)
			// 	}
			// }
			h.activeView.add(newPeer, true)
			addedToActiveView = true
		}
	} else if msg.TTL == h.config.PRWL {
		h.passiveView.add(newPeer, false)
	}
	msg.TTL--
	if !addedToActiveView {
		senderPeer, err := h.activeView.getByConn(sender)
		nodeIdBlacklist := make([]string, 0)
		if err == nil {
			nodeIdBlacklist = append(nodeIdBlacklist, senderPeer.Node.ID)
		}
		randomPeer, err := h.activeView.selectRandom(nodeIdBlacklist, true)
		if err == nil && randomPeer.Conn != nil {
			return randomPeer.Conn.Send(data.Message{
				Type:    data.FORWARD_JOIN,
				Payload: msg,
			})
		}
	}
	return nil
}

func (h *HyParView) onForwardJoinAccept(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.ForwardJoinAccept{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onForwardJoinAccept %v\n", h.self.ID, msg)
	newPeer := Peer{
		Node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		Conn: sender,
	}
	h.activeView.add(newPeer, true)
	if h.activeView.full() {
		return h.disconnectRandomPeer()
	}
	return nil
}

func (h *HyParView) onNeighbor(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.Neighbor{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onNeighbor %v\n", h.self.ID, msg)
	accept := msg.HighPriority || !h.activeView.full()
	if accept {
		if h.activeView.full() {
			err := h.disconnectRandomPeer()
			if err != nil {
				return err
			}
		}
		newPeer := Peer{
			Node: data.Node{
				ID:            msg.NodeID,
				ListenAddress: msg.ListenAddress,
			},
			Conn: sender,
		}
		h.activeView.add(newPeer, true)
		log.Printf("peer [ID=%s, address=%s] added to active view\n", newPeer.Node.ID, newPeer.Conn.GetAddress())
	}
	neighborReplyMsg := data.Message{
		Type: data.NEIGHTBOR_REPLY,
		Payload: data.NeighborReply{
			NodeID:        msg.NodeID,
			ListenAddress: msg.ListenAddress,
			Accepted:      accept,
		},
	}
	return sender.Send(neighborReplyMsg)
}

func (h *HyParView) onNeighborReply(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.NeighborReply{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onNeighborReply %v\n", h.self.ID, msg)
	if !msg.Accepted {
		h.replacePeer([]string{msg.NodeID})
	} else {
		peer, err := h.passiveView.getById(msg.NodeID)
		if err != nil {
			return fmt.Errorf("peer [ID=%s] not found in passive view", msg.NodeID)
		}
		h.passiveView.delete(peer)
		peer.Conn = sender
		h.activeView.add(peer, true)
		log.Printf("peer [ID=%s, address=%s] added to active view\n", peer.Node.ID, peer.Conn.GetAddress())
	}
	return nil
}

func (h *HyParView) onShuffle(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.Shuffle{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onShuffle %v\n", h.self.ID, msg)
	msg.TTL--
	if msg.TTL > 0 && len(h.activeView.peers) > 1 {
		peer, err := h.activeView.selectRandom([]string{msg.NodeID}, true)
		if err != nil {
			return fmt.Errorf("cannot find a peer to forward the shuffle msg")
		}
		return peer.Conn.Send(data.Message{
			Type:    data.SHUFFLE,
			Payload: msg,
		})
	} else {
		passiveViewMaxIndex := int(math.Min(float64(len(msg.Nodes)), float64(len(h.passiveView.peers))))
		peers := h.passiveView.peers[:passiveViewMaxIndex]
		nodes := make([]data.Node, len(peers))
		for i, peer := range peers {
			nodes[i] = peer.Node
		}
		conn, err := h.connManager.Connect(msg.ListenAddress)
		if err != nil {
			return err
		}
		shuffleReplyMsg := data.Message{
			Type: data.SHUFFLE_REPLY,
			Payload: data.ShuffleReply{
				ReceivedNodes: msg.Nodes,
				Nodes:         nodes,
			},
		}
		err = conn.Send(shuffleReplyMsg)
		if err != nil {
			log.Println(err)
		}
		err = h.connManager.Disconnect(conn)
		if err != nil {
			log.Println(err)
		}
		h.integrateNodesIntoPartialView(msg.Nodes, []data.Node{})
		return nil
	}
}

func (h *HyParView) onShuffleReply(msgBytes []byte, sender transport.Conn) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	msg := &data.ShuffleReply{}
	_, err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		return err
	}
	log.Printf("%s received onShuffleReplyy %v\n", h.self.ID, msg)
	h.integrateNodesIntoPartialView(msg.Nodes, msg.ReceivedNodes)
	return nil
}
