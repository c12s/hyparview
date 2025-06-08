package hyparview

import (
	"fmt"
	"math"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

func (h *HyParView) onJoin(msgBytes []byte, sender transport.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.Join{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize Join message", "error", err)
		return err
	}

	h.logger.Println("received Join message", "self", h.self.ID, "from", msg.NodeID, "listenAddress", msg.ListenAddress)

	if peer, err := h.activeView.getById(msg.NodeID); err == nil {
		return fmt.Errorf("peer %s already in active view", peer.Node.ID)
	}

	if h.activeView.full() {
		if err := h.disconnectRandomPeer(); err != nil {
			h.logger.Println("failed to disconnect random peer", "error", err)
		}
	}

	newPeer := Peer{
		Node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		Conn: sender,
	}
	h.activeView.add(newPeer, true, h.peerUp)
	h.logger.Println("added peer to active view", "peerID", newPeer.Node.ID, "address", newPeer.Node.ListenAddress)

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
		if err := peer.Conn.Send(forwardJoinMsg); err != nil {
			h.logger.Println("failed to send ForwardJoin message", "to", peer.Node.ID, "error", err)
		}
	}
	return nil
}

func (h *HyParView) onDisconnect(msgBytes []byte, sender transport.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.Disconnect{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize Disconnect message", "error", err)
		return err
	}

	h.logger.Println("received Disconnect message", "self", h.self.ID, "from", msg.NodeID)

	peer, err := h.activeView.getById(msg.NodeID)
	if err != nil {
		return fmt.Errorf("peer %s not in active view", msg.NodeID)
	}

	h.activeView.delete(peer, h.peerDown)
	return h.connManager.Disconnect(peer.Conn)
}

func (h *HyParView) onForwardJoin(msgBytes []byte, sender transport.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.ForwardJoin{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize ForwardJoin message", "error", err)
		return err
	}

	h.logger.Println("received ForwardJoin message", "self", h.self.ID, "from", msg.NodeID, "TTL", msg.TTL)

	newPeer := Peer{
		Node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		Conn: nil,
	}

	addedToActiveView := false
	if (msg.TTL == 0 || len(h.activeView.peers) == 1) && msg.NodeID != h.self.ID {
		_, err := h.activeView.getById(newPeer.Node.ID)
		if err != nil {
			conn, err := h.connManager.Connect(msg.ListenAddress, h.logger)
			if err != nil {
				h.logger.Println("failed to connect to peer", "address", msg.ListenAddress, "error", err)
				return err
			}
			forwardJoinAcceptMsg := data.Message{
				Type: data.FORWARD_JOIN_ACCEPT,
				Payload: data.ForwardJoinAccept{
					NodeID:        h.self.ID,
					ListenAddress: h.self.ListenAddress,
				},
			}
			if err := conn.Send(forwardJoinAcceptMsg); err != nil {
				h.logger.Println("failed to send ForwardJoinAccept", "to", msg.NodeID, "error", err)
			} else {
				newPeer.Conn = conn
				h.activeView.add(newPeer, true, h.peerUp)
				h.passiveView.delete(newPeer, nil)
				addedToActiveView = true
				h.logger.Println("added peer to active view via forward join", "peerID", newPeer.Node.ID, "address", newPeer.Node.ListenAddress)
			}
		}
	} else if msg.TTL == h.config.PRWL {
		h.passiveView.add(newPeer, false, nil)
		h.logger.Println("added peer to passive view via forward join", "peerID", newPeer.Node.ID)
	}

	msg.TTL--
	if !addedToActiveView && msg.TTL >= 0 {
		senderPeer, err := h.activeView.getByConn(sender)
		nodeIdBlacklist := []string{}
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
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.ForwardJoinAccept{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize ForwardJoinAccept message", "error", err)
		return err
	}

	h.logger.Println("received ForwardJoinAccept", "self", h.self.ID, "from", msg.NodeID)
	if peer, err := h.activeView.getById(msg.NodeID); err == nil {
		return fmt.Errorf("peer %s already in active view", peer.Node.ID)
	}

	newPeer := Peer{
		Node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		Conn: sender,
	}
	h.activeView.add(newPeer, true, h.peerUp)

	if h.activeView.overflow() {
		return h.disconnectRandomPeer()
	}
	return nil
}

// NOTE
// moze se desiti da cvorovi 1 i 2 istovremeno posalju neighbor zahteve jedan drugom
// prvi je sacuvao prvu konekciju, drugi drugu
// na reply se nista nece dodati u active view jer su vec dodati
// ->
// kada cvor 1 napusta mrezu, konekcija koju cuva cvor 2 nece se zatvoriti
// jer je cvor 1 ne cuva u svom active view
func (h *HyParView) onNeighbor(msgBytes []byte, sender transport.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.Neighbor{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize Neighbor message", "error", err)
		return err
	}

	h.logger.Println("received Neighbor message", "self", h.self.ID, "from", msg.NodeID, "highPriority", msg.HighPriority)
	_, err = h.activeView.getById(msg.NodeID)
	accept := (msg.HighPriority || !h.activeView.full()) && err != nil
	if accept {
		if h.activeView.full() {
			if err := h.disconnectRandomPeer(); err != nil {
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
		h.activeView.add(newPeer, true, h.peerUp)
		h.passiveView.delete(newPeer, nil)
		h.logger.Println("added peer to active view from neighbor", "peerID", newPeer.Node.ID, "address", newPeer.Node.ListenAddress)
	}

	neighborReplyMsg := data.Message{
		Type: data.NEIGHBOR_REPLY,
		Payload: data.NeighborReply{
			NodeID:        h.self.ID,
			ListenAddress: msg.ListenAddress,
			Accepted:      accept,
			AttemptsLeft:  msg.AttemptsLeft - 1,
		},
	}
	return sender.Send(neighborReplyMsg)
}

func (h *HyParView) onNeighborReply(msgBytes []byte, sender transport.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.NeighborReply{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize NeighborReply message", "error", err)
		return err
	}

	h.logger.Println("received NeighborReply", "self", h.self.ID, "from", msg.NodeID, "accepted", msg.Accepted, "attempts", msg.AttemptsLeft)

	if !msg.Accepted {
		if msg.AttemptsLeft > 0 {
			h.replacePeer([]string{msg.NodeID}, msg.AttemptsLeft)
		}
	} else {
		peer, err := h.passiveView.getById(msg.NodeID)
		if err != nil {
			return fmt.Errorf("peer [ID=%s] not found in passive view", msg.NodeID)
		}
		h.passiveView.delete(peer, nil)
		peer.Conn = sender

		if p, err := h.activeView.getById(msg.NodeID); err == nil {
			return fmt.Errorf("peer %s already in active view", p.Node.ID)
		}
		h.activeView.add(peer, true, h.peerUp)
		h.logger.Println("added peer to active view from neighbor reply", "peerID", peer.Node.ID, "address", peer.Node.ListenAddress)
	}
	return nil
}

func (h *HyParView) onShuffle(msgBytes []byte, sender transport.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.Shuffle{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize Shuffle message", "error", err)
		return err
	}

	h.logger.Println("received Shuffle", "self", h.self.ID, "from", msg.NodeID, "TTL", msg.TTL, "nodes", msg.Nodes)

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
		maxIndex := int(math.Min(float64(len(msg.Nodes)), float64(len(h.passiveView.peers))))
		peers := h.passiveView.peers[:maxIndex]
		nodes := make([]data.Node, len(peers))
		for i, peer := range peers {
			nodes[i] = peer.Node
		}

		var conn transport.Conn
		var tmp bool
		p, err := h.activeView.getById(msg.NodeID)
		if err == nil {
			conn = p.Conn
			tmp = false
		} else {
			conn, err = h.connManager.Connect(msg.ListenAddress, h.logger)
			tmp = true
			if err != nil {
				return err
			}
		}

		shuffleReplyMsg := data.Message{
			Type: data.SHUFFLE_REPLY,
			Payload: data.ShuffleReply{
				ReceivedNodes: msg.Nodes,
				Nodes:         nodes,
			},
		}
		if err := conn.Send(shuffleReplyMsg); err != nil {
			h.logger.Println("failed to send ShuffleReply", "to", msg.ListenAddress, "error", err)
		}
		if tmp {
			if err := h.connManager.Disconnect(conn); err != nil {
				h.logger.Println("failed to disconnect after sending ShuffleReply", "error", err)
			}
		}

		h.integrateNodesIntoPartialView(msg.Nodes, []data.Node{})
		return nil
	}
}

func (h *HyParView) onShuffleReply(msgBytes []byte, sender transport.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := &data.ShuffleReply{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		h.logger.Println("failed to deserialize ShuffleReply message", "error", err)
		return err
	}

	h.logger.Println("received ShuffleReply", "self", h.self.ID, "nodes", len(msg.Nodes), "receivedNodes", len(msg.ReceivedNodes))
	h.integrateNodesIntoPartialView(msg.Nodes, msg.ReceivedNodes)
	return nil
}
