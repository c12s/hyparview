package hyparview

import (
	"fmt"
	"log"
	"math"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

func (h *HyParView) onJoin(received transport.MsgReceived) error {
	msg, ok := received.Msg.Payload.(data.Join)
	if !ok {
		return fmt.Errorf("msg %v not a join msg", received.Msg.Payload)
	}
	if h.activeViewFull() {
		err := h.disconnectRandomPeer()
		if err != nil {
			return err
		}
	}
	newPeer := Peer{
		node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		conn: received.Sender,
	}
	h.activeView = append(h.activeView, newPeer)
	log.Printf("peer [ID=%s, address=%d] added to active view\n", newPeer.node.ID, newPeer.conn.GetAddress())
	forwardJoinMsg := data.Message{
		Type: data.FORWARD_JOIN,
		Payload: data.ForwardJoin{
			NodeID: msg.NodeID,
			TTL:    h.config.ARWL,
		},
	}
	for _, peer := range h.activeView {
		if peer.node.ID == newPeer.node.ID {
			continue
		}
		err := peer.conn.Send(forwardJoinMsg)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

func (h *HyParView) onDisconnect(received transport.MsgReceived) error {
	msg, ok := received.Msg.Payload.(data.Disconnect)
	if !ok {
		return fmt.Errorf("msg %v not a disconnect msg", received.Msg.Payload)
	}
	peer := h.getPeer(received.Sender)
	if peer == nil {
		log.Printf("peer %s not in active view\n", received.Sender.GetAddress())
	}
	if peer.node.ID != msg.NodeID {
		log.Printf("node trying to disconnect not matching the message info: %v - %s\n", msg, received.Sender.GetAddress())
		return nil
	}
	h.deletePeer(*peer)
	return h.connManager.Disconnect(peer.conn)
}

func (h *HyParView) onForwardJoin(received transport.MsgReceived) error {
	msg, ok := received.Msg.Payload.(data.ForwardJoin)
	if !ok {
		return fmt.Errorf("msg %v not a forward join msg", received.Msg.Payload)
	}
	newPeer := Peer{
		node: data.Node{
			ID:            msg.NodeID,
			ListenAddress: msg.ListenAddress,
		},
		conn: nil,
	}
	addedToActiveView := false
	if msg.TTL == 0 || len(h.activeView) == 1 {
		conn, err := h.connManager.Connect(msg.ListenAddress)
		if err != nil {
			return err
		}
		newPeer.conn = conn
		h.activeView = append(h.activeView, newPeer)
		addedToActiveView = true
	} else if msg.TTL == h.config.PRWL {
		h.passiveView = append(h.passiveView, newPeer)
	}
	msg.TTL--
	if !addedToActiveView {
		senderPeer := h.getPeer(received.Sender)
		nodeIdBlacklist := make([]string, 0)
		if senderPeer != nil {
			nodeIdBlacklist = append(nodeIdBlacklist, senderPeer.node.ID)
		}
		randomPeer := h.selectRandomPeer(nodeIdBlacklist)
		return randomPeer.conn.Send(data.Message{
			Type:    data.FORWARD_JOIN,
			Payload: msg,
		})
	}
	return nil
}

func (h *HyParView) onNeighbor(received transport.MsgReceived) error {
	msg, ok := received.Msg.Payload.(data.Neighbor)
	if !ok {
		return fmt.Errorf("msg %v not a neighbor msg", received.Msg.Payload)
	}
	accept := msg.HighPriority || !h.activeViewFull()
	if accept {
		if h.activeViewFull() {
			err := h.disconnectRandomPeer()
			if err != nil {
				return err
			}
		}
		newPeer := Peer{
			node: data.Node{
				ID:            msg.NodeID,
				ListenAddress: msg.ListenAddress,
			},
			conn: received.Sender,
		}
		h.activeView = append(h.activeView, newPeer)
		log.Printf("peer [ID=%s, address=%d] added to active view\n", newPeer.node.ID, newPeer.conn.GetAddress())
	}
	neighborReplyMsg := data.Message{
		Type: data.NEIGHTBOR_REPLY,
		Payload: data.NeighborReply{
			NodeID:        msg.NodeID,
			ListenAddress: msg.ListenAddress,
			Accepted:      accept,
		},
	}
	return received.Sender.Send(neighborReplyMsg)
}

func (h *HyParView) onNeighborReply(received transport.MsgReceived) error {
	msg, ok := received.Msg.Payload.(data.NeighborReply)
	if !ok {
		return fmt.Errorf("msg %v not a neighbor reply msg", received.Msg.Payload)
	}
	if !msg.Accepted {
		h.replacePeer([]string{msg.NodeID})
	} else {
		peer := h.getPeerCandidate(msg.NodeID)
		if peer == nil {
			return fmt.Errorf("peer [ID=%s] not found in passive view", msg.NodeID)
		}
		h.deletePeerCandidate(*peer)
		peer.conn = received.Sender
		h.activeView = append(h.activeView, *peer)
		log.Printf("peer [ID=%s, address=%d] added to active view\n", peer.node.ID, peer.conn.GetAddress())
	}
	return nil
}

func (h *HyParView) onShuffle(received transport.MsgReceived) error {
	msg, ok := received.Msg.Payload.(data.Shuffle)
	if !ok {
		return fmt.Errorf("msg %v not a shuffle msg", received.Msg.Payload)
	}
	msg.TTL--
	if msg.TTL > 0 && len(h.activeView) > 1 {
		peer := h.selectRandomPeer([]string{msg.NodeID})
		if peer == nil {
			return fmt.Errorf("cannot find a peer to forward the shuffle msg")
		}
		return peer.conn.Send(received.Msg)
	} else {
		passiveViewMaxIndex := int(math.Min(float64(len(msg.Nodes)), float64(len(h.passiveView))))
		peers := h.passiveView[:passiveViewMaxIndex]
		nodes := make([]data.Node, len(peers))
		for i, peer := range peers {
			nodes[i] = peer.node
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

func (h *HyParView) onShuffleReply(received transport.MsgReceived) error {
	msg, ok := received.Msg.Payload.(data.ShuffleReply)
	if !ok {
		return fmt.Errorf("msg %v not a shuffle reply msg", received.Msg.Payload)
	}
	h.integrateNodesIntoPartialView(msg.Nodes, msg.ReceivedNodes)
	return nil
}
