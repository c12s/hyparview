package hyparview

import (
	"log"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

type HyParView struct {
	self        data.Node
	config      HyParViewConfig
	activeView  *PeerList
	passiveView *PeerList
	connManager transport.ConnManager
	peerUp      chan Peer
	peerDown    chan Peer
	msgHandlers map[data.MessageType]func(msg []byte, sender transport.Conn) error
}

func NewHyParView(config HyParViewConfig, self data.Node, connManager transport.ConnManager) (*HyParView, error) {
	hv := &HyParView{
		self:   self,
		config: config,
		activeView: &PeerList{
			peers:    make([]Peer, 0),
			mu:       new(sync.RWMutex),
			capacity: config.Fanout + 1,
		},
		passiveView: &PeerList{
			peers:    make([]Peer, 0),
			mu:       new(sync.RWMutex),
			capacity: config.PassiveViewSize,
		},
		peerUp:      make(chan Peer),
		peerDown:    make(chan Peer),
		connManager: connManager,
	}
	hv.msgHandlers = map[data.MessageType]func(msgAny []byte, sender transport.Conn) error{
		data.JOIN:                hv.onJoin,
		data.DISCONNECT:          hv.onDisconnect,
		data.FORWARD_JOIN:        hv.onForwardJoin,
		data.FORWARD_JOIN_ACCEPT: hv.onForwardJoinAccept,
		data.NEIGHTBOR:           hv.onNeighbor,
		data.NEIGHTBOR_REPLY:     hv.onNeighborReply,
		data.SHUFFLE:             hv.onShuffle,
		data.SHUFFLE_REPLY:       hv.onShuffleReply,
	}
	err := connManager.StartAcceptingConns()
	go hv.shuffle()
	return hv, err
}

func (h *HyParView) Join(contactNodeID, contactNodeAddress string) error {
	h.activeView.mu.Lock()
	defer h.activeView.mu.Unlock()
	h.passiveView.mu.Lock()
	defer h.passiveView.mu.Unlock()
	_ = h.connManager.OnReceive(h.onReeive)
	conn, err := h.connManager.Connect(contactNodeAddress)
	if err != nil {
		return err
	}
	msg := data.Message{
		Type: data.JOIN,
		Payload: data.Join{
			ListenAddress: h.self.ListenAddress,
			NodeID:        h.self.ID,
		},
	}
	err = conn.Send(msg)
	if err != nil {
		return err
	}
	newPeer := Peer{
		node: data.Node{
			ID:            contactNodeID,
			ListenAddress: contactNodeAddress,
		},
		conn: conn,
	}
	// log.Println(newPeer.conn)
	h.activeView.peers = append(h.activeView.peers, newPeer)
	return nil
}

func (h *HyParView) Leave() {

}

func (h *HyParView) GetPeers() []Peer {
	return h.activeView.peers
}

func (h *HyParView) OnPeerUp(handler func(peer Peer)) transport.Subscription {
	return h.connManager.OnConnUp(func(conn transport.Conn) {
		h.activeView.mu.Lock()
		defer h.activeView.mu.Unlock()
		h.passiveView.mu.Lock()
		defer h.passiveView.mu.Unlock()
		if peer, err := h.activeView.getByConn(conn); err == nil {
			handler(peer)
		}
	})
}

func (h *HyParView) OnPeerDown(handler func(peer Peer)) transport.Subscription {
	return h.connManager.OnConnDown(func(conn transport.Conn) {
		h.activeView.mu.Lock()
		defer h.activeView.mu.Unlock()
		h.passiveView.mu.Lock()
		defer h.passiveView.mu.Unlock()
		if peer, err := h.activeView.getByConn(conn); err == nil {
			h.activeView.delete(peer)
			go h.replacePeer([]string{})
			go handler(peer)
		}
	})
}

func (h *HyParView) onReeive(received transport.MsgReceived) {
	handler := h.msgHandlers[received.Msg.Type]
	if handler == nil {
		log.Printf("no handler found for message type %v", received.Msg.Type)
		return
	}
	err := handler(received.MsgBytes, received.Sender)
	if err != nil {
		log.Println(err)
	}
}

func (h *HyParView) disconnectRandomPeer() error {
	disconnectPeer, err := h.activeView.selectRandom([]string{})
	if err != nil {
		return nil
	}
	h.activeView.delete(disconnectPeer)
	disconnectMsg := data.Message{
		Type: data.DISCONNECT,
		Payload: data.Disconnect{
			NodeID: h.self.ID,
		},
	}
	if disconnectPeer.conn == nil {
		return nil
	}
	err = disconnectPeer.conn.Send(disconnectMsg)
	if err != nil {
		return err
	}
	// err = h.connManager.Disconnect(disconnectPeer.conn)
	// if err != nil {
	// 	log.Println(err)
	// }
	return nil
}

func (h *HyParView) replacePeer(nodeIdBlacklist []string) {
	for {
		candidate, err := h.passiveView.selectRandom(nodeIdBlacklist)
		if err != nil {
			log.Println("no peer candidates to replace the failed peer")
			break
		}
		conn, err := h.connManager.Connect(candidate.node.ListenAddress)
		if err != nil {
			log.Println(err)
			h.passiveView.delete(candidate)
			continue
		}
		candidate.conn = conn
		neighborMsg := data.Message{
			Type: data.NEIGHTBOR,
			Payload: data.Neighbor{
				NodeID:        h.self.ID,
				ListenAddress: h.self.ListenAddress,
				HighPriority:  len(h.activeView.peers) == 0,
			},
		}
		err = candidate.conn.Send(neighborMsg)
		if err != nil {
			log.Println(err)
			h.passiveView.delete(candidate)
			continue
		}
		break
	}
}

func (h *HyParView) integrateNodesIntoPartialView(nodes []data.Node, deleteCandidates []data.Node) {
	nodes = slices.DeleteFunc(nodes, func(node data.Node) bool {
		return node.ID == h.self.ID || slices.ContainsFunc(append(h.activeView.peers, h.passiveView.peers...), func(peer Peer) bool {
			return peer.node.ID == node.ID
		})
	})
	discardedCandidates := make([]data.Node, 0)
	for _, node := range nodes {
		if h.passiveView.full() {
			deleteCandidates = slices.DeleteFunc(deleteCandidates, func(node data.Node) bool {
				return slices.ContainsFunc(discardedCandidates, func(discarded data.Node) bool {
					return discarded.ID == node.ID
				})
			})
			passiveViewLen := len(h.passiveView.peers)
			for _, deleteCandidate := range deleteCandidates {
				h.passiveView.peers = slices.DeleteFunc(h.passiveView.peers, func(peer Peer) bool {
					return peer.node.ID == deleteCandidate.ID
				})
				discardedCandidates = append(discardedCandidates, deleteCandidate)
				if len(h.passiveView.peers) < passiveViewLen {
					break
				}
			}
			if len(h.passiveView.peers) == passiveViewLen {
				peer, err := h.passiveView.selectRandom([]string{})
				if err == nil {
					h.passiveView.delete(peer)
				}
			}
		}
		h.passiveView.peers = append(h.passiveView.peers, Peer{node: node})
	}
}

func (h *HyParView) shuffle() {
	ticker := time.NewTicker(time.Duration(h.config.ShuffleInterval) * time.Second)
	for range ticker.C {
		h.activeView.mu.Lock()
		h.passiveView.mu.Lock()
		// log.Printf("%s shuffle triggered\n", h.self.ID)
		activeViewMaxIndex := int(math.Min(float64(h.config.Ka), float64(len(h.activeView.peers))))
		passiveViewMaxIndex := int(math.Min(float64(h.config.Kp), float64(len(h.passiveView.peers))))
		peers := append(h.activeView.peers[:activeViewMaxIndex], h.passiveView.peers[:passiveViewMaxIndex]...)
		nodes := make([]data.Node, len(peers))
		for i, peer := range peers {
			nodes[i] = peer.node
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
		// log.Println(h.activeView)
		peer, err := h.activeView.selectRandom([]string{})
		if err != nil {
			log.Println("no peers in active view to perform shuffle")
			continue
		}
		// log.Println(peer)
		if peer.conn == nil {
			continue
		}
		err = peer.conn.Send(shuffleMsg)
		if err != nil {
			log.Println(err)
		}
		h.activeView.mu.Unlock()
		h.passiveView.mu.Unlock()
	}
}
