package hyparview

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

type HyParView struct {
	self            data.Node
	config          Config
	activeView      *PeerList
	passiveView     *PeerList
	connManager     transport.ConnManager
	msgHandlers     map[data.MessageType]func(msg []byte, sender transport.Conn) error
	peerUp          chan Peer
	peerDown        chan Peer
	stopShuffle     chan struct{}
	peerUpHandler   bool
	peerDownHandler bool
	left            bool
	mu              *sync.Mutex
	connDownSub     transport.Subscription
}

func NewHyParView(config Config, self data.Node, connManager transport.ConnManager) (*HyParView, error) {
	hv := &HyParView{
		self:   self,
		config: config,
		activeView: &PeerList{
			peers:    make([]Peer, 0),
			capacity: config.Fanout + 1,
		},
		passiveView: &PeerList{
			peers:    make([]Peer, 0),
			capacity: config.PassiveViewSize,
		},
		connManager:     connManager,
		stopShuffle:     make(chan struct{}),
		peerUpHandler:   false,
		peerDownHandler: false,
		mu:              new(sync.Mutex),
		left:            false,
	}

	data.LOG.Printf("HyParView node %s initialized at %s", self.ID, self.ListenAddress)
	hv.connDownSub = hv.onConnDown()

	hv.msgHandlers = map[data.MessageType]func(msgAny []byte, sender transport.Conn) error{
		data.JOIN:                hv.onJoin,
		data.DISCONNECT:          hv.onDisconnect,
		data.FORWARD_JOIN:        hv.onForwardJoin,
		data.FORWARD_JOIN_ACCEPT: hv.onForwardJoinAccept,
		data.NEIGHBOR:            hv.onNeighbor,
		data.NEIGHBOR_REPLY:      hv.onNeighborReply,
		data.SHUFFLE:             hv.onShuffle,
		data.SHUFFLE_REPLY:       hv.onShuffleReply,
	}

	err := connManager.StartAcceptingConns()
	go hv.shuffle()
	return hv, err
}

func (h *HyParView) Join(contactNodeID string, contactNodeAddress string) error {
	data.LOG.Printf("%s attempting to join via %s (%s)", h.self.ID, contactNodeID, contactNodeAddress)

	h.mu.Lock()
	defer h.mu.Unlock()

	if peer, err := h.activeView.getById(contactNodeID); err == nil {
		return fmt.Errorf("peer %s already in active view", peer.Node.ID)
	}

	_ = h.connManager.OnReceive(h.onReceive)

	if contactNodeAddress == "" || contactNodeAddress == "x" || contactNodeID == h.self.ID {
		return nil
	}
	conn, err := h.connManager.Connect(contactNodeAddress)
	if err != nil {
		data.LOG.Printf("%s failed to connect to %s: %v", h.self.ID, contactNodeAddress, err)
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
		data.LOG.Printf("%s failed to send JOIN message to %s: %v", h.self.ID, contactNodeID, err)
		return err
	}

	newPeer := Peer{
		Node: data.Node{
			ID:            contactNodeID,
			ListenAddress: contactNodeAddress,
		},
		Conn: conn,
	}
	h.activeView.add(newPeer, true, h.peerUp)
	data.LOG.Printf("%s successfully connected to %s", h.self.ID, contactNodeID)
	return nil
}

func (h *HyParView) Leave() {
	data.LOG.Printf("%s is leaving the network", h.self.ID)
	h.left = true
	h.connManager.StopAcceptingConns()
	data.LOG.Println("stopped accepting connections")
	h.stopShuffle <- struct{}{}
	data.LOG.Println("stopped shuffle")
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, peer := range h.activeView.peers {
		err := h.connManager.Disconnect(peer.Conn)
		if err != nil {
			data.LOG.Println(err)
		}
	}
}

func (h *HyParView) Self() data.Node {
	return h.self
}

func (h *HyParView) GetPeers(num int) []Peer {
	peers := make([]Peer, len(h.activeView.peers))
	copy(peers, h.activeView.peers)
	index := int(math.Min(float64(num), float64(len(peers))))
	return peers[:index]
}

func (h *HyParView) AddCustomMsgHandler(customMsgHandler func(msg []byte, sender transport.Conn) error) {
	h.msgHandlers[data.CUSTOM] = customMsgHandler
}

func (h *HyParView) onConnDown() transport.Subscription {
	return h.connManager.OnConnDown(func(conn transport.Conn) {
		data.LOG.Printf("%s - conn %s down", h.self.ID, conn.GetAddress())
		h.mu.Lock()
		defer h.mu.Unlock()
		data.LOG.Print("lock acquired")
		if peer, err := h.activeView.getByConn(conn); err == nil {
			data.LOG.Printf("%s - peer %s down", h.self.ID, peer.Node.ID)
			h.activeView.delete(peer, h.peerDown)
			if !h.activeView.full() && !h.left {
				h.replacePeer([]string{}, 2)
			}
		}
	})
}

func (h *HyParView) OnPeerUp(handler func(peer Peer)) {
	h.peerUp = make(chan Peer)
	h.peerUpHandler = true
	go func() {
		for peer := range h.peerUp {
			go handler(peer)
		}
	}()
}

func (h *HyParView) OnPeerDown(handler func(peer Peer)) {
	h.peerDown = make(chan Peer)
	h.peerDownHandler = true
	go func() {
		for peer := range h.peerDown {
			go handler(peer)
		}
	}()
}

func (h *HyParView) onReceive(received transport.MsgReceived) {
	if h.left {
		return
	}
	msgType := transport.GetMsgType(received.MsgBytes)
	handler := h.msgHandlers[msgType]
	if handler == nil {
		data.LOG.Printf("no handler found for message type %v", msgType)
		return
	}
	payload, err := transport.GetPayload(received.MsgBytes)
	if err != nil {
		data.LOG.Println(err)
		return
	}
	err = handler(payload, received.Sender)
	if err != nil {
		data.LOG.Println(err)
	}
}

func (h *HyParView) disconnectRandomPeer() error {
	disconnectPeer, err := h.activeView.selectRandom([]string{}, true)
	if err != nil {
		return nil
	}
	data.LOG.Printf("%s is disconnecting random peer %s", h.self.ID, disconnectPeer.Node.ID)
	h.activeView.delete(disconnectPeer, h.peerDown)
	disconnectMsg := data.Message{
		Type: data.DISCONNECT,
		Payload: data.Disconnect{
			NodeID: h.self.ID,
		},
	}
	err = disconnectPeer.Conn.Send(disconnectMsg)
	if err != nil {
		return err
	}
	return nil
}

func (h *HyParView) replacePeer(nodeIdBlacklist []string, attempts int) {
	data.LOG.Printf("%s attempting to replace failed peer", h.self.ID)
	for i := 0; i < 3; i++ {
		data.LOG.Println(i)
		candidate, err := h.passiveView.selectRandom(nodeIdBlacklist, false)
		if err != nil {
			data.LOG.Println("no peer candidates to replace the failed peer")
			break
		}
		nodeIdBlacklist = append(nodeIdBlacklist, candidate.Node.ID)
		conn, err := h.connManager.Connect(candidate.Node.ListenAddress)
		if err != nil {
			data.LOG.Println(err)
			h.passiveView.delete(candidate, nil)
			continue
		}
		neighborMsg := data.Message{
			Type: data.NEIGHBOR,
			Payload: data.Neighbor{
				NodeID:        h.self.ID,
				ListenAddress: h.self.ListenAddress,
				HighPriority:  len(h.activeView.peers) == 0,
				AttemptsLeft:  attempts,
			},
		}
		err = conn.Send(neighborMsg)
		if err != nil {
			data.LOG.Println(err)
			h.passiveView.delete(candidate, nil)
			continue
		}
		data.LOG.Printf("%s sent NEIGHBOR message to %s", h.self.ID, candidate.Node.ID)
		break
	}
}

func (h *HyParView) integrateNodesIntoPartialView(nodes []data.Node, deleteCandidates []data.Node) {
	data.LOG.Println("integrating shuffle peers into passive view")
	data.LOG.Println("before integration", "active view", h.activeView.peers, "passive view", h.passiveView.peers)
	nodes = slices.DeleteFunc(nodes, func(node data.Node) bool {
		return node.ID == h.self.ID || slices.ContainsFunc(append(h.activeView.peers, h.passiveView.peers...), func(peer Peer) bool {
			return peer.Node.ID == node.ID
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
					return peer.Node.ID == deleteCandidate.ID
				})
				discardedCandidates = append(discardedCandidates, deleteCandidate)
				if len(h.passiveView.peers) < passiveViewLen {
					break
				}
			}
			if len(h.passiveView.peers) >= passiveViewLen {
				peer, err := h.passiveView.selectRandom([]string{}, false)
				if err == nil {
					h.passiveView.delete(peer, nil)
				}
			}
		}
		if !h.passiveView.full() {
			h.passiveView.add(Peer{Node: node}, false, nil)
		}
	}
	data.LOG.Println("after integration", "active view", h.activeView.peers, "passive view", h.passiveView.peers)
}

func (h *HyParView) shuffle() {
	ticker := time.NewTicker(time.Duration(h.config.ShuffleInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			h.mu.Lock()
			data.LOG.Printf("%s shuffle triggered\n", h.self.ID)
			data.LOG.Println("before shuffle", "active view", h.activeView.peers, "passive view", h.passiveView.peers)
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
			peer, err := h.activeView.selectRandom([]string{}, true)
			if err != nil {
				h.mu.Unlock()
				data.LOG.Println("no peers in active view to perform shuffle")
				continue
			}
			err = peer.Conn.Send(shuffleMsg)
			if err != nil {
				data.LOG.Println(err)
			}
			data.LOG.Println("after shuffle", "active view", h.activeView.peers, "passive view", h.passiveView.peers)
			h.mu.Unlock()
		case <-h.stopShuffle:
			data.LOG.Println("stop shuffle")
			return
		}
	}
}
