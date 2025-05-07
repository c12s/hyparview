package hyparview

import (
	"log"
	"math"
	"math/rand"
	"slices"
	"time"

	"github.com/tamararankovic/hyparview/data"
	"github.com/tamararankovic/hyparview/transport"
)

type HyParView struct {
	self        data.Node
	config      HyParViewConfig
	activeView  []Peer
	passiveView []Peer
	connManager transport.ConnManager
	peerUp      chan Peer
	peerDown    chan Peer
	msgHandlers map[data.MessageType]func(received transport.MsgReceived) error
}

func NewHyParView(config HyParViewConfig, self data.Node, connManager transport.ConnManager) (HyParView, error) {
	hv := HyParView{
		self:        self,
		config:      config,
		activeView:  make([]Peer, 0),
		passiveView: make([]Peer, 0),
		peerUp:      make(chan Peer),
		peerDown:    make(chan Peer),
		connManager: connManager,
	}
	hv.msgHandlers = map[data.MessageType]func(received transport.MsgReceived) error{
		data.JOIN:            hv.onJoin,
		data.DISCONNECT:      hv.onDisconnect,
		data.FORWARD_JOIN:    hv.onForwardJoin,
		data.NEIGHTBOR:       hv.onNeighbor,
		data.NEIGHTBOR_REPLY: hv.onNeighborReply,
		data.SHUFFLE:         hv.onShuffle,
		data.SHUFFLE_REPLY:   hv.onShuffleReply,
	}
	err := connManager.StartAcceptingConns()
	go hv.shuffle()
	return hv, err
}

func (h *HyParView) Join(contactNodeAddress string) error {
	conn, err := h.connManager.Connect(contactNodeAddress)
	if err != nil {
		return err
	}
	_ = h.connManager.OnReceive(h.onReeive)
	msg := data.Message{
		Type: data.JOIN,
		Payload: data.Join{
			NodeID: h.self.ID,
		},
	}
	return conn.Send(msg)
}

func (h *HyParView) GetPeers() []Peer {
	return h.activeView
}

func (h *HyParView) OnPeerUp(handler func(peer Peer)) transport.Subscription {
	return h.connManager.OnConnUp(func(conn transport.Conn) {
		if peer := h.getPeer(conn); peer != nil {
			handler(*peer)
		}
	})
}

func (h *HyParView) OnPeerDown(handler func(peer Peer)) transport.Subscription {
	return h.connManager.OnConnDown(func(conn transport.Conn) {
		if peer := h.getPeer(conn); peer != nil {
			h.deletePeer(*peer)
			go h.replacePeer([]string{})
			go handler(*peer)
		}
	})
}

func (h *HyParView) onReeive(received transport.MsgReceived) {
	handler := h.msgHandlers[received.Msg.Type]
	if handler == nil {
		log.Printf("no handler found for message type %s", received.Msg.Type)
		return
	}
	err := handler(received)
	if err != nil {
		log.Println(err)
	}
}

func (h *HyParView) disconnectRandomPeer() error {
	disconnectPeer := h.selectRandomPeer([]string{})
	if disconnectPeer == nil {
		return nil
	}
	h.deletePeer(*disconnectPeer)
	disconnectMsg := data.Message{
		Type: data.DISCONNECT,
		Payload: data.Disconnect{
			NodeID: h.self.ID,
		},
	}
	err := disconnectPeer.conn.Send(disconnectMsg)
	if err != nil {
		return err
	}
	err = h.connManager.Disconnect(disconnectPeer.conn)
	if err != nil {
		log.Println(err)
	}
	return nil
}

func (h *HyParView) getPeer(conn transport.Conn) *Peer {
	index := slices.IndexFunc(h.activeView, func(peer Peer) bool {
		return peer.conn.GetAddress() == conn.GetAddress()
	})
	if index < 0 {
		return nil
	}
	return &h.activeView[index]
}

func (h *HyParView) getPeerCandidate(id string) *Peer {
	index := slices.IndexFunc(h.passiveView, func(peer Peer) bool {
		return peer.node.ID == id
	})
	if index < 0 {
		return nil
	}
	return &h.passiveView[index]
}

func (h *HyParView) deletePeer(peer Peer) {
	h.delete(peer, h.activeView)
}

func (h *HyParView) deletePeerCandidate(peer Peer) {
	h.delete(peer, h.passiveView)
}

func (h *HyParView) delete(peer Peer, peers []Peer) {
	index := slices.IndexFunc(peers, func(p Peer) bool {
		return p.node.ID == peer.node.ID
	})
	if index < 0 {
		return
	}
	peers = slices.Delete(peers, index, index+1)
}

func (h *HyParView) activeViewSize() int {
	return h.config.Fanout + 1
}

func (h *HyParView) activeViewFull() bool {
	return len(h.activeView) == h.activeViewSize()
}

func (h *HyParView) passiveViewFull() bool {
	return len(h.passiveView) == h.config.PassiveViewSize
}

func (h *HyParView) selectRandomPeer(nodeIdBlacklist []string) *Peer {
	return h.selectRandom(h.activeView, nodeIdBlacklist)
}

func (h *HyParView) selectRandomPeerCandidate(nodeIdBlacklist []string) *Peer {
	return h.selectRandom(h.passiveView, nodeIdBlacklist)
}

func (h *HyParView) selectRandom(peers []Peer, nodeIdBlacklist []string) *Peer {
	filteredPeers := make([]Peer, 0)
	for _, peer := range peers {
		if !slices.ContainsFunc(nodeIdBlacklist, func(id string) bool { return id == peer.node.ID }) {
			filteredPeers = append(filteredPeers, peer)
		}
	}
	if len(filteredPeers) == 0 {
		return nil
	}
	index := rand.Intn(len(filteredPeers))
	return &filteredPeers[index]
}

func (h *HyParView) replacePeer(nodeIdBlacklist []string) {
	for {
		candidate := h.selectRandomPeerCandidate(nodeIdBlacklist)
		if candidate == nil {
			log.Println("no peer candidates to replace the failed peer")
			break
		}
		conn, err := h.connManager.Connect(candidate.node.ListenAddress)
		if err != nil {
			log.Println(err)
			h.deletePeerCandidate(*candidate)
			continue
		}
		candidate.conn = conn
		neighborMsg := data.Message{
			Type: data.NEIGHTBOR,
			Payload: data.Neighbor{
				NodeID:        h.self.ID,
				ListenAddress: h.self.ListenAddress,
				HighPriority:  len(h.activeView) == 0,
			},
		}
		err = candidate.conn.Send(neighborMsg)
		if err != nil {
			log.Println(err)
			h.deletePeerCandidate(*candidate)
			continue
		}
		break
	}
}

func (h *HyParView) shuffle() {
	ticker := time.NewTicker(time.Duration(h.config.ShuffleInterval) * time.Second)
	for range ticker.C {
		log.Println("shuffle triggered")
		activeViewMaxIndex := int(math.Min(float64(h.config.Ka), float64(len(h.activeView))))
		passiveViewMaxIndex := int(math.Min(float64(h.config.Kp), float64(len(h.passiveView))))
		peers := append(h.activeView[:activeViewMaxIndex], h.passiveView[:passiveViewMaxIndex]...)
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
		peer := h.selectRandomPeer([]string{})
		if peer != nil {
			log.Println("no peers in active view to perform shuffle")
			continue
		}
		err := peer.conn.Send(shuffleMsg)
		if err != nil {
			log.Println(err)
		}
	}
}

func (h *HyParView) integrateNodesIntoPartialView(nodes []data.Node, deleteCandidates []data.Node) {
	nodes = slices.DeleteFunc(nodes, func(node data.Node) bool {
		return node.ID == h.self.ID || slices.ContainsFunc(append(h.activeView, h.passiveView...), func(peer Peer) bool {
			return peer.node.ID == node.ID
		})
	})
	discardedCandidates := make([]data.Node, 0)
	for _, node := range nodes {
		if h.passiveViewFull() {
			deleteCandidates = slices.DeleteFunc(deleteCandidates, func(node data.Node) bool {
				return slices.ContainsFunc(discardedCandidates, func(discarded data.Node) bool {
					return discarded.ID == node.ID
				})
			})
			passiveViewLen := len(h.passiveView)
			for _, deleteCandidate := range deleteCandidates {
				h.passiveView = slices.DeleteFunc(h.passiveView, func(peer Peer) bool {
					return peer.node.ID == deleteCandidate.ID
				})
				discardedCandidates = append(discardedCandidates, deleteCandidate)
				if len(h.passiveView) < passiveViewLen {
					break
				}
			}
			if len(h.passiveView) == passiveViewLen {
				peer := h.selectRandomPeerCandidate([]string{})
				h.deletePeerCandidate(*peer)
			}
		}
		h.passiveView = append(h.passiveView, Peer{node: node})
	}
}
