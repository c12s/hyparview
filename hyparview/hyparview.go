package hyparview

import (
	"log"
	"math"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

type HyParView struct {
	self              data.Node
	config            Config
	activeView        *PeerList
	passiveView       *PeerList
	connManager       transport.ConnManager
	msgHandlers       map[data.MessageType]func(msg []byte, sender transport.Conn) error
	clientMsgHandlers map[data.MessageType]func(msg []byte, sender Peer)
	peerUpHandler     func(Peer)
	peerDownHandler   func(Peer)
	left              bool
	logger            *log.Logger
	activeNeightbor   map[string]int
	joinCh            chan JoinCmd
	leaveCh           chan LeaveCmd
	msgCh             chan MsgEvent
	connDownCh        chan ConnDownEvent
	shuffleCh         chan ShuffleEvent
	stopShuffleCh     chan ShuffleEvent
	peerUpCh          chan PeerUpEvent
	peerDownCh        chan PeerDownEvent
	rcvSub            transport.Subscription
	AllowAny          bool
}

func NewHyParView(config Config, self data.Node, connManager transport.ConnManager, logger *log.Logger) (*HyParView, error) {
	h := &HyParView{
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
		connManager:       connManager,
		peerUpHandler:     func(p Peer) {},
		peerDownHandler:   func(p Peer) {},
		left:              true,
		logger:            logger,
		activeNeightbor:   make(map[string]int),
		clientMsgHandlers: make(map[data.MessageType]func(msg []byte, sender Peer)),
		joinCh:            make(chan JoinCmd, 100),
		leaveCh:           make(chan LeaveCmd, 100),
		msgCh:             make(chan MsgEvent, 100),
		connDownCh:        make(chan ConnDownEvent, 100),
		shuffleCh:         make(chan ShuffleEvent, 100),
		stopShuffleCh:     make(chan ShuffleEvent, 100),
		peerUpCh:          make(chan PeerUpEvent, 100),
		peerDownCh:        make(chan PeerDownEvent, 100),
	}
	h.msgHandlers = map[data.MessageType]func(msgAny []byte, sender transport.Conn) error{
		data.JOIN:                h.onJoin,
		data.DISCONNECT:          h.onDisconnect,
		data.FORWARD_JOIN:        h.onForwardJoin,
		data.FORWARD_JOIN_ACCEPT: h.onForwardJoinAccept,
		data.NEIGHBOR:            h.onNeighbor,
		data.NEIGHBOR_REPLY:      h.onNeighborReply,
		data.SHUFFLE:             h.onShuffle,
		data.SHUFFLE_REPLY:       h.onShuffleReply,
	}
	go h.processEvents()
	h.logger.Printf("HyParView node %s initialized at %s", self.ID, self.ListenAddress)
	return h, nil
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

func (h *HyParView) AddClientMsgHandler(msgType data.MessageType, handler func(msg []byte, sender Peer)) {
	h.clientMsgHandlers[msgType] = handler
}

func (h *HyParView) OnPeerUp(handler func(peer Peer)) {
	if handler == nil {
		handler = func(peer Peer) {}
	}
	h.peerUpHandler = handler
}

func (h *HyParView) OnPeerDown(handler func(peer Peer)) {
	if handler == nil {
		handler = func(peer Peer) {}
	}
	h.peerDownHandler = handler
}
