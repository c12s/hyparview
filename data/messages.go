package data

type MessageType int8

const (
	JOIN MessageType = iota
	FORWARD_JOIN
	FORWARD_JOIN_ACCEPT
	DISCONNECT
	NEIGHBOR
	NEIGHBOR_REPLY
	SHUFFLE
	SHUFFLE_REPLY
	CUSTOM
)

type Message struct {
	Type    MessageType
	Payload any
}

type Join struct {
	NodeID        int64
	ListenAddress string
}

type ForwardJoin struct {
	NodeID        int64
	ListenAddress string
	TTL           int
}

type ForwardJoinAccept struct {
	NodeID        int64
	ListenAddress string
}

type Disconnect struct {
	NodeID int64
}

type Neighbor struct {
	NodeID        int64
	ListenAddress string
	AttemptsLeft  int
	HighPriority  bool
}

type NeighborReply struct {
	NodeID        int64
	ListenAddress string
	AttemptsLeft  int
	Accepted      bool
}

type Shuffle struct {
	NodeID        int64
	ListenAddress string
	Nodes         []Node
	TTL           int
}

type ShuffleReply struct {
	ReceivedNodes []Node
	Nodes         []Node
}
