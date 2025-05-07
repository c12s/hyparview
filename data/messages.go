package data

type MessageType int8

const (
	JOIN MessageType = iota
	FORWARD_JOIN
	DISCONNECT
	NEIGHTBOR
	NEIGHTBOR_REPLY
	SHUFFLE
	SHUFFLE_REPLY
)

type Message struct {
	Type    MessageType
	Payload any
}

type Join struct {
	NodeID        string
	ListenAddress string
}

type ForwardJoin struct {
	NodeID        string
	ListenAddress string
	TTL           int
}

type Disconnect struct {
	NodeID string
}

type Neighbor struct {
	NodeID        string
	ListenAddress string
	HighPriority  bool
}

type NeighborReply struct {
	NodeID        string
	ListenAddress string
	Accepted      bool
}

type Shuffle struct {
	NodeID        string
	ListenAddress string
	Nodes         []Node
	TTL           int
}

type ShuffleReply struct {
	ReceivedNodes []Node
	Nodes         []Node
}
