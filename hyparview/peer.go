package hyparview

import (
	"errors"
	"math/rand"
	"slices"
	"sync"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

type Peer struct {
	node data.Node
	conn transport.Conn
}

type PeerList struct {
	peers    []Peer
	mu       *sync.RWMutex
	capacity int
}

func (p *PeerList) full() bool {
	return len(p.peers) >= p.capacity
}

func (p *PeerList) getById(id string) (Peer, error) {
	index := slices.IndexFunc(p.peers, func(peer Peer) bool {
		return peer.node.ID == id
	})
	if index < 0 {
		return Peer{}, errors.New("tmp")
	}
	return p.peers[index], nil
}

func (p *PeerList) getByConn(conn transport.Conn) (Peer, error) {
	index := slices.IndexFunc(p.peers, func(peer Peer) bool {
		return peer.conn == conn
	})
	if index < 0 {
		return Peer{}, errors.New("tmp")
	}
	return p.peers[index], nil
}

func (p *PeerList) selectRandom(nodeIdBlacklist []string, connected bool) (Peer, error) {
	filteredPeers := make([]Peer, 0)
	for _, peer := range p.peers {
		if (!connected || peer.conn != nil) && !slices.ContainsFunc(nodeIdBlacklist, func(id string) bool { return id == peer.node.ID }) {
			filteredPeers = append(filteredPeers, peer)
		}
	}
	if len(filteredPeers) == 0 {
		return Peer{}, errors.New("tmp")
	}
	index := rand.Intn(len(filteredPeers))
	return filteredPeers[index], nil
}

func (p *PeerList) delete(peer Peer) {
	index := slices.IndexFunc(p.peers, func(p Peer) bool {
		return p.node.ID == peer.node.ID
	})
	if index < 0 {
		return
	}
	p.peers = slices.Delete(p.peers, index, index+1)
}

func (p *PeerList) add(peer Peer, connected bool) {
	if connected && peer.conn == nil {
		return
	}
	p.peers = append(p.peers, peer)
}
