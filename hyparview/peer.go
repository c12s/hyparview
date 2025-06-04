package hyparview

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

type Peer struct {
	Node data.Node
	Conn transport.Conn
}

type PeerList struct {
	peers    []Peer
	capacity int
}

func (p *PeerList) full() bool {
	return len(p.peers) >= p.capacity
}

func (p *PeerList) overflow() bool {
	return len(p.peers) > p.capacity
}

func (p *PeerList) getById(id string) (Peer, error) {
	index := slices.IndexFunc(p.peers, func(peer Peer) bool {
		return peer.Node.ID == id
	})
	if index < 0 {
		return Peer{}, fmt.Errorf("peer %s not found", id)
	}
	return p.peers[index], nil
}

func (p *PeerList) getByConn(conn transport.Conn) (Peer, error) {
	index := slices.IndexFunc(p.peers, func(peer Peer) bool {
		return peer.Conn.GetAddress() == conn.GetAddress()
	})
	if index < 0 {
		return Peer{}, errors.New("tmp")
	}
	return p.peers[index], nil
}

func (p *PeerList) selectRandom(nodeIdBlacklist []string, connected bool) (Peer, error) {
	filteredPeers := make([]Peer, 0)
	for _, peer := range p.peers {
		if (!connected || peer.Conn != nil) && !slices.ContainsFunc(nodeIdBlacklist, func(id string) bool { return id == peer.Node.ID }) {
			filteredPeers = append(filteredPeers, peer)
		}
	}
	if len(filteredPeers) == 0 {
		return Peer{}, errors.New("tmp")
	}
	index := rand.Intn(len(filteredPeers))
	return filteredPeers[index], nil
}

func (p *PeerList) delete(peer Peer, notify chan Peer) {
	index := slices.IndexFunc(p.peers, func(p Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	if index < 0 {
		return
	}
	p.peers = slices.Delete(p.peers, index, index+1)
	if notify != nil {
		notify <- peer
	}
}

func (p *PeerList) add(peer Peer, connected bool, notify chan Peer) {
	if connected && peer.Conn == nil {
		return
	}
	if slices.ContainsFunc(p.peers, func(p Peer) bool {
		return p.Node.ID == peer.Node.ID
	}) {
		return
	}
	p.peers = append(p.peers, peer)
	if notify != nil {
		notify <- peer
	}
}
