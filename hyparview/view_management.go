package hyparview

import (
	"maps"
	"slices"
	"time"

	"github.com/c12s/hyparview/data"
)

func (h *HyParView) disconnectRandomPeer() error {
	disconnectPeer, err := h.activeView.selectRandom([]string{})
	if err != nil {
		return nil
	}
	// h.logger.Printf("%s is disconnecting random peer %s", h.self.ID, disconnectPeer.Node.ID)
	// h.activeView.delete(disconnectPeer, h.peerDown)
	err = h.connManager.Disconnect(disconnectPeer.Conn)
	// disconnectMsg := data.Message{
	// 	Type: data.DISCONNECT,
	// 	Payload: data.Disconnect{
	// 		NodeID: h.self.ID,
	// 	},
	// }
	// err = disconnectPeer.Conn.Send(disconnectMsg)
	if err != nil {
		return err
	}
	return nil
}

func (h *HyParView) replacePeer(nodeIdBlacklist []string, attempts int) {
	h.logger.Printf("%s attempting to replace failed peer", h.self.ID)
	// nemoj slati onima kojima si vec poslao
	nodeIdBlacklist = append(nodeIdBlacklist, slices.Collect(maps.Keys(h.activeNeightbor))...)
	for {
		candidate, err := h.passiveView.selectRandom(nodeIdBlacklist)
		if err != nil {
			h.logger.Println("no peer candidates to replace the failed peer, blacklist", nodeIdBlacklist, "active neighbor", h.activeNeightbor)
			break
		}
		nodeIdBlacklist = append(nodeIdBlacklist, candidate.Node.ID)
		conn, err := h.connManager.Connect(candidate.Node.ListenAddress, h.logger)
		if err != nil {
			h.logger.Println(err)
			h.passiveView.delete(candidate)
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
		conn.Send(neighborMsg)
		// err = conn.Send(neighborMsg)
		// if err != nil {
		// 	h.logger.Println(err)
		// 	h.passiveView.delete(candidate)
		// 	continue
		// }
		// zabelezi da si poslao i cekas odgovor
		h.activeNeightbor[candidate.Node.ID] = int(time.Now().Unix())
		h.logger.Printf("%s sent NEIGHBOR message to %s", h.self.ID, candidate.Node.ID)
		break
	}
}

func (h *HyParView) integrateNodesIntoPartialView(nodes []data.Node, deleteCandidates []data.Node) {
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
				peer, err := h.passiveView.selectRandom([]string{})
				if err == nil {
					h.passiveView.delete(peer)
				}
			}
		}
		if !h.passiveView.full() {
			h.passiveView.add(Peer{Node: node})
		}
	}
}
