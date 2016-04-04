package gkv

import (
	"log"

	"encoding/json"

	"github.com/weaveworks/mesh"
)

// Peer encapsulates state and implements mesh.Gossiper.
// It should be passed to mesh.Router.NewGossip,
// and the resulting Gossip registered in turn,
// before calling mesh.Router.Start.
type peer struct {
	cs     *clusterState
	send   mesh.Gossip
	logger *log.Logger
}

// peer implements mesh.Gossiper.
var _ mesh.Gossiper = &peer{}

// Construct a peer with empty state.
// Be sure to register a channel, later,
// so we can make outbound communication.
func newPeer(self mesh.PeerName, logger *log.Logger) *peer {
	return &peer{
		cs:     newClusterState(self, logger),
		send:   nil, // must .register() later
		logger: logger,
	}
}

// register the result of a mesh.Router.NewGossip.
func (p *peer) register(send mesh.Gossip) {
	p.send = send
}

// Return a copy of our complete state.
func (p *peer) Gossip() (complete mesh.GossipData) {
	return p.cs.copyDeltas()
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (p *peer) OnGossip(buf []byte) (delta mesh.GossipData, err error) {
	deltas := clusterState{}
	err = json.Unmarshal(buf, &deltas)
	if err != nil {
		return nil, err
	}

	return p.cs.Merge(&deltas), nil
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (p *peer) OnGossipBroadcast(src mesh.PeerName, buf []byte) (received mesh.GossipData, err error) {
	return p.OnGossip(buf)
}

// Merge the gossiped data represented by buf into our state.
func (p *peer) OnGossipUnicast(src mesh.PeerName, buf []byte) error {
	_, err := p.OnGossip(buf)
	return err
}
