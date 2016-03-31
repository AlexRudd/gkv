package gkv

import (
	"fmt"
	"sync"

	"github.com/weaveworks/mesh"
)

type clusterState struct {
	self  mesh.PeerName
	nodes map[mesh.PeerName]*nodeState
}

type nodeState struct {
	mtx    *sync.RWMutex
	self   mesh.PeerName
	set    map[string]*valueInstance
	deltas []delta
}

type valueInstance struct {
	c int
	v string
}

type delta struct {
	p   mesh.PeerName
	ttl int
	vi  valueInstance
}

// state implements GossipData.
var _ mesh.GossipData = &clusterState{}

// Construct an empty state object, ready to receive updates.
// This is suitable to use at program start.
// Other peers will populate us with data.
func newState(self mesh.PeerName) *clusterState {
	return &clusterState{
		self: self,
		set:  map[mesh.PeerName]*nodeState{},
	}
}

func (cs *clusterState) String() string {
	return fmt.Sprintf("{ self: %v, set: %v, deltas: %v}", cs.self, cs.set, cs.deltas)
}

func (cs *clusterState) copy() *clusterState {
	return &clusterState{deltas: st.deltas}
}

// Encode serializes the changes that have been made to this state
func (cs *clusterState) Encode() [][]byte {
	return nil
}

// Merge merges the other GossipData into this one
func (cs *clusterState) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	return nil
}
