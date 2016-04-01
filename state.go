package gkv

import (
	"github.com/weaveworks/mesh"
	"log"
	"sync"
)

type clusterState struct {
	self   mesh.PeerName
	nodes  map[mesh.PeerName]*nodeState
	deltas []delta
	logger *log.Logger
}

type nodeState struct {
	mtx    *sync.RWMutex
	self   mesh.PeerName
	set    map[string]*valueInstance
	clock  int
	missed map[int]bool
}

type valueInstance struct {
	c int
	v string
}

type delta struct {
	fix bool
	p   mesh.PeerName
	ttl int
	k   string
	vi  valueInstance
}

// state implements GossipData.
var _ mesh.GossipData = &clusterState{}

// Construct an empty state object, ready to receive updates.
// This is suitable to use at program start.
// Other peers will populate us with data.
func newState(self mesh.PeerName) *clusterState {
	return &clusterState{
		self:  self,
		nodes: map[mesh.PeerName]*nodeState{},
	}
}

func newNodeState(self mesh.PeerName) *nodeState {
	return &nodeState{
		self:   self,
		mtx:    &sync.RWMutex{},
		set:    map[string]*valueInstance{},
		clock:  0,
		missed: map[int]bool{},
	}
}

func (cs *clusterState) copyDeltas() *clusterState {
	return &clusterState{
		deltas: cs.deltas,
	}
}

// Encode serializes the changes that have been made to this state
func (cs *clusterState) Encode() [][]byte {
	return nil
}

// Merge merges the other GossipData into this one
func (cs *clusterState) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	//loop through all recieved deltas
	cs.logger.Printf("Merging %v recieved deltas", len(other.(*clusterState).deltas))
	for _, d := range other.(*clusterState).deltas {
		if !d.fix {
			// is update
			if cs.nodes[d.p] == nil {
				// node did not exist
				cs.logger.Printf("Creating new cluster node: %v", d.p)
				cs.nodes[d.p] = newNodeState(d.p)
				// update
				cs.logger.Printf("Setting key: %v->%v->%v:%v", d.p, d.k, d.vi.c, d.vi.v)
				cs.nodes[d.p].set[d.k] = &d.vi
				cs.nodes[d.p].clock = d.vi.c
				d.ttl = d.ttl - 1
				cs.deltas = append(cs.deltas, d)
				// skip this iteration
				continue
			}

			if d.vi.c > cs.nodes[d.p].clock {
				// is new`update, check if clock has skipped
				for i := cs.nodes[d.p].clock + 1; i < d.vi.c; i++ {
					cs.nodes[d.p].missed[i] = true
					f := delta{
						fix: true,
						p:   d.p,
						ttl: 3,
						vi:  valueInstance{c: i},
					}
					cs.deltas = append(cs.deltas, f)
				}
				// and update
				cs.logger.Printf("Setting key: %v->%v->%v:%v", d.p, d.k, d.vi.c, d.vi.v)
				cs.nodes[d.p].set[d.k] = &d.vi
				cs.nodes[d.p].clock = d.vi.c
				d.ttl = d.ttl - 1
				cs.deltas = append(cs.deltas, d)
			} else {
				// old update
				if cs.nodes[d.p].missed[d.vi.c] {
					// missing update!
					if cs.nodes[d.p].set[d.k] == nil || d.vi.c > cs.nodes[d.p].set[d.k].c {
						// key doesn't exist or has a lower clock
						cs.logger.Printf("Setting key: %v->%v->%v:%v", d.p, d.k, d.vi.c, d.vi.v)
						cs.nodes[d.p].set[d.k] = &d.vi
						d.ttl = d.ttl - 1
						cs.deltas = append(cs.deltas, d)
					}
					cs.nodes[d.p].missed[d.vi.c] = false
				} else {
					d.ttl = d.ttl - 1
					cs.deltas = append(cs.deltas, d)
				}
			}
		} else {
			//repair request!
		}
	}
	return cs.copyDeltas()
}
