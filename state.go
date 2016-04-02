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
	mtx    *sync.RWMutex
}

type nodeState struct {
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
		mtx:   &sync.RWMutex{},
	}
}

func newNodeState(self mesh.PeerName) *nodeState {
	return &nodeState{
		self:   self,
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

func (vi *valueInstance) copy() *valueInstance {
	return &valueInstance{
		c: vi.c,
		v: vi.v,
	}
}

// Encode serializes the changes that have been made to this state
func (cs *clusterState) Encode() [][]byte {
	return nil
}

// Merge merges the other GossipData into this one
func (cs *clusterState) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	// get write lock
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	// loop through all recieved deltas
	n := len(other.(*clusterState).deltas)
	for i, d := range other.(*clusterState).deltas {
		if !d.fix {
			// is update
			if cs.nodes[d.p] == nil {
				// node did not exist
				cs.nodes[d.p] = newNodeState(d.p)
				// update
				cs.logger.Printf("%v/%v deltas: new node with key: %v->%v->%v:%v", i+1, n, d.p, d.k, d.vi.c, d.vi.v)
				cs.nodes[d.p].set[d.k] = d.vi.copy()
				cs.nodes[d.p].clock = d.vi.c
				d.ttl = d.ttl - 1
				if d.ttl > 0 {
					cs.deltas = append(cs.deltas, d)
				}
			} else if d.vi.c > cs.nodes[d.p].clock {
				// is new`update, check if clock has skipped
				for j := cs.nodes[d.p].clock + 1; j < d.vi.c; j++ {
					cs.nodes[d.p].missed[j] = true
					cs.logger.Printf("Missed delta clock %v for node %v", j, d.p)
					f := delta{
						fix: true,
						p:   d.p,
						ttl: 3,
						vi:  valueInstance{c: j},
					}
					cs.deltas = append(cs.deltas, f)
				}
				// and update
				cs.logger.Printf("%v/%v deltas: update key: %v->%v->%v:%v", i+1, n, d.p, d.k, d.vi.c, d.vi.v)
				cs.nodes[d.p].set[d.k] = d.vi.copy()
				cs.nodes[d.p].clock = d.vi.c
				d.ttl = d.ttl - 1
				if d.ttl > 0 {
					cs.deltas = append(cs.deltas, d)
				}
			} else {
				// old update
				if cs.nodes[d.p].missed[d.vi.c] {
					// missing update!
					if cs.nodes[d.p].set[d.k] == nil || d.vi.c > cs.nodes[d.p].set[d.k].c {
						// key doesn't exist or has a lower clock
						cs.logger.Printf("%v/%v deltas: repair key: %v->%v->%v:%v", i+1, n, d.p, d.k, d.vi.c, d.vi.v)
						cs.nodes[d.p].set[d.k] = d.vi.copy()
						d.ttl = d.ttl - 1
						if d.ttl > 0 {
							cs.deltas = append(cs.deltas, d)
						}
					} else {
						// stale repair
						cs.logger.Printf("%v/%v deltas: stale repair: %v->%v->%v:%v", i+1, n, d.p, d.k, d.vi.c, d.vi.v)
						d.ttl = d.ttl - 1
						if d.ttl > 0 {
							cs.deltas = append(cs.deltas, d)
						}
					}
					cs.nodes[d.p].missed[d.vi.c] = false
				} else {
					// repair not needed
					cs.logger.Printf("%v/%v deltas: already consistent: %v->%v->%v:%v", i+1, n, d.p, d.k, d.vi.c, d.vi.v)
					d.ttl = d.ttl - 1
					if d.ttl > 0 {
						cs.deltas = append(cs.deltas, d)
					}
				}
			}
		} else {
			// repair request!
			if cs.nodes[d.p] == nil {
				// node did not exist, pass on request
				cs.logger.Printf("%v/%v deltas: repair request for unknown node %v", i+1, n, d.p)
				d.ttl = d.ttl - 1
				if d.ttl > 0 {
					cs.deltas = append(cs.deltas, d)
				}
			} else {
				// see if we have key with said clock
				found := false
				for k, vi := range cs.nodes[d.p].set {
					if vi.c == d.vi.c {
						// found key!
						cs.logger.Printf("%v/%v deltas: repair request fulfilled: %v->%v->%v:%v", i+1, n, d.p, k, vi.c, vi.v)
						r := delta{
							p:   d.p,
							ttl: 3,
							k:   k,
							vi: valueInstance{
								c: vi.c,
								v: vi.v,
							},
						}
						// send out repair
						cs.deltas = append(cs.deltas, r)
						found = true
						break
					}
				}
				if !found {
					cs.logger.Printf("%v/%v deltas: repair request for unknown clock %v", i+1, n, d.vi.c)
					d.ttl = d.ttl - 1
					if d.ttl > 0 {
						cs.deltas = append(cs.deltas, d)
					}
				}
			}
		}
	}
	return cs.copyDeltas()
}
