package gkv

import (
	"encoding/json"
	"errors"
	"github.com/weaveworks/mesh"
	"log"
	"sync"
)

type clusterState struct {
	self   mesh.PeerName
	nodes  map[mesh.PeerName]*nodeState
	Deltas []delta
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
	C int
	V string
}

type delta struct {
	Fix bool
	P   mesh.PeerName
	Ttl int
	K   string
	Vi  valueInstance
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
		Deltas: cs.Deltas,
	}
}

func (vi *valueInstance) copy() *valueInstance {
	return &valueInstance{
		C: vi.C,
		V: vi.V,
	}
}

func (cs *clusterState) Set(key, value string) {
	// get write lock
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	// set key
	cs.nodes[cs.self].set[key] = &valueInstance{
		C: cs.nodes[cs.self].clock + 1,
		V: value,
	}
	// create delta
	cs.Deltas = append(cs.Deltas, delta{
		Fix: true,
		P:   cs.self,
		Ttl: 3,
		Vi:  *cs.nodes[cs.self].set[key],
	})
	// update clock
	cs.nodes[cs.self].clock++
}

func (cs *clusterState) Get(node mesh.PeerName, key string) (string, error) {
	// get read lock
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	// check node exists
	ns := cs.nodes[node]
	if ns == nil {
		return "", errors.New("node not found")
	}
	// check key exists
	vi := ns.set[key]
	if vi == nil {
		return "", errors.New("key not found")
	} else {
		return vi.V, nil
	}
}

// Encode serializes the changes that have been made to this state
func (cs *clusterState) Encode() [][]byte {
	// get write lock
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	// copy and clear deltas
	out := cs.copyDeltas()
	cs.Deltas = nil
	// encode
	cs.logger.Printf("Encoding %v deltas", len(out.Deltas))
	b, err := json.Marshal(out)
	if err != nil {
		cs.logger.Printf("Error encoding clusterState deltas: %v", err)
	}
	return [][]byte{b}
}

// Merge merges the deltas from the other clusterState into this one.
// If deltas are determined to be missing, then Fix requests are sent out
func (cs *clusterState) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	// get write lock
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	// loop through all recieved deltas
	n := len(other.(*clusterState).Deltas)
	for i, d := range other.(*clusterState).Deltas {
		if !d.Fix {
			// is update
			if cs.nodes[d.P] == nil {
				// node did not exist
				cs.nodes[d.P] = newNodeState(d.P)
				// update
				cs.logger.Printf("%v/%v deltas: new node with key: %v->%v->%v:%v", i+1, n, d.P, d.K, d.Vi.C, d.Vi.V)
				cs.nodes[d.P].set[d.K] = d.Vi.copy()
				cs.nodes[d.P].clock = d.Vi.C
				d.Ttl = d.Ttl - 1
				if d.Ttl > 0 {
					cs.Deltas = append(cs.Deltas, d)
				}
			} else if d.Vi.C > cs.nodes[d.P].clock {
				// is new`update, check if clock has skipped
				for j := cs.nodes[d.P].clock + 1; j < d.Vi.C; j++ {
					cs.nodes[d.P].missed[j] = true
					cs.logger.Printf("Missed delta clock %v for node %v", j, d.P)
					f := delta{
						Fix: true,
						P:   d.P,
						Ttl: 3,
						Vi:  valueInstance{C: j},
					}
					cs.Deltas = append(cs.Deltas, f)
				}
				// and update
				cs.logger.Printf("%v/%v deltas: update key: %v->%v->%v:%v", i+1, n, d.P, d.K, d.Vi.C, d.Vi.V)
				cs.nodes[d.P].set[d.K] = d.Vi.copy()
				cs.nodes[d.P].clock = d.Vi.C
				d.Ttl = d.Ttl - 1
				if d.Ttl > 0 {
					cs.Deltas = append(cs.Deltas, d)
				}
			} else {
				// old update
				if cs.nodes[d.P].missed[d.Vi.C] {
					// missing update!
					if cs.nodes[d.P].set[d.K] == nil || d.Vi.C > cs.nodes[d.P].set[d.K].C {
						// key doesn't exist or has a lower clock
						cs.logger.Printf("%v/%v deltas: repair key: %v->%v->%v:%v", i+1, n, d.P, d.K, d.Vi.C, d.Vi.V)
						cs.nodes[d.P].set[d.K] = d.Vi.copy()
						d.Ttl = d.Ttl - 1
						if d.Ttl > 0 {
							cs.Deltas = append(cs.Deltas, d)
						}
					} else {
						// stale repair
						cs.logger.Printf("%v/%v deltas: stale repair: %v->%v->%v:%v", i+1, n, d.P, d.K, d.Vi.C, d.Vi.V)
						d.Ttl = d.Ttl - 1
						if d.Ttl > 0 {
							cs.Deltas = append(cs.Deltas, d)
						}
					}
					cs.nodes[d.P].missed[d.Vi.C] = false
				} else {
					// repair not needed
					cs.logger.Printf("%v/%v deltas: already consistent: %v->%v->%v:%v", i+1, n, d.P, d.K, d.Vi.C, d.Vi.V)
					d.Ttl = d.Ttl - 1
					if d.Ttl > 0 {
						cs.Deltas = append(cs.Deltas, d)
					}
				}
			}
		} else {
			// repair request!
			if cs.nodes[d.P] == nil {
				// node did not exist, pass on request
				cs.logger.Printf("%v/%v deltas: repair request for unknown node %v", i+1, n, d.P)
				d.Ttl = d.Ttl - 1
				if d.Ttl > 0 {
					cs.Deltas = append(cs.Deltas, d)
				}
			} else {
				// see if we have key with said clock
				found := false
				for k, vi := range cs.nodes[d.P].set {
					if vi.C == d.Vi.C {
						// found key!
						cs.logger.Printf("%v/%v deltas: repair request fulfilled: %v->%v->%v:%v", i+1, n, d.P, k, vi.C, vi.V)
						r := delta{
							P:   d.P,
							Ttl: 3,
							K:   k,
							Vi: valueInstance{
								C: vi.C,
								V: vi.V,
							},
						}
						// send out repair
						cs.Deltas = append(cs.Deltas, r)
						found = true
						break
					}
				}
				if !found {
					cs.logger.Printf("%v/%v deltas: repair request for unknown clock %v", i+1, n, d.Vi.C)
					d.Ttl = d.Ttl - 1
					if d.Ttl > 0 {
						cs.Deltas = append(cs.Deltas, d)
					}
				}
			}
		}
	}
	return cs.copyDeltas()
}
