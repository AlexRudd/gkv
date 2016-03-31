package gkvold

import (
	"fmt"
	"sync"

	"github.com/weaveworks/mesh"
)

type state struct {
	mtx    sync.RWMutex
	self   mesh.PeerName
	set    map[mesh.PeerName]map[string]*vectorclock
	deltas []delta
}

func (st *state) String() string {
	return fmt.Sprintf("{ self: %v, set: %v, deltas: %v}", st.self, st.set, st.deltas)
}

type vectorclock struct {
	o   mesh.PeerName
	k   string
	c   int
	val string
}

func (vc *vectorclock) String() string {
	return fmt.Sprintf("{ vc: \"[%v][%v][%v]\", val: %v}", vc.o, vc.k, vc.c, vc.val)
}

type delta struct {
	repair bool
	ttl    int
	vc     vectorclock
}

// state implements GossipData.
var _ mesh.GossipData = &state{}

// Construct an empty state object, ready to receive updates.
// This is suitable to use at program start.
// Other peers will populate us with data.
func newState(self mesh.PeerName) *state {
	return &state{
		set:  map[mesh.PeerName]map[string]*vectorclock{},
		self: self,
	}
}

func (st *state) deltaState() *state {
	return &state{deltas: st.deltas}
}

// Encode serializes the changes that have been made to this state
func (st *state) Encode() [][]byte {
	return nil
}

// Merge merges the other GossipData into this one
func (st *state) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	for _, d := range other.(*state).deltas {
		if !d.repair {
			if st.set[d.vc.o] == nil {
				// New peer
				st.set[d.vc.o] = map[string]*vectorclock{}
			}
			// check for existing k/v
			current := st.set[d.vc.o][d.vc.k]
			if current != nil {
				// k/v exists
				if current.c < d.vc.c {
					// received more recent delta
					st.set[d.vc.o][d.vc.k] = &d.vc
					d.ttl = 3
					st.deltas = append(st.deltas, d)
				} else {
					d.ttl = d.ttl - 1
					if d.ttl >= 0 {
						st.deltas = append(st.deltas, d)
					}
				}
			} else {
				// received new vector clock
				st.set[d.vc.o][d.vc.k] = &d.vc
				d.ttl = 3
				st.deltas = append(st.deltas, d)
			}
		} else {
			// repair request
		}
	}
	return st.deltaState()
}
