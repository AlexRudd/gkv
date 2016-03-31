package gkv

import (
	"fmt"
	"github.com/weaveworks/mesh"
	"sync"
)

type state struct {
	mtx    sync.RWMutex
	self   mesh.PeerName
	set    map[mesh.PeerName]map[string]*vctClk
	deltas []delta
}

func (this state) String() string {
	return fmt.Sprintf("{ self: %v, set: %v, deltas: %v}", this.self, this.set, this.deltas)
}

type vctClk struct {
	o   mesh.PeerName
	k   string
	c   int
	val string
}

func (this vctClk) String() string {
	return fmt.Sprintf("{ vc: \"[%v][%v][%v]\", val: %v}", this.o, this.k, this.c, this.val)
}

type delta struct {
	repair bool
	ttl    int
	vc     vctClk
}

// state implements GossipData.
var _ mesh.GossipData = &state{}

// Construct an empty state object, ready to receive updates.
// This is suitable to use at program start.
// Other peers will populate us with data.
func newState(self mesh.PeerName) *state {
	return &state{
		set:  map[mesh.PeerName]map[string]*vctClk{},
		self: self,
	}
}

func (this *state) deltaState() *state {
	return &state{deltas: this.deltas}
}

// Encode serializes our complete state to a slice of byte-slices.
// In this simple example, we use a single JSON-encoded buffer.
func (this *state) Encode() [][]byte {
	return nil
}

// Merge merges the other GossipData into this one,
// and returns our resulting, complete state.
func (this *state) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	for _, d := range other.(*state).deltas {
		if !d.repair {
			if this.set[d.vc.o] == nil {
				// New peer
				this.set[d.vc.o] = map[string]*vctClk{}
			}
			// check for existing k/v
			current := this.set[d.vc.o][d.vc.k]
			if current != nil {
				// k/v exists
				if current.c < d.vc.c {
					// received more recent delta
					this.set[d.vc.o][d.vc.k] = &d.vc
					d.ttl = 3
					this.deltas = append(this.deltas, d)
				} else {
					d.ttl = d.ttl - 1
					if d.ttl >= 0 {
						this.deltas = append(this.deltas, d)
					}
				}
			} else {
				// received new vector clock
				this.set[d.vc.o][d.vc.k] = &d.vc
				d.ttl = 3
				this.deltas = append(this.deltas, d)
			}
		} else {
			// repair request
		}
	}
	return this.deltaState()
}
