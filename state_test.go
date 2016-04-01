package gkv

import (
	"reflect"
	"sync"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/weaveworks/mesh"
)

func csDeepEquals(a, b clusterState) bool {
	equal := reflect.DeepEqual(a.self, a.self)
	equal = equal && reflect.DeepEqual(a.deltas, a.deltas)
	for p, ans := range a.nodes {
		bns := b.nodes[p]
		if bns == nil {
			return false
		}
		equal = equal && reflect.DeepEqual(ans.self, bns.self)
		equal = equal && reflect.DeepEqual(ans.clock, bns.clock)
		equal = equal && reflect.DeepEqual(ans.missed, bns.missed)
		for k, avi := range ans.set {
			bvi := bns.set[k]
			if bvi == nil {
				return false
			}
			equal = equal && reflect.DeepEqual(avi.c, bvi.c)
			equal = equal && reflect.DeepEqual(avi.v, bvi.v)
		}
	}
	for p, bns := range b.nodes {
		ans := a.nodes[p]
		if ans == nil {
			return false
		}
		equal = equal && reflect.DeepEqual(ans.self, bns.self)
		equal = equal && reflect.DeepEqual(ans.clock, bns.clock)
		equal = equal && reflect.DeepEqual(ans.missed, bns.missed)
		for k, bvi := range bns.set {
			avi := ans.set[k]
			if avi == nil {
				return false
			}
			equal = equal && reflect.DeepEqual(avi.c, bvi.c)
			equal = equal && reflect.DeepEqual(avi.v, bvi.v)
		}
	}
	return equal
}

func TestStateMergeReceived(t *testing.T) {
	for _, tc := range []struct {
		initial clusterState
		in      clusterState
		out     clusterState
		want    clusterState
	}{
		{
			// empty set, empty changes
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
		},
		{
			// empty set, valid delta
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{deltas: []delta{delta{false, 123, 3, "k1", valueInstance{1, "v1"}}}},
			clusterState{deltas: []delta{delta{false, 123, 2, "k1", valueInstance{1, "v1"}}}},
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						mtx:  &sync.RWMutex{},
						set: map[string]*valueInstance{
							"k1": &valueInstance{1, "v1"},
						},
						clock:  1,
						missed: map[int]bool{},
					}}},
		},
		{
			// exisiting set, valid update delta
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						mtx:  &sync.RWMutex{},
						set: map[string]*valueInstance{
							"k1": &valueInstance{1, "v1"},
						},
						clock:  1,
						missed: map[int]bool{},
					}}},
			clusterState{deltas: []delta{delta{false, 123, 3, "k1", valueInstance{2, "v2"}}}},
			clusterState{deltas: []delta{delta{false, 123, 2, "k1", valueInstance{2, "v2"}}}},
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{2, "v2"},
						},
						clock:  2,
						missed: map[int]bool{},
					}}},
		},
		//exiting set, valid new key delta
		//exiting set, invalid (lower clock) update delta
		//exiting set, invalid (equal clock) update delta
		//exiting set, valid (skipped clock) update delta (requests repair)
		//exiting set, valid (skipped clock) update delta (requests repair) followed by repairing update
	} {
		out := *tc.initial.Merge(&tc.in).(*clusterState)
		if !csDeepEquals(tc.initial, tc.want) {
			t.Errorf("Check clusterState failed:\nWanted: %s\nGot: %s", spew.Sdump(tc.want), spew.Sdump(tc.initial))
		}
		if !reflect.DeepEqual(out.deltas, tc.out.deltas) {
			t.Errorf("Check Merge() output failed:\nWanted: %s\nGot: %s", spew.Sdump(tc.out.deltas), spew.Sdump(out.deltas))
		}
	}
}
