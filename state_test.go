package gkv

import (
	"log"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/weaveworks/mesh"
)

func csDeepEquals(a, b clusterState) bool {
	equal := reflect.DeepEqual(a.self, b.self)
	equal = equal && reflect.DeepEqual(a.deltas, b.deltas)
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
	logger := log.New(os.Stdout, "TEST ", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	for _, tc := range []struct {
		description string
		initial     clusterState
		in          clusterState
		out         clusterState
		want        clusterState
	}{
		{
			"empty set, empty deltas",
			clusterState{logger: logger, mtx: &sync.RWMutex{}, nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{logger: logger, nodes: map[mesh.PeerName]*nodeState{}},
		},
		{
			"empty set, valid delta",
			clusterState{logger: logger, mtx: &sync.RWMutex{}, nodes: map[mesh.PeerName]*nodeState{}},
			clusterState{deltas: []delta{delta{false, 123, 3, "k1", valueInstance{1, "v1"}}}},
			clusterState{deltas: []delta{delta{false, 123, 2, "k1", valueInstance{1, "v1"}}}},
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{1, "v1"},
						},
						clock:  1,
						missed: map[int]bool{},
					}},
				deltas: []delta{delta{false, 123, 2, "k1", valueInstance{1, "v1"}}}},
		},
		{
			"exisiting set, valid update delta",
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
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
					}},
				deltas: []delta{delta{false, 123, 2, "k1", valueInstance{2, "v2"}}}},
		},
		{
			"existing set, valid new key delta",
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{1, "v1"},
						},
						clock:  1,
						missed: map[int]bool{},
					}}},
			clusterState{deltas: []delta{delta{false, 123, 3, "k2", valueInstance{2, "v1"}}}},
			clusterState{deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v1"}}}},
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{1, "v1"},
							"k2": &valueInstance{2, "v1"},
						},
						clock:  2,
						missed: map[int]bool{},
					}},
				deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v1"}}}},
		},
		{
			"existing set, invalid (lower clock) update delta",
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{2, "v2"},
						},
						clock:  2,
						missed: map[int]bool{},
					}}},
			clusterState{deltas: []delta{delta{false, 123, 3, "k2", valueInstance{1, "v1"}}}},
			clusterState{deltas: []delta{delta{false, 123, 2, "k2", valueInstance{1, "v1"}}}},
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{2, "v2"},
						},
						clock:  2,
						missed: map[int]bool{},
					}},
				deltas: []delta{delta{false, 123, 2, "k2", valueInstance{1, "v1"}}}},
		},
		{
			"existing set, invalid (equal clock) update delta",
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{2, "v2"},
						},
						clock:  2,
						missed: map[int]bool{},
					}}},
			clusterState{deltas: []delta{delta{false, 123, 3, "k2", valueInstance{2, "v2"}}}},
			clusterState{deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v2"}}}},
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{2, "v2"},
						},
						clock:  2,
						missed: map[int]bool{},
					}},
				deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v2"}}}},
		},
		{
			"existing set, valid (skipped clock) update delta (requests repair)",
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{1, "v1"},
						},
						clock:  1,
						missed: map[int]bool{},
					}}},
			clusterState{deltas: []delta{delta{false, 123, 3, "k1", valueInstance{3, "v3"}}}},
			clusterState{deltas: []delta{
				delta{true, 123, 3, "", valueInstance{2, ""}},
				delta{false, 123, 2, "k1", valueInstance{3, "v3"}},
			}},
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{3, "v3"},
						},
						clock:  3,
						missed: map[int]bool{2: true},
					}},
				deltas: []delta{
					delta{true, 123, 3, "", valueInstance{2, ""}},
					delta{false, 123, 2, "k1", valueInstance{3, "v3"}},
				}},
		},
		{
			"existing set, valid (skipped clock) update delta (requests repair) followed by repairing update",
			//initial
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{1, "v1"},
						},
						clock:  1,
						missed: map[int]bool{},
					}}},
			//in
			clusterState{deltas: []delta{
				delta{false, 123, 3, "k1", valueInstance{3, "v2"}},
				delta{false, 123, 3, "k2", valueInstance{2, "v1"}},
			}},
			//out
			clusterState{deltas: []delta{
				delta{true, 123, 3, "", valueInstance{2, ""}},
				delta{false, 123, 2, "k1", valueInstance{3, "v2"}},
				delta{false, 123, 2, "k2", valueInstance{2, "v1"}},
			}},
			//want
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{3, "v2"},
							"k2": &valueInstance{2, "v1"},
						},
						clock:  3,
						missed: map[int]bool{2: false},
					}},
				deltas: []delta{
					delta{true, 123, 3, "", valueInstance{2, ""}},
					delta{false, 123, 2, "k1", valueInstance{3, "v2"}},
					delta{false, 123, 2, "k2", valueInstance{2, "v1"}},
				}},
		},
		{
			"empty set, repair request",
			//initial
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes:  map[mesh.PeerName]*nodeState{},
			},
			//in
			clusterState{deltas: []delta{
				delta{true, 123, 3, "", valueInstance{2, ""}},
			}},
			//out
			clusterState{deltas: []delta{
				delta{true, 123, 2, "", valueInstance{2, ""}},
			},
			},
			//want
			clusterState{
				nodes:  map[mesh.PeerName]*nodeState{},
				deltas: []delta{delta{true, 123, 2, "", valueInstance{2, ""}}},
			},
		},
		{
			"existing set, repair request (known)",
			//initial
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{3, "v3"},
							"k2": &valueInstance{2, "v2"},
						},
						clock:  3,
						missed: map[int]bool{},
					}}},
			//in
			clusterState{deltas: []delta{
				delta{true, 123, 3, "", valueInstance{2, ""}},
			}},
			//out
			clusterState{deltas: []delta{
				delta{false, 123, 3, "k2", valueInstance{2, "v2"}},
			}},
			//want
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{3, "v3"},
							"k2": &valueInstance{2, "v2"},
						},
						clock:  3,
						missed: map[int]bool{},
					}},
				deltas: []delta{
					delta{false, 123, 3, "k2", valueInstance{2, "v2"}},
				}},
		},
	} {
		out := *tc.initial.Merge(&tc.in).(*clusterState)
		if !csDeepEquals(tc.initial, tc.want) {
			t.Errorf("Failed test for: %s (clusterState)", tc.description)
			t.Errorf("Check clusterState failed:\nWanted: %s\nGot: %s", spew.Sdump(tc.want), spew.Sdump(tc.initial))
		} else {
			t.Logf("Passed test for: %s (clusterState)", tc.description)
		}
		if !reflect.DeepEqual(out.deltas, tc.out.deltas) {
			t.Errorf("Failed test for: %s (Merge() deltas)", tc.description)
			t.Errorf("Check Merge() output failed:\nWanted: %s\nGot: %s", spew.Sdump(tc.out.deltas), spew.Sdump(out.deltas))
		} else {
			t.Logf("Passed test for: %s (Merge() deltas)", tc.description)
		}
	}
}
