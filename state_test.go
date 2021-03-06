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
	equal = equal && reflect.DeepEqual(a.Deltas, b.Deltas)
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
			equal = equal && reflect.DeepEqual(avi.C, bvi.C)
			equal = equal && reflect.DeepEqual(avi.V, bvi.V)
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
			equal = equal && reflect.DeepEqual(avi.C, bvi.C)
			equal = equal && reflect.DeepEqual(avi.V, bvi.V)
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
			"empty set, empty Deltas",
			//initial
			clusterState{logger: logger, mtx: &sync.RWMutex{}, nodes: map[mesh.PeerName]*nodeState{}},
			//in
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			//out
			clusterState{nodes: map[mesh.PeerName]*nodeState{}},
			//want
			clusterState{logger: logger, nodes: map[mesh.PeerName]*nodeState{}},
		},
		{
			"empty set, valid delta",
			//initial
			clusterState{logger: logger, mtx: &sync.RWMutex{}, nodes: map[mesh.PeerName]*nodeState{}},
			//in
			clusterState{Deltas: []delta{delta{false, 123, 3, "k1", valueInstance{1, "v1"}}}},
			//out
			clusterState{Deltas: []delta{delta{false, 123, 2, "k1", valueInstance{1, "v1"}}}},
			//want
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
				Deltas: []delta{delta{false, 123, 2, "k1", valueInstance{1, "v1"}}}},
		},
		{
			"exisiting set, valid update delta",
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
			clusterState{Deltas: []delta{delta{false, 123, 3, "k1", valueInstance{2, "v2"}}}},
			//out
			clusterState{Deltas: []delta{delta{false, 123, 2, "k1", valueInstance{2, "v2"}}}},
			//want
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
				Deltas: []delta{delta{false, 123, 2, "k1", valueInstance{2, "v2"}}}},
		},
		{
			"existing set, valid new key delta",
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
			clusterState{Deltas: []delta{delta{false, 123, 3, "k2", valueInstance{2, "v1"}}}},
			//out
			clusterState{Deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v1"}}}},
			//want
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
				Deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v1"}}}},
		},
		{
			"existing set, invalid (lower clock) update delta",
			//initial
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
			//in
			clusterState{Deltas: []delta{delta{false, 123, 3, "k2", valueInstance{1, "v1"}}}},
			//out
			clusterState{Deltas: []delta{delta{false, 123, 2, "k2", valueInstance{1, "v1"}}}},
			//want
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
				Deltas: []delta{delta{false, 123, 2, "k2", valueInstance{1, "v1"}}}},
		},
		{
			"existing set, invalid (equal clock) update delta",
			//initial
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
			//in
			clusterState{Deltas: []delta{delta{false, 123, 3, "k2", valueInstance{2, "v2"}}}},
			//out
			clusterState{Deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v2"}}}},
			//want
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
				Deltas: []delta{delta{false, 123, 2, "k2", valueInstance{2, "v2"}}}},
		},
		{
			"existing set, valid (skipped clock) update delta (requests repair)",
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
			clusterState{Deltas: []delta{delta{false, 123, 3, "k1", valueInstance{3, "v3"}}}},
			//out
			clusterState{Deltas: []delta{
				delta{true, 123, 3, "", valueInstance{2, ""}},
				delta{false, 123, 2, "k1", valueInstance{3, "v3"}},
			}},
			//want
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
				Deltas: []delta{
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
			clusterState{Deltas: []delta{
				delta{false, 123, 3, "k1", valueInstance{3, "v2"}},
				delta{false, 123, 3, "k2", valueInstance{2, "v1"}},
			}},
			//out
			clusterState{Deltas: []delta{
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
				Deltas: []delta{
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
			clusterState{Deltas: []delta{
				delta{true, 123, 3, "", valueInstance{2, ""}},
			}},
			//out
			clusterState{Deltas: []delta{
				delta{true, 123, 2, "", valueInstance{2, ""}},
			},
			},
			//want
			clusterState{
				nodes:  map[mesh.PeerName]*nodeState{},
				Deltas: []delta{delta{true, 123, 2, "", valueInstance{2, ""}}},
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
			clusterState{Deltas: []delta{
				delta{true, 123, 3, "", valueInstance{2, ""}},
			}},
			//out
			clusterState{Deltas: []delta{
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
				Deltas: []delta{
					delta{false, 123, 3, "k2", valueInstance{2, "v2"}},
				}},
		},
		{
			"existing set, ttl validation",
			//initial
			clusterState{
				logger: logger,
				mtx:    &sync.RWMutex{},
				nodes:  map[mesh.PeerName]*nodeState{},
			},
			//in
			clusterState{Deltas: []delta{
				delta{false, 123, 1, "k1", valueInstance{1, "v1"}},
				delta{true, 123, 3, "", valueInstance{2, ""}},
				delta{true, 124, 3, "", valueInstance{1, ""}},
				delta{true, 123, 3, "", valueInstance{1, ""}},
				delta{false, 123, 3, "k1", valueInstance{2, "v2"}},
				delta{false, 123, 2, "k1", valueInstance{3, "v3"}},
				delta{false, 123, 1, "k1", valueInstance{4, "v4"}},
				delta{false, 123, 1, "k1", valueInstance{7, "v5"}},
				delta{false, 123, 1, "k2", valueInstance{6, "v2"}},
				delta{false, 123, 2, "k2", valueInstance{5, "v1"}},
				delta{false, 123, 3, "k2", valueInstance{5, "v1"}},
			}},
			//out
			clusterState{Deltas: []delta{
				delta{true, 123, 2, "", valueInstance{2, ""}},
				delta{true, 124, 2, "", valueInstance{1, ""}},
				delta{false, 123, 3, "k1", valueInstance{1, "v1"}},
				delta{false, 123, 2, "k1", valueInstance{2, "v2"}},
				delta{false, 123, 1, "k1", valueInstance{3, "v3"}},
				delta{true, 123, 3, "", valueInstance{5, ""}},
				delta{true, 123, 3, "", valueInstance{6, ""}},
				delta{false, 123, 1, "k2", valueInstance{5, "v1"}},
				delta{false, 123, 2, "k2", valueInstance{5, "v1"}},
			}},
			//want
			clusterState{
				nodes: map[mesh.PeerName]*nodeState{
					123: &nodeState{
						self: 123,
						set: map[string]*valueInstance{
							"k1": &valueInstance{7, "v5"},
							"k2": &valueInstance{6, "v2"},
						},
						clock:  7,
						missed: map[int]bool{5: false, 6: false},
					}},
				Deltas: []delta{
					delta{true, 123, 2, "", valueInstance{2, ""}},
					delta{true, 124, 2, "", valueInstance{1, ""}},
					delta{false, 123, 3, "k1", valueInstance{1, "v1"}},
					delta{false, 123, 2, "k1", valueInstance{2, "v2"}},
					delta{false, 123, 1, "k1", valueInstance{3, "v3"}},
					delta{true, 123, 3, "", valueInstance{5, ""}},
					delta{true, 123, 3, "", valueInstance{6, ""}},
					delta{false, 123, 1, "k2", valueInstance{5, "v1"}},
					delta{false, 123, 2, "k2", valueInstance{5, "v1"}},
				},
			},
		},
	} {
		out := *tc.initial.Merge(&tc.in).(*clusterState)
		if !csDeepEquals(tc.initial, tc.want) {
			t.Errorf("Failed test for: %s (clusterState)", tc.description)
			t.Errorf("Check clusterState failed:\nWanted: %s\nGot: %s", spew.Sdump(tc.want), spew.Sdump(tc.initial))
		} else {
			t.Logf("Passed test for: %s (clusterState)", tc.description)
		}
		if !reflect.DeepEqual(out.Deltas, tc.out.Deltas) {
			t.Errorf("Failed test for: %s (Merge() Deltas)", tc.description)
			t.Errorf("Check Merge() output failed:\nWanted: %s\nGot: %s", spew.Sdump(tc.out.Deltas), spew.Sdump(out.Deltas))
		} else {
			t.Logf("Passed test for: %s (Merge() Deltas)", tc.description)
		}
	}
}

func TestStateEncode(t *testing.T) {
	logger := log.New(os.Stdout, "TEST ", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	for _, tc := range []struct {
		description string
		initial     clusterState
		expected    string
	}{
		{
			"empty clusterState",
			//initial
			clusterState{logger: logger, mtx: &sync.RWMutex{}, Deltas: []delta{}},
			//expected
			"{\"Deltas\":[]}",
		},
		{
			"single update delta",
			//initial
			clusterState{logger: logger, mtx: &sync.RWMutex{}, Deltas: []delta{
				delta{false, 123, 3, "k1", valueInstance{1, "v1"}},
			}},
			//expected
			"{\"Deltas\":[{\"Fix\":false,\"P\":123,\"Ttl\":3,\"K\":\"k1\",\"Vi\":{\"C\":1,\"V\":\"v1\"}}]}",
		},
		{
			"double update delta",
			//initial
			clusterState{logger: logger, mtx: &sync.RWMutex{}, Deltas: []delta{
				delta{false, 123, 3, "k1", valueInstance{1, "v1"}},
				delta{false, 123, 3, "k2", valueInstance{2, "v1"}},
			}},
			//expected
			"{\"Deltas\":[{\"Fix\":false,\"P\":123,\"Ttl\":3,\"K\":\"k1\",\"Vi\":{\"C\":1,\"V\":\"v1\"}},{\"Fix\":false,\"P\":123,\"Ttl\":3,\"K\":\"k2\",\"Vi\":{\"C\":2,\"V\":\"v1\"}}]}",
		},
	} {
		o := tc.initial.Encode()
		out := string(o[0])
		if !reflect.DeepEqual(tc.expected, out) {
			t.Errorf("Failed test for: %s (Encode() output)", tc.description)
			t.Errorf("Check Encode() failed:\nWanted: %s\nGot: %s", tc.expected, out)
		} else {
			t.Logf("Passed test for: %s (Encode() output)", tc.description)
		}
	}
}
