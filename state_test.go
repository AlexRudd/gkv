package gkv

import (
	"reflect"
	"testing"

	"github.com/weaveworks/mesh"
	//	. "github.com/alexrudd/gkv"
)

func TestStateMergeReceived(t *testing.T) {
	for _, testcase := range []struct {
		initial  state
		merge    state
		mergeOut state
		want     state
	}{
		{
			// empty set, valid delta
			state{set: map[mesh.PeerName]map[string]*vctClk{}},
			state{deltas: []delta{delta{repair: false, ttl: 1, vc: vctClk{o: 999, k: "k1", c: 1, val: "v1"}}}},
			state{deltas: []delta{delta{repair: false, ttl: 3, vc: vctClk{o: 999, k: "k1", c: 1, val: "v1"}}}},
			state{set: map[mesh.PeerName]map[string]*vctClk{999: map[string]*vctClk{"k1": &vctClk{o: 999, k: "k1", c: 1, val: "v1"}}}},
		},
		{
			// existing set, newer delta
			state{set: map[mesh.PeerName]map[string]*vctClk{999: map[string]*vctClk{"k1": &vctClk{o: 999, k: "k1", c: 1, val: "v1"}}}},
			state{deltas: []delta{delta{repair: false, ttl: 1, vc: vctClk{o: 999, k: "k1", c: 2, val: "v2"}}}},
			state{deltas: []delta{delta{repair: false, ttl: 3, vc: vctClk{o: 999, k: "k1", c: 2, val: "v2"}}}},
			state{set: map[mesh.PeerName]map[string]*vctClk{999: map[string]*vctClk{"k1": &vctClk{o: 999, k: "k1", c: 2, val: "v2"}}}},
		},
		{
			// existing set, older delta
			state{set: map[mesh.PeerName]map[string]*vctClk{999: map[string]*vctClk{"k1": &vctClk{o: 999, k: "k1", c: 2, val: "v2"}}}},
			state{deltas: []delta{delta{repair: false, ttl: 1, vc: vctClk{o: 999, k: "k1", c: 1, val: "v1"}}}},
			state{deltas: []delta{delta{repair: false, ttl: 0, vc: vctClk{o: 999, k: "k1", c: 1, val: "v1"}}}},
			state{set: map[mesh.PeerName]map[string]*vctClk{999: map[string]*vctClk{"k1": &vctClk{o: 999, k: "k1", c: 2, val: "v2"}}}},
		},
	} {
		target, merge := testcase.initial, testcase.merge
		out := target.Merge(&merge).(*state)
		if !reflect.DeepEqual(testcase.want.set, target.set) {
			t.Errorf("Check state failed:\ninitial: %v\nmerged: %v\nwant: %v\nhave: %v", testcase.initial, merge.deltas, testcase.want.set, target.set)
		}
		if !reflect.DeepEqual(out.deltas, testcase.mergeOut.deltas) {
			t.Errorf("Check Merge return failed:\ninitial: %v\nmerged: %v\nwant: %v\nhave: %v", testcase.initial, merge.deltas, testcase.mergeOut.deltas, out.deltas)
		}
	}
}
