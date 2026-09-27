package metrics

import (
	"reflect"
	"testing"
)

func TestMergeTraffic(t *testing.T) {
	a := PeerTraffic{TotalIn: 100, CurrentIn: 10, History: []Sample{{T: 1, InBps: 1}, {T: 2, InBps: 2}}}
	b := PeerTraffic{TotalOut: 50, CurrentOut: 5, History: []Sample{{T: 2, OutBps: 3}, {T: 3, OutBps: 4}}}

	got := mergeTraffic(a, b)
	want := PeerTraffic{
		TotalIn: 100, TotalOut: 50, CurrentIn: 10, CurrentOut: 5,
		History: []Sample{{T: 1, InBps: 1}, {T: 2, InBps: 2, OutBps: 3}, {T: 3, OutBps: 4}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mergeTraffic = %+v, want %+v", got, want)
	}
}
