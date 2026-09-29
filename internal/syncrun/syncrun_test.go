package syncrun

import "testing"

func TestQueueCoalesces(t *testing.T) {
	q := NewQueue()
	if _, ok := q.Take(); ok {
		t.Fatal("empty queue returned a request")
	}

	q.Push(Request{Reason: ReasonPeerNotify})
	q.Push(Request{ForceScan: true, RefreshLocal: true, Reason: ReasonManualForce})
	q.Push(Request{Reason: ReasonPeerNotify})

	<-q.C()
	got, ok := q.Take()
	want := Request{ForceScan: true, RefreshLocal: true, Reason: "peer-notify+manual-force"}
	if !ok || got != want {
		t.Fatalf("Take() = %+v, %v; want the merged force request", got, ok)
	}
	if _, ok := q.Take(); ok {
		t.Fatal("request delivered twice")
	}
}
