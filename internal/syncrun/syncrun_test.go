package syncrun

import "testing"

func TestQueueCoalesces(t *testing.T) {
	q := NewQueue()
	if _, ok := q.Take(); ok {
		t.Fatal("empty queue returned a request")
	}

	q.Push(Request{Reason: ReasonPeerNotify})
	q.Push(Request{RefreshLocal: true, Reason: ReasonManual})
	q.Push(Request{Reason: ReasonPeerNotify})

	<-q.C()
	got, ok := q.Take()
	want := Request{RefreshLocal: true, Reason: "peer-notify+manual"}
	if !ok || got != want {
		t.Fatalf("Take() = %+v, %v; want the merged request", got, ok)
	}
	if _, ok := q.Take(); ok {
		t.Fatal("request delivered twice")
	}
}
