package syncrun

import "testing"

func TestQueueCoalesces(t *testing.T) {
	q := NewQueue()
	if _, ok := q.Take(); ok {
		t.Fatal("empty queue returned a request")
	}

	q.Push(Request{})
	q.Push(Request{ForceScan: true, RefreshLocal: true})
	q.Push(Request{})

	<-q.C()
	got, ok := q.Take()
	if !ok || got != (Request{ForceScan: true, RefreshLocal: true}) {
		t.Fatalf("Take() = %+v, %v; want the merged force request", got, ok)
	}
	if _, ok := q.Take(); ok {
		t.Fatal("request delivered twice")
	}
}
