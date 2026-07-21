package darwin

import (
	"errors"
	"testing"
	"time"
)

// The slot's send path runs on cbgo's dispatch-queue thread, so every test
// here is really about one property: RxSignal must never block, no matter
// what the waiter did.

// A delivered signal reaches a single receive even though RxSignal closed
// the channel immediately after buffering it.
func TestEventSlotDeliversThenCloses(t *testing.T) {
	var s eventSlot
	ch := s.Listen()
	want := errors.New("boom")
	s.RxSignal(want)

	itf, ok := <-ch
	if !ok || itf != want {
		t.Fatalf("receive = (%v, %v), want (%v, true)", itf, ok, want)
	}
	if _, ok := <-ch; ok {
		t.Fatal("channel not closed after the single signal")
	}
}

// RxSignal into an abandoned listen (waiter went away, Close not yet run —
// the ctx.Done interleaving) must return immediately, and the waiter's
// deferred Close must then discard the unconsumed signal so a later Listen
// starts clean.
func TestEventSlotRxSignalNeverBlocks(t *testing.T) {
	var s eventSlot
	s.Listen() // waiter abandons without receiving

	done := make(chan struct{})
	go func() {
		s.RxSignal(errors.New("late callback"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RxSignal blocked on an abandoned listen (dispatch-thread deadlock)")
	}

	s.Close() // the abandoning waiter's deferred cleanup
	ch := s.Listen()
	s.RxSignal(nil)
	if itf, ok := <-ch; !ok || itf != nil {
		t.Fatalf("fresh listen after abandoned signal = (%v, %v), want (nil, true)", itf, ok)
	}
}

// Close under a still-selecting waiter (connection teardown) must yield
// ok=false — the waiter treats that as disconnected, never as success.
func TestEventSlotCloseYieldsNotOK(t *testing.T) {
	var s eventSlot
	ch := s.Listen()
	s.Close()
	if itf, ok := <-ch; ok {
		t.Fatalf("receive after Close = (%v, %v), want ok=false", itf, ok)
	}
}

// A second Listen replaces the first: the old channel closes (its waiter
// sees ok=false) and the signal goes to the new one.
func TestEventSlotListenReplacesListen(t *testing.T) {
	var s eventSlot
	old := s.Listen()
	fresh := s.Listen()
	if _, ok := <-old; ok {
		t.Fatal("stale listen channel not closed by the replacing Listen")
	}
	s.RxSignal(nil)
	if itf, ok := <-fresh; !ok || itf != nil {
		t.Fatalf("fresh listen = (%v, %v), want (nil, true)", itf, ok)
	}
}
