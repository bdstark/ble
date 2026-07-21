package darwin

import (
	"sync"
)

// eventSlot is a receiver for asynchronous events from CoreBluetooth.  To
// prevent deadlock in the case of spurious events, eventSlot discards incoming
// signals if it is not explicitly listening for them.
//
// The slot channel is buffered (capacity 1) and RxSignal never blocks. This
// is load-bearing: RxSignal runs on cbgo's dispatch-queue thread, and the
// waiter may have abandoned the slot via ctx.Done with its deferred Close
// still pending. With an unbuffered channel that interleaving parked the
// dispatch thread in the send while Close waited on the mutex the sender
// held — a mutual deadlock that froze every CoreBluetooth callback in the
// process.
type eventSlot struct {
	ch  chan any
	mtx sync.Mutex
}

// closeNoLock abandons the current listen: any delivered-but-unconsumed
// signal is discarded, and a waiter still selecting on the channel sees it
// closed (receives ok=false — waiters must treat that as "slot torn down",
// not as a successful zero-value event).
func (e *eventSlot) closeNoLock() {
	if e.ch == nil {
		return
	}

	// Drain the (possibly) buffered, unconsumed signal.
	for len(e.ch) > 0 {
		<-e.ch
	}

	close(e.ch)
	e.ch = nil
}

func (e *eventSlot) Close() {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	e.closeNoLock()
}

// Listen listens for a single event on this slot.  It returns the channel on
// which the event will be received.
func (e *eventSlot) Listen() chan any {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	if e.ch != nil {
		e.closeNoLock()
	}

	e.ch = make(chan any, 1)
	return e.ch
}

// RxSignal causes the event slot to process the given signal (i.e., it sends a
// signal to the slot). It never blocks: the signal lands in the channel's
// buffer and the slot stops listening. The channel is closed WITHOUT
// draining, so a waiter's single receive still yields the value (a receive
// on a closed channel drains the buffer first); only a subsequent Close —
// an abandoning waiter's deferred cleanup — discards it.
func (e *eventSlot) RxSignal(sig any) {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	if e.ch == nil {
		// Not listening.  Discard signal.
		return
	}

	e.ch <- sig // cap 1, one signal per Listen: never blocks
	close(e.ch)
	e.ch = nil
}

type eventConnected struct {
	conn *conn
	err  error
}

type eventRSSIRead struct {
	rssi int
	err  error
}

// Each Client owns one of these (us-as-central).
type clientEventListener struct {
	svcsDiscovered eventSlot // error
	chrsDiscovered eventSlot // error
	dscsDiscovered eventSlot // error
	chrWritten     eventSlot // error
	dscRead        eventSlot // error
	dscWritten     eventSlot // error
	notifyChanged  eventSlot // error
	rssiRead       eventSlot // *eventRSSIRead
}

func (cevl *clientEventListener) Close() {
	cevl.svcsDiscovered.Close()
	cevl.chrsDiscovered.Close()
	cevl.dscsDiscovered.Close()
	cevl.chrWritten.Close()
	cevl.dscRead.Close()
	cevl.dscWritten.Close()
	cevl.notifyChanged.Close()
	cevl.rssiRead.Close()
}

// Each Device owns one of these (us-as-peripheral).
type deviceEventListener struct {
	stateChanged eventSlot // struct{}
	connected    eventSlot // *eventConnected
	svcAdded     eventSlot // error
	advStarted   eventSlot // error
}

func (devl *deviceEventListener) Close() {
	devl.stateChanged.Close()
	devl.svcAdded.Close()
	devl.advStarted.Close()
}
