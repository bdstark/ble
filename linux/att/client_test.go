package att

import (
	"context"
	"encoding/binary"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bdstark/ble"
)

// fakeConn is a minimal ble.Conn whose Read delivers pre-queued PDUs.
type fakeConn struct {
	in     chan []byte
	writes chan []byte
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		in:     make(chan []byte),
		writes: make(chan []byte, 128),
	}
}

func (f *fakeConn) Read(p []byte) (int, error) {
	b, ok := <-f.in
	if !ok {
		return 0, io.EOF
	}
	return copy(p, b), nil
}

func (f *fakeConn) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	select {
	case f.writes <- b:
	default:
	}
	return len(p), nil
}

func (f *fakeConn) Close() error                   { close(f.in); return nil }
func (f *fakeConn) Context() context.Context       { return context.Background() }
func (f *fakeConn) SetContext(ctx context.Context) {}
func (f *fakeConn) LocalAddr() ble.Addr            { return nil }
func (f *fakeConn) RemoteAddr() ble.Addr           { return nil }
func (f *fakeConn) RxMTU() int                     { return ble.DefaultMTU }
func (f *fakeConn) SetRxMTU(mtu int)               {}
func (f *fakeConn) TxMTU() int                     { return ble.DefaultMTU }
func (f *fakeConn) SetTxMTU(mtu int)               {}
func (f *fakeConn) ReadRSSI() (int, error)         { return 0, nil }
func (f *fakeConn) UpdateParams(context.Context, ble.ConnParams) error {
	return nil
}
func (f *fakeConn) SetDataLength(context.Context, uint16, uint16) error {
	return nil
}
func (f *fakeConn) Disconnected() <-chan struct{} { return nil }

type checkHandler struct {
	t       *testing.T
	handled atomic.Uint64
	bad     atomic.Uint64
}

// HandleNotification validates the self-describing payload at call time.
// If a pooled buffer were recycled while still in the handler's hands, the
// fill byte would no longer match the sequence number.
func (h *checkHandler) HandleNotification(req []byte) {
	if len(req) < 7 {
		h.bad.Add(1)
		return
	}
	seq := binary.LittleEndian.Uint32(req[3:7])
	fill := byte(seq)
	for _, v := range req[7:] {
		if v != fill {
			h.bad.Add(1)
			return
		}
	}
	h.handled.Add(1)
}

// TestLoopNotificationPooling streams notifications and indications through
// Client.Loop and checks that every delivered payload is intact at the time
// of the callback (the pooled-buffer contract). Run with -race.
func TestLoopNotificationPooling(t *testing.T) {
	f := newFakeConn()
	h := &checkHandler{t: t}
	c := NewClient(f, h)
	go c.Loop()

	const total = 5000
	for i := 0; i < total; i++ {
		// A fresh slice per send: fakeConn.Read copies it on the consumer
		// side after the channel rendezvous, so reusing one here would be a
		// race in the test itself.
		pdu := make([]byte, 20)
		pdu[0] = HandleValueNotificationCode
		if i%10 == 0 {
			pdu[0] = HandleValueIndicationCode
		}
		binary.LittleEndian.PutUint16(pdu[1:3], 0x0042)
		binary.LittleEndian.PutUint32(pdu[3:7], uint32(i))
		for j := 7; j < len(pdu); j++ {
			pdu[j] = byte(i)
		}
		f.in <- pdu
	}
	f.Close()

	// Every PDU is either handed to the consumer (eventually handled) or
	// dropped-and-counted when the async queue is full. Wait for that
	// accounting to balance: it is exact regardless of scheduler load, so
	// the test asserts integrity, never throughput — the dispatcher is
	// lossy by design, and a machine-dependent floor flakes under -count=N.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h.handled.Load()+h.bad.Load()+c.AsyncDropped() == total {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if bad := h.bad.Load(); bad != 0 {
		t.Fatalf("%d notifications had corrupted payloads at callback time", bad)
	}
	handled, dropped := h.handled.Load(), c.AsyncDropped()
	if handled+dropped != total {
		t.Fatalf("handled %d + dropped %d != %d sent", handled, dropped, total)
	}
	if handled == 0 {
		t.Fatal("every notification was dropped; the dispatch path never ran")
	}
	// Indications must have been confirmed.
	select {
	case w := <-f.writes:
		if len(w) != 1 || w[0] != HandleValueConfirmationCode {
			t.Fatalf("expected confirmation write, got % X", w)
		}
	default:
		t.Fatal("no confirmation was written for indications")
	}
}

// wedgeHandler blocks every callback until release is closed.
type wedgeHandler struct {
	release chan struct{}
	handled atomic.Uint64
}

func (h *wedgeHandler) HandleNotification([]byte) {
	<-h.release
	h.handled.Add(1)
}

// TestLoopAsyncQueueDropsCounted wedges the async consumer so the 16-slot
// queue fills, then proves every further PDU — notification or incoming
// request — is dropped AND counted rather than blocking Loop. fakeConn.in
// is unbuffered, so each send returns only after Loop fully processed the
// previous PDU: once 17 PDUs are in (consumer holding at most one), the
// queue is provably full and the arithmetic below is exact up to whether
// the consumer picked up the first item (k ∈ {0,1}).
func TestLoopAsyncQueueDropsCounted(t *testing.T) {
	f := newFakeConn()
	h := &wedgeHandler{release: make(chan struct{})}
	c := NewClient(f, h)
	go c.Loop()

	notif := []byte{HandleValueNotificationCode, 0x42, 0x00, 0x01}
	for i := 0; i < 17; i++ {
		f.in <- notif
	}
	// Queue full from here: both enqueue paths must drop and count.
	for i := 0; i < 5; i++ {
		f.in <- []byte{ExchangeMTURequestCode, 0x17, 0x00} // request path
	}
	for i := 0; i < 5; i++ {
		f.in <- notif // notification path
	}
	// A send returns when Loop receives the PDU, not when it finishes the
	// drop bookkeeping for it — poll the counter to its settled value.
	const total = 27
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && c.AsyncDropped() < 10 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := c.AsyncDropped(); got < 10 || got > 11 {
		t.Fatalf("AsyncDropped = %d with a wedged consumer, want 10 or 11", got)
	}

	close(h.release)
	// Exact accounting: every PDU was handled as a notification, answered
	// as an MTU request (one response write each — the only writes in this
	// test), or dropped-and-counted. The consumer frees its one slot at an
	// arbitrary moment, so WHICH pdu got that slot varies; the sum doesn't.
	acct := func() uint64 { return h.handled.Load() + c.AsyncDropped() + uint64(len(f.writes)) }
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && acct() != total {
		time.Sleep(10 * time.Millisecond)
	}
	if got := acct(); got != total {
		t.Fatalf("handled %d + dropped %d + mtu responses %d != %d sent",
			h.handled.Load(), c.AsyncDropped(), len(f.writes), total)
	}
	f.Close()
}
