package att

import (
	"context"
	"encoding/binary"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-ble/ble"
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
func (f *fakeConn) ReadRSSI() int                  { return 0 }
func (f *fakeConn) Disconnected() <-chan struct{}  { return nil }

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

	// Wait until the async consumer has drained everything it was handed.
	deadline := time.Now().Add(5 * time.Second)
	var last uint64
	for time.Now().Before(deadline) {
		cur := h.handled.Load()
		if cur == last && cur > 0 {
			break
		}
		last = cur
		time.Sleep(50 * time.Millisecond)
	}

	if bad := h.bad.Load(); bad != 0 {
		t.Fatalf("%d notifications had corrupted payloads at callback time", bad)
	}
	// Loop drops notifications when the asyncWork channel is full (by
	// design), so only require that a healthy majority made it through.
	if got := h.handled.Load(); got < total/2 {
		t.Fatalf("handled %d of %d notifications; expected at least half", got, total)
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
