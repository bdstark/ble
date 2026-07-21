package att

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bdstark/ble"
)

// waitCond polls cond until it holds or 2s elapse.
func waitCond(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

// mtuRecordConn records the last SetTxMTU value.
type mtuRecordConn struct {
	*onceConn
	txMTU atomic.Int64
}

// signalCloseConn defers the actual close of f.in to the feeder goroutine
// (the sole sender): Close only raises closeSignal, so the bearer-poison
// path's close-at-timeout cannot race a send in flight on f.in.
type signalCloseConn struct {
	*onceConn
	closeSignal chan struct{}
	signalOnce  sync.Once
}

func (c *signalCloseConn) Close() error {
	c.signalOnce.Do(func() { close(c.closeSignal) })
	return nil
}

func (c *mtuRecordConn) SetTxMTU(mtu int) { c.txMTU.Store(int64(mtu)) }

// TestLoopDropsUnclaimedResponse: responses to abandoned requests must not
// park the read loop. The first unclaimed response sits in rspc's single
// buffer slot; the second is dropped and counted; Loop keeps serving.
func TestLoopDropsUnclaimedResponse(t *testing.T) {
	debugLogging.Store(true) // cover the debug branch of the drop path
	defer debugLogging.Store(false)

	f := newOnceConn()
	c := startClient(t, f)

	if got := c.RspDropped(); got != 0 {
		t.Fatalf("RspDropped = %d on a fresh client, want 0", got)
	}

	// Two unsolicited responses while no request is pending.
	f.in <- []byte{WriteResponseCode}
	f.in <- []byte{WriteResponseCode}
	waitCond(t, func() bool { return c.RspDropped() == 1 })

	// Loop must still be alive and a normal request must succeed: sendReq
	// first drains the buffered stale response, then sees the real one.
	go func() {
		<-f.writes // the read request
		f.in <- []byte{ReadResponseCode, 0xCC}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := c.Read(ctx, 1)
	if err != nil || len(v) != 1 || v[0] != 0xCC {
		t.Fatalf("Read after unclaimed responses = % X, %v", v, err)
	}
	if got := c.RspDropped(); got != 1 {
		t.Fatalf("RspDropped = %d after successful request, want 1", got)
	}
}

// TestMalformedMTURequestKeepsTxBuf: a malformed incoming MTU request is
// rejected without permanently draining chTxBuf — a later request must
// still be able to acquire the tx buffer.
func TestMalformedMTURequestKeepsTxBuf(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)

	// Undersized advertised MTU (22 < 23).
	f.in <- []byte{ExchangeMTURequestCode, 22, 0}
	if w := recvWrite(t, f.writes); len(w) != 5 || w[0] != ErrorResponseCode || w[4] != byte(ble.ErrInvalidPDU) {
		t.Fatalf("undersized MTU request response = % X", w)
	}

	// Truncated request (2 bytes instead of 3).
	f.in <- []byte{ExchangeMTURequestCode, 23}
	if w := recvWrite(t, f.writes); len(w) != 5 || w[0] != ErrorResponseCode {
		t.Fatalf("truncated MTU request response = % X", w)
	}

	// The tx buffer must have been released on both error paths.
	go func() {
		<-f.writes // the read request
		f.in <- []byte{ReadResponseCode, 0xDD}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := c.Read(ctx, 1)
	if err != nil || len(v) != 1 || v[0] != 0xDD {
		t.Fatalf("Read after malformed MTU requests = % X, %v", v, err)
	}
}

// TestExchangeMTUServerBelowMinimum: a server answering with an MTU below
// the spec minimum must fail the exchange with ErrInvalidMTU instead of
// shrinking the tx buffer and panicking on a later request.
func TestExchangeMTUServerBelowMinimum(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)

	go func() {
		<-f.writes // the MTU request
		f.in <- []byte{ExchangeMTUResponseCode, 3, 0}
	}()
	_, err := c.ExchangeMTU(context.Background(), 185)
	if !errors.Is(err, ErrInvalidMTU) {
		t.Fatalf("ExchangeMTU with server MTU 3 = %v, want ErrInvalidMTU", err)
	}

	// The client must remain usable: the tx buffer was restored intact and
	// a fixed-header request (txBuf[:3]) must not panic.
	go func() {
		<-f.writes
		f.in <- []byte{ReadResponseCode, 0xEE}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := c.Read(ctx, 1)
	if err != nil || len(v) != 1 || v[0] != 0xEE {
		t.Fatalf("Read after rejected MTU exchange = % X, %v", v, err)
	}
}

// TestExchangeMTUServerOversizedCapped: a server advertising more than
// ble.MaxMTU is capped at ble.MaxMTU.
func TestExchangeMTUServerOversizedCapped(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)

	go func() {
		<-f.writes // the MTU request
		rsp := []byte{ExchangeMTUResponseCode, 0, 0}
		binary.LittleEndian.PutUint16(rsp[1:3], 1000)
		f.in <- rsp
	}()
	mtu, err := c.ExchangeMTU(context.Background(), 185)
	if err != nil || mtu != ble.MaxMTU {
		t.Fatalf("ExchangeMTU with server MTU 1000 = %d, %v, want %d capped", mtu, err, ble.MaxMTU)
	}
}

// TestMTURequestOversizedClamped: the server-side handler must clamp the
// peer's advertised MTU at ble.MaxMTU before adopting it as the tx MTU.
func TestMTURequestOversizedClamped(t *testing.T) {
	f := &mtuRecordConn{onceConn: newOnceConn()}
	c := NewClient(f, nopHandler{})

	rsp := c.handleExchangeMTURequest([]byte{ExchangeMTURequestCode, 0xFF, 0xFF})
	if len(rsp) != 3 || rsp[0] != ExchangeMTUResponseCode {
		t.Fatalf("MTU response = % X", rsp)
	}
	if got := f.txMTU.Load(); got != ble.MaxMTU {
		t.Fatalf("SetTxMTU got %d, want clamped %d", got, ble.MaxMTU)
	}
	// The reallocated tx buffer matches the clamped MTU and was released.
	select {
	case buf := <-c.chTxBuf:
		if len(buf) != ble.MaxMTU {
			t.Fatalf("tx buffer len = %d, want %d", len(buf), ble.MaxMTU)
		}
		c.chTxBuf <- buf
	default:
		t.Fatal("tx buffer was not released after the MTU request")
	}
}

// TestSendReqAbsoluteDeadline: the ATT transaction timeout is absolute — a
// chatty peer streaming unexpected PDUs must not extend it.
func TestSendReqAbsoluteDeadline(t *testing.T) {
	old := seqProtoTimeout
	seqProtoTimeout = 200 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })

	f := &signalCloseConn{onceConn: newOnceConn(), closeSignal: make(chan struct{})}
	c := startClient(t, f)

	// Unexpected PDUs every 40ms, far more often than the timeout. With a
	// per-iteration timer each PDU would restart the clock and the request
	// would never time out. The feeder is the sole closer of f.in (after it
	// stops sending); the timeout's bearer close only raises closeSignal.
	stop := make(chan struct{})
	feederDone := make(chan struct{})
	go func() {
		defer close(feederDone)
		defer f.onceConn.Close()
		<-f.writes // the read request
		for {
			select {
			case <-stop:
				return
			case <-f.closeSignal:
				return
			case f.in <- []byte{WriteResponseCode}:
			}
			time.Sleep(40 * time.Millisecond)
		}
	}()

	start := time.Now()
	_, err := c.Read(context.Background(), 1)
	elapsed := time.Since(start)
	close(stop)
	<-feederDone

	if !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("Read under chatty peer = %v, want ErrSeqProtoTimeout", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("deadline was extended by unexpected PDUs: took %v", elapsed)
	}
}

// TestPrepareWriteSendReqError pins PrepareWrite's sendReq-error arm: an
// unanswered request must surface ErrSeqProtoTimeout through PrepareWrite.
func TestPrepareWriteSendReqError(t *testing.T) {
	old := seqProtoTimeout
	seqProtoTimeout = 50 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })

	f := newOnceConn()
	c := startClient(t, f)
	go func() { <-f.writes }() // swallow the request; never respond

	if _, _, _, err := c.PrepareWrite(context.Background(), 3, 0, []byte{0xAB}); !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("PrepareWrite = %v, want ErrSeqProtoTimeout", err)
	}
}

// countNotifyHandler counts deliveries and records the last payload length.
type countNotifyHandler struct {
	calls   atomic.Uint64
	lastLen atomic.Int64
}

func (h *countNotifyHandler) HandleNotification(req []byte) {
	h.lastLen.Store(int64(len(req)))
	h.calls.Add(1)
}

// TestLoopDropsZeroLengthPDU: a header-only L2CAP frame reaches Loop as
// (0, nil). Loop must drop it rather than classify on the stale first byte
// of the previous PDU — with a request pending, the stale byte reads as a
// response opcode and the resulting empty PDU used to panic sendReq's
// rsp[0] access.
func TestLoopDropsZeroLengthPDU(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Prime rxBuf[0] with ReadResponseCode via a successful request.
	go func() {
		<-f.writes
		f.in <- []byte{ReadResponseCode, 0xAA}
	}()
	if v, err := c.Read(ctx, 1); err != nil || len(v) != 1 || v[0] != 0xAA {
		t.Fatalf("priming Read = % X, %v", v, err)
	}

	// While the next request is pending, deliver a zero-length PDU (stale
	// rxBuf[0] still holds ReadResponseCode), then the real response.
	go func() {
		<-f.writes
		f.in <- []byte{}
		f.in <- []byte{ReadResponseCode, 0xBB}
	}()
	v, err := c.Read(ctx, 1)
	if err != nil || len(v) != 1 || v[0] != 0xBB {
		t.Fatalf("Read after zero-length PDU = % X, %v, want BB, nil", v, err)
	}
}

// TestLoopDropsRuntNotificationAndIndication: notification/indication PDUs
// below the spec minimum (opcode + 2-byte handle) must be dropped before
// dispatch — the gatt dispatcher parses the handle unconditionally — and a
// runt indication must still be confirmed so the peer's sequential protocol
// isn't left waiting.
func TestLoopDropsRuntNotificationAndIndication(t *testing.T) {
	f := newOnceConn()
	h := &countNotifyHandler{}
	c := NewClient(f, h)
	done := make(chan struct{})
	go func() { c.Loop(); close(done) }()
	t.Cleanup(func() {
		f.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("client Loop did not exit")
		}
	})

	f.in <- []byte{HandleValueNotificationCode}     // opcode only
	f.in <- []byte{HandleValueIndicationCode, 0x42} // truncated handle
	if w := recvWrite(t, f.writes); w[0] != HandleValueConfirmationCode {
		t.Fatalf("runt indication answered with % X, want confirmation", w)
	}

	// A well-formed notification must still be delivered — and be the only
	// delivery, proving the runts never reached the handler.
	f.in <- []byte{HandleValueNotificationCode, 0x42, 0x00, 0xAB}
	waitCond(t, func() bool { return h.calls.Load() == 1 })
	if got := h.lastLen.Load(); got != 4 {
		t.Errorf("delivered payload length = %d, want 4", got)
	}
}
