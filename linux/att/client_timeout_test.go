package att

// Tests for the ATT sequential-protocol timeout bearer-close behavior
// [Vol 3, Part F, 3.3.3]: a transaction timeout poisons the client and
// closes the bearer, so no later request can be paired with the timed-out
// transaction's late response.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// closeRecordConn is an onceConn that records whether Close was called and
// can inject a Close error (the underlying conn is still closed either way,
// so Loop exits and startClient's cleanup passes).
type closeRecordConn struct {
	*onceConn
	closed   atomic.Bool
	closeErr error
}

func (c *closeRecordConn) Close() error {
	c.closed.Store(true)
	_ = c.onceConn.Close()
	return c.closeErr
}

// TestSeqProtoTimeoutClosesBearer: a request that hits the ATT transaction
// timeout must return ErrSeqProtoTimeout and close the underlying
// connection (even a failing Close is only logged, not surfaced).
func TestSeqProtoTimeoutClosesBearer(t *testing.T) {
	old := seqProtoTimeout
	seqProtoTimeout = 100 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })

	f := &closeRecordConn{onceConn: newOnceConn(), closeErr: errors.New("induced close failure")}
	c := startClient(t, f)

	// The request goes unanswered.
	if _, err := c.Read(context.Background(), 1); !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("unanswered Read = %v, want ErrSeqProtoTimeout", err)
	}
	if !f.closed.Load() {
		t.Fatal("ATT timeout did not close the bearer [Vol 3, Part F, 3.3.3]")
	}
	// startClient's cleanup additionally verifies that Loop exits cleanly
	// once the poison path has closed the connection.
}

// TestPoisonedClientFailsFast: after a timeout has poisoned the client,
// further requests and commands must fail immediately with ErrBearerClosed
// and never reach the wire.
func TestPoisonedClientFailsFast(t *testing.T) {
	old := seqProtoTimeout
	seqProtoTimeout = 100 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })

	f := &closeRecordConn{onceConn: newOnceConn()}
	c := startClient(t, f)

	if _, err := c.Read(context.Background(), 1); !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("unanswered Read = %v, want ErrSeqProtoTimeout", err)
	}
	<-f.writes // drain the timed-out request's PDU

	// A generous timeout from here on: the next request must fail fast, not
	// merely time out again.
	seqProtoTimeout = 5 * time.Second

	start := time.Now()
	_, err := c.Read(context.Background(), 2)
	elapsed := time.Since(start)
	if !errors.Is(err, ErrBearerClosed) {
		t.Fatalf("Read on poisoned client = %v, want ErrBearerClosed", err)
	}
	if elapsed > time.Second {
		t.Fatalf("poisoned request took %v, want fail-fast", elapsed)
	}

	// The command path (sendCmd) fails with the same typed error.
	if err := c.WriteCommand(context.Background(), 2, []byte{0x01}); !errors.Is(err, ErrBearerClosed) {
		t.Fatalf("WriteCommand on poisoned client = %v, want ErrBearerClosed", err)
	}

	// Nothing was written to the dead bearer.
	select {
	case w := <-f.writes:
		t.Fatalf("PDU written to a closed bearer: % X", w)
	default:
	}
}

// TestStaleResponseNotMisattributed: the end-to-end hazard. Request A times
// out; its response arrives only after request B (same opcode) would have
// been written. The pre-fix client wrote B and accepted A's stale response
// as B's — silently wrong data. The fixed client refuses B outright.
// (Reverting the bearerClosed poison flag makes this test fail via the
// "consumed a stale response" branch.)
func TestStaleResponseNotMisattributed(t *testing.T) {
	old := seqProtoTimeout
	seqProtoTimeout = 100 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })

	f := &closeRecordConn{onceConn: newOnceConn()}
	c := startClient(t, f)

	// Request A goes unanswered and times out.
	if _, err := c.Read(context.Background(), 1); !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("request A = %v, want ErrSeqProtoTimeout", err)
	}
	<-f.writes // request A's PDU

	// If request B ever reaches the wire, deliver request A's late response:
	// same opcode, so a client that wrote B would accept it as B's.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-f.writes: // request B on the wire (pre-fix behavior)
			select {
			case f.in <- []byte{ReadResponseCode, 0xAA}: // A's stale answer
			case <-done:
			}
		case <-done:
		}
	}()

	v, err := c.Read(context.Background(), 2)
	if err == nil {
		t.Fatalf("request B consumed a stale response: % X", v)
	}
	if !errors.Is(err, ErrBearerClosed) {
		t.Fatalf("request B = %v, want ErrBearerClosed", err)
	}
}

// TestCtxCancelDoesNotPoison: abandoning a request via context cancellation
// is not a protocol failure — the bearer must stay open and usable (the
// daemon cancels requests during normal shutdown and reconnect).
func TestCtxCancelDoesNotPoison(t *testing.T) {
	f := &closeRecordConn{onceConn: newOnceConn()}
	c := startClient(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-f.writes // the request reached the wire
		cancel()
	}()
	if _, err := c.Read(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Read = %v, want context.Canceled", err)
	}
	if f.closed.Load() {
		t.Fatal("ctx cancellation closed the bearer")
	}

	// The peer answers the abandoned request late; resolveAbandoned drops
	// that answer and the next request then completes normally.
	debugLogging.Store(true) // cover the debug branch of the late-rsp drop
	defer debugLogging.Store(false)
	f.in <- []byte{ReadResponseCode, 0xAA}
	go func() {
		<-f.writes
		f.in <- []byte{ReadResponseCode, 0xBB}
	}()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	v, err := c.Read(ctx2, 1)
	if err != nil || len(v) != 1 || v[0] != 0xBB {
		t.Fatalf("Read after cancellation = % X, %v", v, err)
	}
}

// cancelAfterWire abandons one Read request via context cancellation timed
// to fire only after the request PDU reached the wire, leaving the client
// with an outstanding abandoned transaction (expected response: Read).
func cancelAfterWire(t *testing.T, c *Client, f *closeRecordConn) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-f.writes // the request reached the wire
		cancel()
	}()
	if _, err := c.Read(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Read = %v, want context.Canceled", err)
	}
}

// TestAbandonedResponseNotMisattributed: the P0 hazard. Request A is
// canceled after reaching the wire; request B (same opcode) is issued while
// A's response is still in flight. B must be held off the wire until A's
// transaction resolves, then receive its own answer — never A's. (Reverting
// the resolveAbandoned call in sendReq makes this fail: B is written
// immediately and consumes A's 0xAA as its own response.)
func TestAbandonedResponseNotMisattributed(t *testing.T) {
	f := &closeRecordConn{onceConn: newOnceConn()}
	c := startClient(t, f)

	cancelAfterWire(t, c, f)

	type result struct {
		v   []byte
		err error
	}
	resc := make(chan result, 1)
	go func() {
		v, err := c.Read(context.Background(), 2)
		resc <- result{v, err}
	}()
	// B must not reach the wire while the abandoned transaction is open.
	select {
	case w := <-f.writes:
		t.Fatalf("request B written while the abandoned transaction was unresolved: % X", w)
	case <-time.After(100 * time.Millisecond):
	}
	// A's late response arrives and is dropped; B then proceeds.
	f.in <- []byte{ReadResponseCode, 0xAA}
	recvWrite(t, f.writes) // request B's PDU
	f.in <- []byte{ReadResponseCode, 0xBB}
	select {
	case r := <-resc:
		if r.err != nil || len(r.v) != 1 || r.v[0] != 0xBB {
			t.Fatalf("request B = % X, %v, want BB", r.v, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request B never completed")
	}
}

// TestAbandonedDeadlineClosesBearer: if the abandoned transaction's own
// absolute deadline passes with no response, the next request must treat it
// as the transaction timeout it is — bearer poisoned and closed
// [Vol 3, Part F, 3.3.3] — rather than writing over a dead exchange.
func TestAbandonedDeadlineClosesBearer(t *testing.T) {
	old := seqProtoTimeout
	seqProtoTimeout = 100 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })

	// closeErr: the poison path must survive (and only log) a failing Close.
	f := &closeRecordConn{onceConn: newOnceConn(), closeErr: errors.New("induced close failure")}
	c := startClient(t, f)

	cancelAfterWire(t, c, f)

	if _, err := c.Read(context.Background(), 2); !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("request after abandoned deadline = %v, want ErrSeqProtoTimeout", err)
	}
	if !f.closed.Load() {
		t.Fatal("abandoned-transaction timeout did not close the bearer")
	}
	// Nothing was written for the refused request, and the client is
	// poisoned for good.
	select {
	case w := <-f.writes:
		t.Fatalf("request written over a timed-out abandoned transaction: % X", w)
	default:
	}
	if _, err := c.Read(context.Background(), 3); !errors.Is(err, ErrBearerClosed) {
		t.Fatalf("request on poisoned client = %v, want ErrBearerClosed", err)
	}
}

// TestAbandonedResolveCtxExpiry: a request whose own ctx gives out while
// waiting for the abandoned transaction returns ctx.Err() without writing,
// and the abandoned state persists for the attempt after it.
func TestAbandonedResolveCtxExpiry(t *testing.T) {
	f := &closeRecordConn{onceConn: newOnceConn()}
	c := startClient(t, f)

	cancelAfterWire(t, c, f)

	ctxB, cancelB := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelB()
	if _, err := c.Read(ctxB, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("request during unresolved abandonment = %v, want context.DeadlineExceeded", err)
	}
	select {
	case w := <-f.writes:
		t.Fatalf("request written while the abandoned transaction was unresolved: % X", w)
	default:
	}

	// The late response finally lands; the next request completes normally.
	f.in <- []byte{ReadResponseCode, 0xAA}
	go func() {
		<-f.writes
		f.in <- []byte{ReadResponseCode, 0xBB}
	}()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	v, err := c.Read(ctx2, 1)
	if err != nil || len(v) != 1 || v[0] != 0xBB {
		t.Fatalf("Read after late resolution = % X, %v", v, err)
	}
}

// TestAbandonedResolveRefusesUnexpectedPDU: a PDU that is neither the
// abandoned transaction's response nor an Error Response (some peers issue
// ATT requests asynchronously) is refused with Request Not Supported — the
// same treatment sendReq gives it — and the wait continues.
func TestAbandonedResolveRefusesUnexpectedPDU(t *testing.T) {
	f := &closeRecordConn{onceConn: newOnceConn()}
	c := startClient(t, f)

	cancelAfterWire(t, c, f)

	type result struct {
		v   []byte
		err error
	}
	resc := make(chan result, 1)
	go func() {
		v, err := c.Read(context.Background(), 2)
		resc <- result{v, err}
	}()

	// An unrelated PDU lands mid-resolution: refused, wait continues.
	f.in <- []byte{WriteResponseCode}
	if w := recvWrite(t, f.writes); w[0] != ErrorResponseCode {
		t.Fatalf("unexpected PDU during resolution answered with % X, want an Error Response", w)
	}
	// Then A's response resolves the transaction and B proceeds.
	f.in <- []byte{ReadResponseCode, 0xAA}
	recvWrite(t, f.writes) // request B's PDU
	f.in <- []byte{ReadResponseCode, 0xBB}
	select {
	case r := <-resc:
		if r.err != nil || len(r.v) != 1 || r.v[0] != 0xBB {
			t.Fatalf("request B = % X, %v, want BB", r.v, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request B never completed")
	}
}

// TestAbandonedResolveRefusalWriteError: if refusing an unexpected PDU
// mid-resolution fails at the transport, the request errors out instead of
// looping on a dead bearer.
func TestAbandonedResolveRefusalWriteError(t *testing.T) {
	f := &failWriteConn{onceConn: newOnceConn(), okWrites: 1} // request A only
	c := startClient(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-f.writes // request A reached the wire
		cancel()
	}()
	if _, err := c.Read(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Read = %v, want context.Canceled", err)
	}

	// An unexpected PDU lands mid-resolution; the ErrReqNotSupp refusal
	// hits the induced write failure.
	go func() { f.in <- []byte{WriteResponseCode} }()
	_, err := c.Read(context.Background(), 2)
	if err == nil || !strings.Contains(err.Error(), "unexpected ATT response received") {
		t.Fatalf("resolution with a dead transport = %v, want the refusal write error", err)
	}
}

// TestAbandonedResolveTransportError: the transport dying while a request
// waits out an abandoned transaction surfaces as an error, not a hang.
func TestAbandonedResolveTransportError(t *testing.T) {
	f := &closeRecordConn{onceConn: newOnceConn()}
	c := startClient(t, f)

	cancelAfterWire(t, c, f)

	f.Close() // Loop's Read returns EOF, which lands in chErr
	if _, err := c.Read(context.Background(), 2); !errors.Is(err, io.EOF) {
		t.Fatalf("request over a dead transport = %v, want io.EOF", err)
	}
}
