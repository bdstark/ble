package att

// Tests for the ATT sequential-protocol timeout bearer-close behavior
// [Vol 3, Part F, 3.3.3]: a transaction timeout poisons the client and
// closes the bearer, so no later request can be paired with the timed-out
// transaction's late response.

import (
	"context"
	"errors"
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

	// The client is not poisoned: the next request completes normally.
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
