package hci

import (
	"errors"
	"testing"
	"time"
)

// The immediate-acquire path is covered by TestGetTimeoutSuccess
// (conn_more_test.go) and the done/timeout sentinel paths by
// TestGetTimeoutSentinels (error_test.go). The tests here pin the
// time.NewTimer replacement for time.After: a caller that blocks and
// is then freed by another goroutine must still acquire, and a long
// timeout passed on the fast path must not delay the return.

// TestGetTimeoutAcquireAfterWait exercises the path where the pool is
// empty at call time and a buffer is returned by another goroutine
// (the NumberOfCompletedPackets handler in real use) before the
// timeout fires.
func TestGetTimeoutAcquireAfterWait(t *testing.T) {
	c := newTxCredits(NewPool(4, 1))
	c.Get() // drain the pool; the buffer is now on c.sent

	release := make(chan struct{})
	go func() {
		<-release
		c.Put() // recycle the outstanding buffer back to the pool
	}()

	// Start the blocked acquire, then free the buffer.
	done := make(chan struct{})
	type result struct {
		err error
	}
	got := make(chan result, 1)
	go func() {
		_, err := c.GetTimeout(done, 5*time.Second)
		got <- result{err}
	}()
	close(release)

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("GetTimeout after concurrent Put = %v, want nil", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GetTimeout did not acquire the buffer freed by Put")
	}
}

// TestGetTimeoutImmediateWithLongTimeout: with a credit available, a
// huge timeout must not matter — the acquire returns at once and the
// deferred Stop releases the timer.
func TestGetTimeoutImmediateWithLongTimeout(t *testing.T) {
	c := newTxCredits(NewPool(4, 1))
	start := time.Now()
	b, err := c.GetTimeout(make(chan struct{}), time.Hour)
	if err != nil || b == nil {
		t.Fatalf("GetTimeout with credit available = (%v, %v), want a buffer", b, err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("immediate acquire took %v", elapsed)
	}
}

// TestGetTimeoutExpiresPromptly: an empty pool with a tiny timeout
// must return ErrCreditTimeout via the timer channel, not hang.
func TestGetTimeoutExpiresPromptly(t *testing.T) {
	c := newTxCredits(NewPool(4, 1))
	c.Get() // drain the pool

	start := time.Now()
	_, err := c.GetTimeout(make(chan struct{}), time.Millisecond)
	if !errors.Is(err, ErrCreditTimeout) {
		t.Fatalf("GetTimeout on exhausted pool = %v, want ErrCreditTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("timeout path took %v", elapsed)
	}
}

// TestReclaimAllDoesNotBlockBehindAnotherConnsCreditWait pins the stall the
// per-connection train mutex removes. Setup mirrors the field scenario: a
// dead connection (A) holds every credit of the shared pool, a writer on a
// healthy connection (B) is parked in its fragment train's credit wait
// exactly as writePDU parks (train mutex held, GetTimeout pending), and
// sktLoop then processes A's DisconnectionComplete — ReclaimAll(A). With the
// old shared-pool lock, ReclaimAll queued behind B's wait, freezing sktLoop
// (all connections) for the full ACLWriteTimeout; per-conn mutexes let A's
// reclaim run immediately, which in turn feeds B's wait.
func TestReclaimAllDoesNotBlockBehindAnotherConnsCreditWait(t *testing.T) {
	pool := NewPool(4, 2)
	a := newTxCredits(pool)
	b := newTxCredits(pool)

	// Connection A dies with both pool buffers in flight, un-acked.
	a.Get()
	a.Get()

	// Connection B's writer enters its fragment train: train mutex held
	// across the credit wait, like writePDU.
	bGot := make(chan error, 1)
	bWaiting := make(chan struct{})
	go func() {
		b.lock()
		defer b.unlock()
		close(bWaiting)
		_, err := b.GetTimeout(make(chan struct{}), 5*time.Second)
		bGot <- err
	}()
	<-bWaiting

	// "sktLoop": A's DisconnectionComplete arrives; its credits must be
	// reclaimed without waiting out B's credit timeout.
	reclaimed := make(chan struct{})
	go func() {
		a.ReclaimAll()
		close(reclaimed)
	}()
	select {
	case <-reclaimed:
	case <-time.After(2 * time.Second):
		t.Fatal("ReclaimAll blocked behind another connection's credit wait")
	}

	// And the reclaimed credits satisfy B's pending acquire.
	select {
	case err := <-bGot:
		if err != nil {
			t.Fatalf("GetTimeout after cross-conn reclaim = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reclaimed credits never reached the waiting connection")
	}
}
