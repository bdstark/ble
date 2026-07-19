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
