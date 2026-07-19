package hci

import (
	"errors"
	"io"
	"testing"
	"time"
)

// The daemon consuming this library matches errors with errors.Is to decide
// between reconnect and process-restart recovery; these chains are contract.
func TestSentinelErrorChains(t *testing.T) {
	if !errors.Is(ErrClosed, io.ErrClosedPipe) {
		t.Error("ErrClosed must wrap io.ErrClosedPipe for legacy callers")
	}
}

func TestGetTimeoutSentinels(t *testing.T) {
	// A pool with a single buffer that is never returned: both GetTimeout
	// failure modes must yield errors.Is-able sentinels.
	c := NewClient(NewPool(4, 1))
	c.Get() // drain the only buffer

	done := make(chan struct{})
	close(done)
	if _, err := c.GetTimeout(done, time.Second); !errors.Is(err, ErrClosed) {
		t.Errorf("closed-connection wait: got %v, want errors.Is(..., ErrClosed)", err)
	}

	if _, err := c.GetTimeout(make(chan struct{}), time.Millisecond); !errors.Is(err, ErrCreditTimeout) {
		t.Errorf("credit wait timeout: got %v, want errors.Is(..., ErrCreditTimeout)", err)
	}
}
