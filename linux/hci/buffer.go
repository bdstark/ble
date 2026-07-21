package hci

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

// Pool ...
type Pool struct {
	sz  int
	cnt int
	ch  chan *bytes.Buffer
}

// NewPool ...
func NewPool(sz int, cnt int) *Pool {
	ch := make(chan *bytes.Buffer, cnt)
	for len(ch) < cnt {
		ch <- bytes.NewBuffer(make([]byte, sz))
	}
	return &Pool{sz: sz, cnt: cnt, ch: ch}
}

// txCredits is a connection's view of the shared ACL TX buffer pool: it
// hands out flow-controlled buffers and tracks the ones in flight so they
// can be reclaimed when the controller acknowledges them (or the connection
// tears down). It is not a BLE client — despite the historical name it
// replaces, it has nothing to do with att/gatt/ble.Client.
type txCredits struct {
	// mu serializes this connection's fragment trains (writePDU holds it
	// across a whole PDU, per [Vol 3, Part A, 7.2.1] — fragments of one
	// PDU must not interleave with another PDU on the SAME logical
	// transport) and teardown's ReclaimAll. It is deliberately
	// per-connection, not the shared pool's: sktLoop runs ReclaimAll, and
	// one connection's 20s credit wait must never block sktLoop — that
	// stalls every connection's events, including the very
	// NumberOfCompletedPackets that would refill the pool.
	mu   sync.Mutex
	p    *Pool
	sent chan *bytes.Buffer
}

// newTxCredits returns a credit tracker over the shared pool p.
func newTxCredits(p *Pool) *txCredits {
	return &txCredits{p: p, sent: make(chan *bytes.Buffer, p.cnt)}
}

// lock/unlock guard this connection's fragment-train critical section
// (writePDU holds it across a whole fragment train). Single-step reclaim
// should use ReclaimAll, which locks internally.
func (c *txCredits) lock()   { c.mu.Lock() }
func (c *txCredits) unlock() { c.mu.Unlock() }

// Get returns a buffer from the shared buffer pool, blocking indefinitely.
// Production TX uses GetTimeout; this remains for tests that need to park
// credits on a connection.
func (c *txCredits) Get() *bytes.Buffer {
	b := <-c.p.ch
	b.Reset()
	c.sent <- b
	return b
}

// GetTimeout returns a buffer from the shared buffer pool, giving up
// when done closes or after the timeout elapses. ACL buffers are
// returned only by NumberOfCompletedPackets events from the
// controller; on a connection that died without a processed
// disconnect event those never arrive, and a bare Get would block its
// caller forever.
func (c *txCredits) GetTimeout(done <-chan struct{}, d time.Duration) (*bytes.Buffer, error) {
	// time.NewTimer + Stop instead of time.After: a buffer is usually
	// available immediately, and time.After would leave a live timer
	// (and its channel) around for the full duration on every ACL TX
	// fragment.
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case b := <-c.p.ch:
		b.Reset()
		c.sent <- b
		return b, nil
	case <-done:
		return nil, fmt.Errorf("connection closed while waiting for ACL buffer: %w", ErrClosed)
	case <-t.C:
		return nil, ErrCreditTimeout
	}
}

// Put puts the oldest sent buffer back to the shared pool.
func (c *txCredits) Put() {
	select {
	case b := <-c.sent:
		c.p.ch <- b
	default:
	}
}

// ReclaimAll returns every in-flight buffer to the shared pool. It takes the
// connection's train mutex, so it cannot drain c.sent while a writePDU train
// on the SAME connection is mid-flight — buffers that train still holds stay
// out of the pool until it finishes. Liveness: both teardown paths close
// chDone (closeChans) before calling this, so a train parked in GetTimeout
// wakes through its done arm and releases the mutex promptly rather than
// after the full credit timeout. Trains on OTHER connections are unaffected
// — they hold their own mutex, never this one.
func (c *txCredits) ReclaimAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		select {
		case b := <-c.sent:
			c.p.ch <- b
		default:
			return
		}
	}
}
