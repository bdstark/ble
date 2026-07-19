package hci

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

// Pool ...
type Pool struct {
	sync.Mutex

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

// Client ...
type Client struct {
	p    *Pool
	sent chan *bytes.Buffer
}

// NewClient ...
func NewClient(p *Pool) *Client {
	return &Client{p: p, sent: make(chan *bytes.Buffer, p.cnt)}
}

// LockPool ...
func (c *Client) LockPool() {
	c.p.Lock()
}

// UnlockPool ...
func (c *Client) UnlockPool() {
	c.p.Unlock()
}

// Get returns a buffer from the shared buffer pool.
func (c *Client) Get() *bytes.Buffer {
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
func (c *Client) GetTimeout(done <-chan struct{}, d time.Duration) (*bytes.Buffer, error) {
	select {
	case b := <-c.p.ch:
		b.Reset()
		c.sent <- b
		return b, nil
	case <-done:
		return nil, fmt.Errorf("hci: connection closed while waiting for ACL buffer")
	case <-time.After(d):
		return nil, fmt.Errorf("hci: timed out waiting for ACL buffer credits (dead connection?)")
	}
}

// Put puts the oldest sent buffer back to the shared pool.
func (c *Client) Put() {
	select {
	case b := <-c.sent:
		c.p.ch <- b
	default:
	}
}

// PutAll puts all the sent buffers back to the shared pool.
func (c *Client) PutAll() {
	for {
		select {
		case b := <-c.sent:
			c.p.ch <- b
		default:
			return
		}
	}
}
