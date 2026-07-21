package att

// Tests for the client's roundTrip response-validation arms that the
// existing client tests do not reach, plus an end-to-end exercise of the
// att.Client against the att.Server over an in-memory bearer — the two
// halves of this package acting as each other's oracle.

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bdstark/ble"
)

// respondWith queues one canned response for the next request written to f.
func respondWith(f *onceConn, rsp []byte) {
	go func() {
		<-f.writes
		f.in <- rsp
	}()
}

// TestRoundTripInvalidResponses drives malformed responses through every
// distinct validation arm of the client's roundTrip helper.
func TestRoundTripInvalidResponses(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)
	ctx := context.Background()

	// Response shorter than the method's minimum length.
	respondWith(f, []byte{FindInformationResponseCode, 0x01, 0x01, 0x00, 0x00})
	if _, _, err := c.FindInformation(ctx, 1, 0xFFFF); err != ErrInvalidResponse {
		t.Errorf("FindInformation short rsp = %v, want ErrInvalidResponse", err)
	}
	respondWith(f, []byte{PrepareWriteResponseCode, 0x03, 0x00, 0x00})
	if _, _, _, err := c.PrepareWrite(ctx, 3, 0, []byte{1}); err != ErrInvalidResponse {
		t.Errorf("PrepareWrite short rsp = %v, want ErrInvalidResponse", err)
	}

	// Method-specific validation past the shared envelope: incomplete
	// entries for the declared format/length.
	respondWith(f, []byte{FindInformationResponseCode, 0x01, 0x01, 0x00, 0x00, 0x28, 0xFF})
	if _, _, err := c.FindInformation(ctx, 1, 0xFFFF); err != ErrInvalidResponse {
		t.Errorf("FindInformation format-1 ragged rsp = %v, want ErrInvalidResponse", err)
	}
	respondWith(f, append([]byte{FindInformationResponseCode, 0x02}, make([]byte, 17)...))
	if _, _, err := c.FindInformation(ctx, 1, 0xFFFF); err != ErrInvalidResponse {
		t.Errorf("FindInformation format-2 ragged rsp = %v, want ErrInvalidResponse", err)
	}
	// An undefined format byte must be rejected too: the caller derives the
	// entry stride from it and would walk off the end of the data list.
	respondWith(f, []byte{FindInformationResponseCode, 0x03, 0x01, 0x00, 0x00, 0x28})
	if _, _, err := c.FindInformation(ctx, 1, 0xFFFF); err != ErrInvalidResponse {
		t.Errorf("FindInformation unknown-format rsp = %v, want ErrInvalidResponse", err)
	}
	respondWith(f, []byte{ReadByTypeResponseCode, 0x04, 0x01, 0x00, 0xAA})
	if _, _, err := c.ReadByType(ctx, 1, 0xFFFF, ble.UUID16(0x2A00)); err != ErrInvalidResponse {
		t.Errorf("ReadByType ragged rsp = %v, want ErrInvalidResponse", err)
	}
	respondWith(f, []byte{ReadByGroupTypeResponseCode, 0x06, 0x01, 0x00, 0x05})
	if _, _, err := c.ReadByGroupType(ctx, 1, 0xFFFF, ble.UUID16(0x2800)); err != ErrInvalidResponse {
		t.Errorf("ReadByGroupType ragged rsp = %v, want ErrInvalidResponse", err)
	}

	// A peer-declared entry length of zero must be rejected, not divided
	// by: the bare modulo panicked the central with integer divide-by-zero
	// on the connect-time discovery path.
	// Four bytes clears the minLen-4 envelope so the length byte reaches
	// the modulo; 0x00 would divide by zero without the guard.
	respondWith(f, []byte{ReadByTypeResponseCode, 0x00, 0xAA, 0xBB})
	if _, _, err := c.ReadByType(ctx, 1, 0xFFFF, ble.UUID16(0x2A00)); err != ErrInvalidResponse {
		t.Errorf("ReadByType zero-length rsp = %v, want ErrInvalidResponse", err)
	}
	respondWith(f, []byte{ReadByGroupTypeResponseCode, 0x00, 0xAA, 0xBB})
	if _, _, err := c.ReadByGroupType(ctx, 1, 0xFFFF, ble.UUID16(0x2800)); err != ErrInvalidResponse {
		t.Errorf("ReadByGroupType zero-length rsp = %v, want ErrInvalidResponse", err)
	}

	// A well-formed ATT error response surfaces as its typed error, a
	// malformed one as ErrInvalidResponse — also on methods that return
	// only an error.
	respondWith(f, []byte{ErrorResponseCode, WriteRequestCode, 0x03, 0x00, 0x03})
	if err := c.Write(ctx, 3, []byte{1}); err != ble.ErrWriteNotPerm {
		t.Errorf("Write ATT error = %v, want ErrWriteNotPerm", err)
	}
	respondWith(f, []byte{ErrorResponseCode, WriteRequestCode, 0x03})
	if err := c.Write(ctx, 3, []byte{1}); err != ErrInvalidResponse {
		t.Errorf("Write malformed error rsp = %v, want ErrInvalidResponse", err)
	}
	respondWith(f, []byte{ErrorResponseCode, ExecuteWriteRequestCode, 0x00, 0x00, 0x09})
	if err := c.ExecuteWrite(ctx, 1); err != ble.ErrPrepQueueFull {
		t.Errorf("ExecuteWrite ATT error = %v, want ErrPrepQueueFull", err)
	}

	// Format-2 information data of exactly one entry is accepted.
	respondWith(f, append([]byte{FindInformationResponseCode, 0x02}, make([]byte, 18)...))
	format, data, err := c.FindInformation(ctx, 1, 0xFFFF)
	if err != nil || format != 2 || len(data) != 18 {
		t.Errorf("FindInformation format-2 = %d, %d bytes, %v", format, len(data), err)
	}
}

// pipeConn is one end of an in-memory duplex ATT bearer. Both ends share a
// done channel, so closing either side unblocks both loops.
type pipeConn struct {
	in, out   chan []byte
	done      chan struct{}
	closeOnce *sync.Once
	rx, tx    atomic.Int64
}

func newPipe(clientRxMTU, serverRxMTU int) (cli, srv *pipeConn) {
	c2s := make(chan []byte, 64)
	s2c := make(chan []byte, 64)
	done := make(chan struct{})
	once := &sync.Once{}
	cli = &pipeConn{in: s2c, out: c2s, done: done, closeOnce: once}
	srv = &pipeConn{in: c2s, out: s2c, done: done, closeOnce: once}
	cli.rx.Store(int64(clientRxMTU))
	cli.tx.Store(ble.DefaultMTU)
	srv.rx.Store(int64(serverRxMTU))
	srv.tx.Store(ble.DefaultMTU)
	return cli, srv
}

func (p *pipeConn) Read(b []byte) (int, error) {
	select {
	case pdu := <-p.in:
		return copy(b, pdu), nil
	case <-p.done:
		return 0, io.EOF
	}
}

func (p *pipeConn) Write(b []byte) (int, error) {
	pdu := append([]byte(nil), b...)
	select {
	case p.out <- pdu:
		return len(b), nil
	case <-p.done:
		return 0, io.ErrClosedPipe
	}
}

func (p *pipeConn) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	return nil
}

func (p *pipeConn) Context() context.Context       { return context.Background() }
func (p *pipeConn) SetContext(ctx context.Context) {}
func (p *pipeConn) LocalAddr() ble.Addr            { return nil }
func (p *pipeConn) RemoteAddr() ble.Addr           { return nil }
func (p *pipeConn) RxMTU() int                     { return int(p.rx.Load()) }
func (p *pipeConn) SetRxMTU(mtu int)               { p.rx.Store(int64(mtu)) }
func (p *pipeConn) TxMTU() int                     { return int(p.tx.Load()) }
func (p *pipeConn) SetTxMTU(mtu int)               { p.tx.Store(int64(mtu)) }
func (p *pipeConn) ReadRSSI() (int, error)         { return 0, nil }
func (p *pipeConn) UpdateParams(context.Context, ble.ConnParams) error {
	return nil
}
func (p *pipeConn) SetDataLength(context.Context, uint16, uint16) error {
	return nil
}
func (p *pipeConn) Disconnected() <-chan struct{} { return nil }

// valueRecorder collects handler-written values under a lock (the server
// runs in its own goroutine).
type valueRecorder struct {
	mu   sync.Mutex
	data []byte
}

func (r *valueRecorder) set(b []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = append([]byte(nil), b...)
}

func (r *valueRecorder) get() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.data...)
}

// TestClientServerRoundTrip runs the package's client against the
// package's server over an in-memory bearer: discovery, reads (including
// long-value truncation and blob offsets), writes, prepared writes, the
// MTU exchange, and error responses.
func TestClientServerRoundTrip(t *testing.T) {
	long := make([]byte, 40)
	for i := range long {
		long[i] = byte(0x40 + i)
	}
	rec := &valueRecorder{}

	svc := ble.NewService(ble.UUID16(0x1815))
	dyn := svc.NewCharacteristic(ble.UUID16(0x2A56))
	dyn.HandleRead(ble.ReadHandlerFunc(func(req ble.Request, w ble.ResponseWriter) {
		w.Write([]byte{0x2A})
	}))
	dyn.HandleWrite(ble.WriteHandlerFunc(func(req ble.Request, w ble.ResponseWriter) {
		rec.set(req.Data())
	}))
	svc.NewCharacteristic(ble.UUID16(0x2A57)).SetValue(long)
	// DB layout at base 1: h1 svc, h2/h3 dynamic char, h4/h5 static char.

	cliConn, srvConn := newPipe(185, 185)
	s, err := NewServer(NewDB([]*ble.Service{svc}, 1), srvConn)
	if err != nil {
		t.Fatal(err)
	}
	srvDone := make(chan struct{})
	go func() { s.Loop(); close(srvDone) }()

	c := NewClient(cliConn, nopHandler{})
	cliDone := make(chan struct{})
	go func() { c.Loop(); close(cliDone) }()

	defer func() {
		cliConn.Close()
		for _, done := range []chan struct{}{srvDone, cliDone} {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("a loop did not exit on close")
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Service discovery.
	length, data, err := c.ReadByGroupType(ctx, 1, 0xFFFF, ble.UUID16(0x2800))
	if err != nil || length != 6 {
		t.Fatalf("ReadByGroupType = %d, %v", length, err)
	}
	wantBytes(t, data, 0x01, 0x00, 0xFF, 0xFF, 0x15, 0x18)

	// Characteristic discovery.
	length, data, err = c.ReadByType(ctx, 1, 0xFFFF, ble.UUID16(0x2803))
	if err != nil || length != 7 {
		t.Fatalf("ReadByType = %d, %v", length, err)
	}
	wantBytes(t, data,
		0x02, 0x00, 0x0E, 0x03, 0x00, 0x56, 0x2A,
		0x04, 0x00, 0x02, 0x05, 0x00, 0x57, 0x2A)

	// Descriptor discovery.
	format, data, err := c.FindInformation(ctx, 1, 0xFFFF)
	if err != nil || format != 1 || len(data) != 5*4 {
		t.Fatalf("FindInformation = %d, %d bytes, %v", format, len(data), err)
	}

	// Dynamic read, write, and write-command.
	if v, err := c.Read(ctx, 3); err != nil || len(v) != 1 || v[0] != 0x2A {
		t.Fatalf("Read(3) = % X, %v", v, err)
	}
	if err := c.Write(ctx, 3, []byte("hi")); err != nil {
		t.Fatalf("Write = %v", err)
	}
	wantBytes(t, rec.get(), 'h', 'i')
	if err := c.WriteCommand(ctx, 3, []byte("yo")); err != nil {
		t.Fatalf("WriteCommand = %v", err)
	}
	waitCond(t, func() bool { return string(rec.get()) == "yo" })

	// Prepared write round trip.
	h, off, part, err := c.PrepareWrite(ctx, 3, 0, []byte("ab"))
	if err != nil || h != 3 || off != 0 || string(part) != "ab" {
		t.Fatalf("PrepareWrite = %d, %d, %q, %v", h, off, part, err)
	}
	if err := c.ExecuteWrite(ctx, 0x01); err != nil {
		t.Fatalf("ExecuteWrite = %v", err)
	}
	wantBytes(t, rec.get(), 'a', 'b')

	// Long static value at the default MTU: truncated read, then the
	// remainder via blob offset.
	head, err := c.Read(ctx, 5)
	if err != nil || len(head) != ble.DefaultMTU-1 {
		t.Fatalf("Read(5) = %d bytes, %v; want %d", len(head), err, ble.DefaultMTU-1)
	}
	tail, err := c.ReadBlob(ctx, 5, uint16(len(head)))
	if err != nil {
		t.Fatalf("ReadBlob = %v", err)
	}
	if got := append(head, tail...); string(got) != string(long) {
		t.Fatalf("reassembled long value = % X", got)
	}

	// MTU exchange, then the long value fits a single read.
	mtu, err := c.ExchangeMTU(ctx, 185)
	if err != nil || mtu != 185 {
		t.Fatalf("ExchangeMTU = %d, %v", mtu, err)
	}
	if v, err := c.Read(ctx, 5); err != nil || string(v) != string(long) {
		t.Fatalf("Read(5) after MTU exchange = %d bytes, %v", len(v), err)
	}

	// Error responses come back typed.
	if _, err := c.Read(ctx, 0x63); err != ble.ErrInvalidHandle {
		t.Fatalf("Read(bad handle) = %v, want ErrInvalidHandle", err)
	}
	if _, err := c.ReadMultiple(ctx, []uint16{2, 4}); err != ble.ErrReqNotSupp {
		t.Fatalf("ReadMultiple = %v, want ErrReqNotSupp", err)
	}
}
