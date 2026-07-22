package att

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bdstark/ble"
)

// debugLogging toggles the level of the process-wide test logger. It is an
// atomic because goroutines leaked by earlier tests (e.g. a client Loop
// parked in Read) may still consult the logger; the logger itself is
// installed exactly once, in TestMain, before any test goroutine exists.
var debugLogging atomic.Bool

type levelToggleHandler struct {
	slog.Handler
	debug *atomic.Bool
}

func (h levelToggleHandler) Enabled(ctx context.Context, l slog.Level) bool {
	if h.debug.Load() {
		return l >= slog.LevelDebug
	}
	return l >= slog.LevelInfo
}

func TestMain(m *testing.M) {
	ble.SetLogger(slog.New(levelToggleHandler{
		Handler: slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		debug:   &debugLogging,
	}))
	os.Exit(m.Run())
}

// nopHandler discards notifications.
type nopHandler struct{}

func (nopHandler) HandleNotification(req []byte) {}

// onceConn is a fakeConn whose Close is idempotent, so both a test and the
// code under test (e.g. the server Loop's shutdown path) may close it.
type onceConn struct {
	*fakeConn
	once sync.Once
}

func newOnceConn() *onceConn { return &onceConn{fakeConn: newFakeConn()} }

func (c *onceConn) Close() error {
	c.once.Do(func() { close(c.fakeConn.in) })
	return nil
}

// failWriteConn fails every Write after the first okWrites calls.
type failWriteConn struct {
	*onceConn
	mu       sync.Mutex
	okWrites int
}

func (c *failWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.okWrites <= 0 {
		return 0, errors.New("induced write failure")
	}
	c.okWrites--
	return c.fakeConn.Write(p)
}

// startClient runs c.Loop for a client on l2c and makes the test wait for
// the Loop goroutine to exit during cleanup (so no goroutine of an earlier
// test can race a later test's access to package state).
func startClient(t *testing.T, l2c ble.Conn) *Client {
	t.Helper()
	c := NewClient(l2c, nopHandler{})
	done := make(chan struct{})
	go func() { c.Loop(); close(done) }()
	t.Cleanup(func() {
		l2c.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("client Loop did not exit")
		}
	})
	return c
}

func recvWrite(t *testing.T, ch chan []byte) []byte {
	t.Helper()
	select {
	case w := <-ch:
		return w
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a write")
		return nil
	}
}

// testService builds a small GATT service with one characteristic that
// supports read and (prepare/execute) write. If written is non-nil, the
// write handler records the written value there.
func testService(written *[]byte) (*ble.Service, *ble.Characteristic) {
	svc := ble.NewService(ble.UUID16(0x1815))
	ch := svc.NewCharacteristic(ble.UUID16(0x2A56))
	ch.HandleRead(ble.ReadHandlerFunc(func(req ble.Request, rsp ble.ResponseWriter) {
		rsp.Write([]byte{0x2A})
	}))
	ch.HandleWrite(ble.WriteHandlerFunc(func(req ble.Request, rsp ble.ResponseWriter) {
		if written != nil {
			*written = append([]byte(nil), req.Data()...)
		}
	}))
	return svc, ch
}

// TestDebugLogging enables debug-level logging and exercises the
// debug-gated branches in the client, the server, and DumpAttributes.
func TestDebugLogging(t *testing.T) {
	debugLogging.Store(true)
	defer debugLogging.Store(false)

	f := newOnceConn()
	c := NewClient(f, nopHandler{})
	done := make(chan struct{})
	go func() { c.Loop(); close(done) }()
	ctx := context.Background()

	// Stale-response drain: park Loop on the unsolicited response, then the
	// next request must drop it before writing.
	f.in <- []byte{WriteResponseCode}
	time.Sleep(50 * time.Millisecond)
	go func() {
		<-f.writes // the read request
		f.in <- []byte{ReadResponseCode, 0xAA}
	}()
	v, err := c.Read(ctx, 1)
	if err != nil || len(v) != 1 || v[0] != 0xAA {
		t.Fatalf("Read after stale drain = % X, %v", v, err)
	}

	// Unexpected response, then the real one (Apple async request path).
	go func() {
		<-f.writes // the read request
		f.in <- []byte{WriteResponseCode}
		w := <-f.writes // the generated error response
		if len(w) != 5 || w[0] != ErrorResponseCode {
			t.Errorf("expected error response for unexpected rsp, got % X", w)
		}
		f.in <- []byte{ReadResponseCode, 0xBB}
	}()
	v, err = c.Read(ctx, 1)
	if err != nil || len(v) != 1 || v[0] != 0xBB {
		t.Fatalf("Read after unexpected rsp = % X, %v", v, err)
	}

	// Notification and indication under debug logging.
	f.in <- []byte{HandleValueNotificationCode, 0x01, 0x00, 0xFF}
	f.in <- []byte{HandleValueIndicationCode, 0x01, 0x00, 0xFF}
	if w := recvWrite(t, f.writes); len(w) != 1 || w[0] != HandleValueConfirmationCode {
		t.Fatalf("expected confirmation, got % X", w)
	}
	f.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("client Loop did not exit")
	}

	// DumpAttributes with debug on, via NewDB (covers attrs with and
	// without a static value).
	var written []byte
	svc, char := testService(&written)
	db := NewDB([]*ble.Service{svc}, 1)

	// Server prepare-write / execute-write with debug on.
	fs := newOnceConn()
	s, err := NewServer(db, fs)
	if err != nil {
		t.Fatal(err)
	}
	vh := char.ValueHandle
	pdu := append([]byte{PrepareWriteRequestCode, byte(vh), byte(vh >> 8), 0x00, 0x00}, 'h', 'i')
	rsp := s.handleRequest(pdu)
	if len(rsp) == 0 || rsp[0] != PrepareWriteResponseCode {
		t.Fatalf("prepare write rsp = % X", rsp)
	}
	rsp = s.handleRequest([]byte{ExecuteWriteRequestCode, 0x01})
	if len(rsp) != 1 || rsp[0] != ExecuteWriteResponseCode {
		t.Fatalf("execute write rsp = % X", rsp)
	}
	if !bytes.Equal(written, []byte("hi")) {
		t.Fatalf("executed write delivered %q", written)
	}
}

// TestClientRequestsHappyPath drives every request method through Loop with
// a canned responder.
func TestClientRequestsHappyPath(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)
	ctx := context.Background()

	respond := func(rsp []byte) chan []byte {
		req := make(chan []byte, 1)
		go func() {
			w := <-f.writes
			req <- w
			f.in <- rsp
		}()
		return req
	}

	// ExchangeMTU: server keeps the default MTU.
	req := respond([]byte{ExchangeMTUResponseCode, 23, 0})
	mtu, err := c.ExchangeMTU(ctx, 185)
	if err != nil || mtu != 23 {
		t.Fatalf("ExchangeMTU = %d, %v", mtu, err)
	}
	if w := <-req; w[0] != ExchangeMTURequestCode || len(w) != 3 {
		t.Fatalf("ExchangeMTU request = % X", w)
	}

	// FindInformation: format 0x01, one 4-byte entry.
	respond([]byte{FindInformationResponseCode, 0x01, 0x01, 0x00, 0x00, 0x28})
	format, data, err := c.FindInformation(ctx, 1, 0xFFFF)
	if err != nil || format != 1 || len(data) != 4 {
		t.Fatalf("FindInformation = %d, % X, %v", format, data, err)
	}

	// ReadByType: one entry of length 4.
	respond([]byte{ReadByTypeResponseCode, 0x04, 0x03, 0x00, 0xAA, 0xBB})
	length, data, err := c.ReadByType(ctx, 1, 0xFFFF, ble.UUID16(0x2A00))
	if err != nil || length != 4 || len(data) != 4 {
		t.Fatalf("ReadByType = %d, % X, %v", length, data, err)
	}

	// Read.
	respond([]byte{ReadResponseCode, 0xAA})
	v, err := c.Read(ctx, 3)
	if err != nil || len(v) != 1 || v[0] != 0xAA {
		t.Fatalf("Read = % X, %v", v, err)
	}

	// ReadBlob.
	respond([]byte{ReadBlobResponseCode, 0xBB})
	v, err = c.ReadBlob(ctx, 3, 1)
	if err != nil || len(v) != 1 || v[0] != 0xBB {
		t.Fatalf("ReadBlob = % X, %v", v, err)
	}

	// ReadMultiple.
	req = respond([]byte{ReadMultipleResponseCode, 0x01, 0x02})
	v, err = c.ReadMultiple(ctx, []uint16{2, 4})
	if err != nil || len(v) != 2 {
		t.Fatalf("ReadMultiple = % X, %v", v, err)
	}
	if w := <-req; !bytes.Equal(w, []byte{ReadMultipleRequestCode, 2, 0, 4, 0}) {
		t.Fatalf("ReadMultiple request = % X", w)
	}

	// ReadByGroupType: one entry of length 6.
	respond([]byte{ReadByGroupTypeResponseCode, 0x06, 0x01, 0x00, 0x05, 0x00, 0x18, 0x0A})
	length, data, err = c.ReadByGroupType(ctx, 1, 0xFFFF, ble.UUID16(0x2800))
	if err != nil || length != 6 || len(data) != 6 {
		t.Fatalf("ReadByGroupType = %d, % X, %v", length, data, err)
	}

	// Write.
	respond([]byte{WriteResponseCode})
	if err := c.Write(ctx, 3, []byte{1, 2}); err != nil {
		t.Fatalf("Write = %v", err)
	}

	// WriteCommand: no response expected.
	if err := c.WriteCommand(ctx, 3, []byte{9}); err != nil {
		t.Fatalf("WriteCommand = %v", err)
	}
	if w := recvWrite(t, f.writes); w[0] != WriteCommandCode {
		t.Fatalf("WriteCommand request = % X", w)
	}

	// SignedWrite: no response expected.
	if err := c.SignedWrite(ctx, 3, []byte{7}, [12]byte{}); err != nil {
		t.Fatalf("SignedWrite = %v", err)
	}
	if w := recvWrite(t, f.writes); w[0] != SignedWriteCommandCode || len(w) != 16 {
		t.Fatalf("SignedWrite request = % X", w)
	}

	// PrepareWrite: the request PDU must carry the part value (a former
	// upstream bug sent stale txBuf bytes instead).
	req = respond([]byte{PrepareWriteResponseCode, 0x03, 0x00, 0x00, 0x00, 0x01})
	h, off, v, err := c.PrepareWrite(ctx, 3, 0, []byte{0xAB})
	if err != nil || h != 3 || off != 0 || len(v) != 1 {
		t.Fatalf("PrepareWrite = %d, %d, % X, %v", h, off, v, err)
	}
	if w := recvWrite(t, req); w[0] != PrepareWriteRequestCode || len(w) != 6 || w[5] != 0xAB {
		t.Fatalf("PrepareWrite request = % X, want value byte AB at the end", w)
	}

	// ExecuteWrite: a 2-byte request (opcode + flags; a former upstream bug
	// sliced the txBuf to 1 byte and panicked in SetFlags).
	req = respond([]byte{ExecuteWriteResponseCode})
	if err := c.ExecuteWrite(ctx, 0x01); err != nil {
		t.Fatalf("ExecuteWrite = %v", err)
	}
	if w := recvWrite(t, req); w[0] != ExecuteWriteRequestCode || len(w) != 2 || w[1] != 0x01 {
		t.Fatalf("ExecuteWrite request = % X, want [18 01]", w)
	}
}

// TestClientRequestErrors covers the ATT-error and invalid-response arms.
func TestClientRequestErrors(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)
	ctx := context.Background()

	// A well-formed ATT error response surfaces as ble.ATTError.
	go func() {
		<-f.writes
		f.in <- []byte{ErrorResponseCode, ReadRequestCode, 0x03, 0x00, 0x0E}
	}()
	if _, err := c.Read(ctx, 3); err != ble.ATTError(0x0E) {
		t.Fatalf("Read ATT error = %v", err)
	}

	// A malformed (short) error response is an invalid response.
	go func() {
		<-f.writes
		f.in <- []byte{ErrorResponseCode, ReadRequestCode, 0x03}
	}()
	if _, err := c.Read(ctx, 3); err != ErrInvalidResponse {
		t.Fatalf("Read invalid response = %v", err)
	}
}

// TestClientInvalidArguments covers input validation; nothing hits the wire.
func TestClientInvalidArguments(t *testing.T) {
	c := NewClient(newOnceConn(), nopHandler{})
	ctx := context.Background()

	if _, err := c.ExchangeMTU(ctx, 22); err != ErrInvalidArgument {
		t.Errorf("ExchangeMTU(22) = %v", err)
	}
	if _, err := c.ExchangeMTU(ctx, ble.MaxMTU+1); err != ErrInvalidArgument {
		t.Errorf("ExchangeMTU(max+1) = %v", err)
	}
	if _, _, err := c.FindInformation(ctx, 0, 1); err != ErrInvalidArgument {
		t.Errorf("FindInformation(0,1) = %v", err)
	}
	if _, _, err := c.FindInformation(ctx, 2, 1); err != ErrInvalidArgument {
		t.Errorf("FindInformation(2,1) = %v", err)
	}
	if _, _, err := c.ReadByType(ctx, 2, 1, ble.UUID16(0x2A00)); err != ErrInvalidArgument {
		t.Errorf("ReadByType bad range = %v", err)
	}
	if _, _, err := c.ReadByType(ctx, 1, 2, ble.UUID([]byte{1, 2, 3})); err != ErrInvalidArgument {
		t.Errorf("ReadByType bad uuid = %v", err)
	}
	if _, err := c.ReadMultiple(ctx, []uint16{1}); err != ErrInvalidArgument {
		t.Errorf("ReadMultiple(1 handle) = %v", err)
	}
	if _, err := c.ReadMultiple(ctx, make([]uint16, 12)); err != ErrInvalidArgument {
		t.Errorf("ReadMultiple(12 handles) = %v", err)
	}
	if _, _, err := c.ReadByGroupType(ctx, 2, 1, ble.UUID16(0x2800)); err != ErrInvalidArgument {
		t.Errorf("ReadByGroupType bad range = %v", err)
	}
	if err := c.Write(ctx, 1, make([]byte, 21)); err != ErrInvalidArgument {
		t.Errorf("Write oversized = %v", err)
	}
	if err := c.WriteCommand(ctx, 1, make([]byte, 21)); err != ErrInvalidArgument {
		t.Errorf("WriteCommand oversized = %v", err)
	}
	if err := c.SignedWrite(ctx, 1, make([]byte, 9), [12]byte{}); err != ErrInvalidArgument {
		t.Errorf("SignedWrite oversized = %v", err)
	}
	if _, _, _, err := c.PrepareWrite(ctx, 1, 0, make([]byte, 19)); err != ErrInvalidArgument {
		t.Errorf("PrepareWrite oversized = %v", err)
	}
}

// TestAcquireTxBufCancelled starves the tx buffer and calls every request
// method with an already-cancelled context.
func TestAcquireTxBufCancelled(t *testing.T) {
	c := NewClient(newOnceConn(), nopHandler{})
	buf := <-c.chTxBuf // hold the txBuf hostage
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	checks := []struct {
		name string
		err  error
	}{
		{"ExchangeMTU", func() error { _, err := c.ExchangeMTU(ctx, 185); return err }()},
		{"FindInformation", func() error { _, _, err := c.FindInformation(ctx, 1, 2); return err }()},
		{"ReadByType", func() error { _, _, err := c.ReadByType(ctx, 1, 2, ble.UUID16(0x2A00)); return err }()},
		{"Read", func() error { _, err := c.Read(ctx, 1); return err }()},
		{"ReadBlob", func() error { _, err := c.ReadBlob(ctx, 1, 0); return err }()},
		{"ReadMultiple", func() error { _, err := c.ReadMultiple(ctx, []uint16{1, 2}); return err }()},
		{"ReadByGroupType", func() error { _, _, err := c.ReadByGroupType(ctx, 1, 2, ble.UUID16(0x2800)); return err }()},
		{"Write", c.Write(ctx, 1, []byte{1})},
		{"WriteCommand", c.WriteCommand(ctx, 1, []byte{1})},
		{"SignedWrite", c.SignedWrite(ctx, 1, []byte{1}, [12]byte{})},
		{"PrepareWrite", func() error { _, _, _, err := c.PrepareWrite(ctx, 1, 0, []byte{1}); return err }()},
		{"ExecuteWrite", c.ExecuteWrite(ctx, 0)},
	}
	for _, chk := range checks {
		if !errors.Is(chk.err, context.Canceled) {
			t.Errorf("%s with starved txBuf = %v, want context.Canceled", chk.name, chk.err)
		}
	}
	c.chTxBuf <- buf
}

// TestSendReqWriteError covers the request-write failure path.
func TestSendReqWriteError(t *testing.T) {
	f := &failWriteConn{onceConn: newOnceConn()}
	c := NewClient(f, nopHandler{})
	_, err := c.Read(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "send ATT request failed") {
		t.Fatalf("Read with failing write = %v", err)
	}
}

// TestSendReqUnexpectedRspWriteError: an unexpected response arrives and the
// generated error response cannot be written.
func TestSendReqUnexpectedRspWriteError(t *testing.T) {
	f := &failWriteConn{onceConn: newOnceConn(), okWrites: 1}
	c := startClient(t, f)
	go func() {
		<-f.writes // the read request went out
		f.in <- []byte{WriteResponseCode}
	}()
	_, err := c.Read(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "unexpected ATT response received") {
		t.Fatalf("Read = %v", err)
	}
}

// TestSendReqConnError: the bearer dies while a request is pending.
func TestSendReqConnError(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)
	go func() {
		<-f.writes
		f.Close()
	}()
	_, err := c.Read(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "ATT request failed") || !errors.Is(err, io.EOF) {
		t.Fatalf("Read on dead conn = %v", err)
	}
}

// TestSendReqCtxCancelled: the context is cancelled while waiting for the
// response.
func TestSendReqCtxCancelled(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-f.writes
		cancel()
	}()
	_, err := c.Read(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Read with cancelled ctx = %v", err)
	}
}

// TestLoopMTURequestOverflow drives incoming ExchangeMTU requests through
// Loop, including overflowing the async work queue while the handler is
// starved of the tx buffer.
func TestLoopMTURequestOverflow(t *testing.T) {
	f := newOnceConn()
	c := startClient(t, f)

	buf := <-c.chTxBuf // starve handleExchangeMTURequest
	req := []byte{ExchangeMTURequestCode, 0xB7, 0x00}
	const total = 20
	for i := 0; i < total; i++ {
		pdu := make([]byte, len(req))
		copy(pdu, req)
		f.in <- pdu
	}
	c.chTxBuf <- buf // let the queued requests drain

	// Collect MTU responses until 300ms of silence. The queue holds at most
	// 16 requests plus one in the consumer's hands, so some of the 20 must
	// have been dropped.
	var got int
	for {
		select {
		case w := <-f.writes:
			if len(w) != 3 || w[0] != ExchangeMTUResponseCode {
				t.Fatalf("unexpected write % X", w)
			}
			got++
		case <-time.After(300 * time.Millisecond):
			goto done
		}
	}
done:
	if got < 16 || got >= total {
		t.Fatalf("processed %d MTU requests, want 16..%d (some dropped)", got, total-1)
	}
}

// TestHandleRequestUnknownOpcode exercises handleRequest's default branch,
// which Loop never routes to.
func TestHandleRequestUnknownOpcode(t *testing.T) {
	f := newOnceConn()
	c := NewClient(f, nopHandler{})
	c.handleRequest([]byte{0x42})
	w := recvWrite(t, f.writes)
	if len(w) != 5 || w[0] != ErrorResponseCode || w[1] != 0x42 || w[4] != byte(ble.ErrReqNotSupp) {
		t.Fatalf("unknown request error response = % X", w)
	}
}

// TestHandleRequestMTUResponseSendError: the MTU response write fails.
func TestHandleRequestMTUResponseSendError(t *testing.T) {
	f := &failWriteConn{onceConn: newOnceConn()}
	c := NewClient(f, nopHandler{})
	c.handleRequest([]byte{ExchangeMTURequestCode, 0xB7, 0x00}) // must not panic
}
