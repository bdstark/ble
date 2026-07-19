package gatt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/att"
)

// fakeConn is a minimal ble.Conn: Read delivers PDUs pushed into in, Write
// hands request PDUs to the fake ATT server. Close unblocks both sides.
type fakeConn struct {
	in     chan []byte
	writes chan []byte
	closed chan struct{}
	once   sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		in:     make(chan []byte),
		writes: make(chan []byte, 64),
		closed: make(chan struct{}),
	}
}

func (f *fakeConn) Read(p []byte) (int, error) {
	select {
	case b := <-f.in:
		return copy(p, b), nil
	case <-f.closed:
		return 0, io.EOF
	}
}

func (f *fakeConn) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	select {
	case f.writes <- b:
	case <-f.closed:
	}
	return len(p), nil
}

func (f *fakeConn) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}
func (f *fakeConn) Context() context.Context       { return context.Background() }
func (f *fakeConn) SetContext(ctx context.Context) {}
func (f *fakeConn) LocalAddr() ble.Addr            { return ble.NewAddr("11:11:11:11:11:11") }
func (f *fakeConn) RemoteAddr() ble.Addr           { return ble.NewAddr("22:22:22:22:22:22") }
func (f *fakeConn) RxMTU() int                     { return ble.DefaultMTU }
func (f *fakeConn) SetRxMTU(mtu int)               {}
func (f *fakeConn) TxMTU() int                     { return ble.DefaultMTU }
func (f *fakeConn) SetTxMTU(mtu int)               {}
func (f *fakeConn) ReadRSSI() (int, error)         { return -42, nil }
func (f *fakeConn) UpdateParams(context.Context, ble.ConnParams) error {
	return nil
}
func (f *fakeConn) SetDataLength(context.Context, uint16, uint16) error {
	return nil
}
func (f *fakeConn) Disconnected() <-chan struct{} { return f.closed }

// fakeServer models a tiny GATT server: one primary service (handles 1-5)
// holding one characteristic (declaration 2, value 3) and its CCCD (handle
// 4). It answers each ATT request the client writes with a canned, correctly
// encoded response; repeated discovery requests get an ErrAttrNotFound error
// response so the client's discovery loops terminate.
type fakeServer struct {
	conn *fakeConn

	mu        sync.Mutex
	readValue []byte       // payload of the Read Response
	blobValue []byte       // payload of the Read Blob Response
	failReads bool         // answer Read Requests with an ATT error response
	discovery map[byte]int // per-opcode discovery request count
	writeReqs [][]byte     // recorded Write Requests (0x12)

	cmds chan []byte // Write Commands (0x52); they get no response
}

func newFakeServer(conn *fakeConn) *fakeServer {
	s := &fakeServer{
		conn:      conn,
		readValue: []byte{1, 2, 3},
		blobValue: []byte{9, 9},
		discovery: make(map[byte]int),
		cmds:      make(chan []byte, 8),
	}
	go s.serve()
	return s
}

func (s *fakeServer) serve() {
	for {
		var req []byte
		select {
		case req = <-s.conn.writes:
		case <-s.conn.closed:
			return
		}
		rsp := s.respond(req)
		if rsp == nil {
			continue
		}
		select {
		case s.conn.in <- rsp:
		case <-s.conn.closed:
			return
		}
	}
}

func errAttrNotFound(reqOp byte) []byte {
	return []byte{att.ErrorResponseCode, reqOp, 0x00, 0x00, byte(ble.ErrAttrNotFound)}
}

func (s *fakeServer) respond(req []byte) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := req[0]
	switch op {
	case att.ExchangeMTURequestCode:
		return []byte{att.ExchangeMTUResponseCode, byte(ble.DefaultMTU), 0x00}
	case att.ReadByGroupTypeRequestCode: // service discovery
		s.discovery[op]++
		if s.discovery[op] > 1 {
			return errAttrNotFound(op)
		}
		// One service: handles 1..5, 16-bit UUID 0x180F.
		return []byte{att.ReadByGroupTypeResponseCode, 6,
			0x01, 0x00, 0x05, 0x00, 0x0F, 0x18}
	case att.ReadByTypeRequestCode:
		// The client uses this PDU for characteristic discovery (0x2803),
		// include discovery (0x2802), and the lazy GAP device-name read
		// (0x2A00). This plain fixture has no includes and no GAP name.
		if len(req) == 7 {
			switch binary.LittleEndian.Uint16(req[5:7]) {
			case 0x2802, 0x2A00:
				return errAttrNotFound(op)
			}
		}
		// characteristic discovery
		s.discovery[op]++
		if s.discovery[op] > 1 {
			return errAttrNotFound(op)
		}
		// One characteristic: declaration 2, properties
		// read|write|notify|indicate (0x3A), value handle 3, UUID 0x2A19.
		return []byte{att.ReadByTypeResponseCode, 7,
			0x02, 0x00, 0x3A, 0x03, 0x00, 0x19, 0x2A}
	case att.FindInformationRequestCode: // descriptor discovery
		s.discovery[op]++
		if s.discovery[op] > 1 {
			return errAttrNotFound(op)
		}
		// One descriptor: the CCCD (0x2902) at handle 4.
		return []byte{att.FindInformationResponseCode, 0x01,
			0x04, 0x00, 0x02, 0x29}
	case att.ReadRequestCode:
		if s.failReads {
			return []byte{att.ErrorResponseCode, op, 0x03, 0x00, byte(ble.ErrAttrNotFound)}
		}
		return append([]byte{att.ReadResponseCode}, s.readValue...)
	case att.ReadBlobRequestCode:
		return append([]byte{att.ReadBlobResponseCode}, s.blobValue...)
	case att.WriteRequestCode:
		s.writeReqs = append(s.writeReqs, req)
		return []byte{att.WriteResponseCode}
	case att.WriteCommandCode:
		select {
		case s.cmds <- req:
		default:
		}
		return nil
	case att.HandleValueConfirmationCode:
		return nil // the client's ack of our indication
	default:
		return nil
	}
}

func (s *fakeServer) writes() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.writeReqs))
	copy(out, s.writeReqs)
	return out
}

func (s *fakeServer) setReadValue(v []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readValue = v
}

func (s *fakeServer) setFailReads(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failReads = fail
}

func newTestClient(t *testing.T) (*Client, *fakeServer) {
	t.Helper()
	conn := newFakeConn()
	srv := newFakeServer(conn)
	cln, err := NewClient(conn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return cln, srv
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// checkCCCDWrite asserts that the i-th recorded Write Request targeted the
// given handle with the given little-endian 16-bit value.
func checkCCCDWrite(t *testing.T, srv *fakeServer, i int, handle, val uint16) {
	t.Helper()
	w := srv.writes()
	if len(w) <= i {
		t.Fatalf("only %d write requests recorded, want at least %d", len(w), i+1)
	}
	req := att.WriteRequest(w[i])
	if req.AttributeHandle() != handle {
		t.Fatalf("write %d targeted handle 0x%04X, want 0x%04X", i, req.AttributeHandle(), handle)
	}
	if got := binary.LittleEndian.Uint16(req.AttributeValue()); got != val {
		t.Fatalf("write %d value = 0x%04X, want 0x%04X", i, got, val)
	}
}

func TestClientDiscoverProfile(t *testing.T) {
	cln, _ := newTestClient(t)
	ctx := testCtx(t)

	p, err := cln.DiscoverProfile(ctx, false)
	if err != nil {
		t.Fatalf("DiscoverProfile: %v", err)
	}
	if len(p.Services) != 1 {
		t.Fatalf("discovered %d services, want 1", len(p.Services))
	}
	s := p.Services[0]
	if s.Handle != 1 || s.EndHandle != 5 || !s.UUID.Equal(ble.UUID16(0x180F)) {
		t.Fatalf("service = {h: %d, endh: %d, uuid: %s}, want {1, 5, 180f}", s.Handle, s.EndHandle, s.UUID)
	}
	if len(s.Characteristics) != 1 {
		t.Fatalf("discovered %d characteristics, want 1", len(s.Characteristics))
	}
	c := s.Characteristics[0]
	if c.Handle != 2 || c.ValueHandle != 3 || !c.UUID.Equal(ble.UUID16(0x2A19)) {
		t.Fatalf("characteristic = {h: %d, vh: %d, uuid: %s}, want {2, 3, 2a19}", c.Handle, c.ValueHandle, c.UUID)
	}
	if len(c.Descriptors) != 1 {
		t.Fatalf("discovered %d descriptors, want 1", len(c.Descriptors))
	}
	if c.CCCD == nil || c.CCCD.Handle != 4 {
		t.Fatalf("CCCD = %+v, want handle 4", c.CCCD)
	}

	// A second non-forced call returns the cached profile.
	p2, err := cln.DiscoverProfile(ctx, false)
	if err != nil || p2 != p {
		t.Fatalf("cached DiscoverProfile = (%p, %v), want (%p, nil)", p2, err, p)
	}
	if cln.Profile() != p {
		t.Fatal("Profile() did not return the discovered profile")
	}

	// A service without includes yields an empty non-nil slice and nil
	// error (the walk ran; the old stub's (nil, nil) faked success).
	inc, err := cln.DiscoverIncludedServices(ctx, nil, s)
	if inc == nil || len(inc) != 0 || err != nil {
		t.Fatalf("DiscoverIncludedServices = (%v, %v), want ([], nil)", inc, err)
	}
}

func TestClientReadWriteAndMTU(t *testing.T) {
	cln, srv := newTestClient(t)
	ctx := testCtx(t)

	c := &ble.Characteristic{ValueHandle: 3, EndHandle: 5}
	got, err := cln.ReadCharacteristic(ctx, c)
	if err != nil {
		t.Fatalf("ReadCharacteristic: %v", err)
	}
	if !bytes.Equal(got, []byte{1, 2, 3}) || !bytes.Equal(c.Value, got) {
		t.Fatalf("ReadCharacteristic = % X (c.Value % X), want 01 02 03", got, c.Value)
	}

	if err := cln.WriteCharacteristic(ctx, c, []byte{0xAA}, false); err != nil {
		t.Fatalf("WriteCharacteristic: %v", err)
	}
	w := srv.writes()
	if len(w) != 1 {
		t.Fatalf("recorded %d write requests, want 1", len(w))
	}
	req := att.WriteRequest(w[0])
	if req.AttributeHandle() != 3 || !bytes.Equal(req.AttributeValue(), []byte{0xAA}) {
		t.Fatalf("write request = {h: %d, v: % X}, want {3, AA}", req.AttributeHandle(), req.AttributeValue())
	}

	// noRsp: a Write Command, which the server never acknowledges.
	if err := cln.WriteCharacteristic(ctx, c, []byte{0xBB}, true); err != nil {
		t.Fatalf("WriteCharacteristic (no rsp): %v", err)
	}
	select {
	case cmd := <-srv.cmds:
		wc := att.WriteCommand(cmd)
		if wc.AttributeHandle() != 3 || !bytes.Equal(wc.AttributeValue(), []byte{0xBB}) {
			t.Fatalf("write command = {h: %d, v: % X}, want {3, BB}", wc.AttributeHandle(), wc.AttributeValue())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write command never reached the server")
	}

	d := &ble.Descriptor{Handle: 4}
	dv, err := cln.ReadDescriptor(ctx, d)
	if err != nil || !bytes.Equal(dv, []byte{1, 2, 3}) || !bytes.Equal(d.Value, dv) {
		t.Fatalf("ReadDescriptor = (% X, %v), d.Value % X, want 01 02 03", dv, err, d.Value)
	}
	if err := cln.WriteDescriptor(ctx, d, []byte{0xCC}); err != nil {
		t.Fatalf("WriteDescriptor: %v", err)
	}

	if rssi, err := cln.ReadRSSI(ctx); err != nil || rssi != -42 {
		t.Fatalf("ReadRSSI = (%d, %v), want (-42, nil) (from the conn)", rssi, err)
	}

	mtu, err := cln.ExchangeMTU(ctx, ble.DefaultMTU)
	if err != nil || mtu != ble.DefaultMTU {
		t.Fatalf("ExchangeMTU = (%d, %v), want (%d, nil)", mtu, err, ble.DefaultMTU)
	}

	// An ATT error response must surface as the typed ATTError.
	srv.setFailReads(true)
	if _, err := cln.ReadCharacteristic(ctx, c); !errors.Is(err, ble.ErrAttrNotFound) {
		t.Fatalf("ReadCharacteristic with server error = %v, want ble.ErrAttrNotFound", err)
	}
}

func TestClientReadLongCharacteristic(t *testing.T) {
	cln, srv := newTestClient(t)
	ctx := testCtx(t)

	// A full MTU-1 first read forces the follow-up Read Blob round trip.
	full := bytes.Repeat([]byte{0x5A}, ble.DefaultMTU-1)
	srv.setReadValue(full)

	c := &ble.Characteristic{ValueHandle: 3}
	got, err := cln.ReadLongCharacteristic(ctx, c)
	if err != nil {
		t.Fatalf("ReadLongCharacteristic: %v", err)
	}
	want := append(append([]byte{}, full...), 9, 9)
	if !bytes.Equal(got, want) || !bytes.Equal(c.Value, want) {
		t.Fatalf("ReadLongCharacteristic = % X, want % X", got, want)
	}
}

func TestClientSubscribeNotifyAndIndicate(t *testing.T) {
	cln, srv := newTestClient(t)
	ctx := testCtx(t)

	c := &ble.Characteristic{
		ValueHandle: 3,
		CCCD:        &ble.Descriptor{UUID: ble.ClientCharacteristicConfigUUID, Handle: 4},
	}

	notified := make(chan []byte, 4)
	if err := cln.Subscribe(ctx, c, false, func(b []byte) {
		notified <- append([]byte(nil), b...)
	}); err != nil {
		t.Fatalf("Subscribe(notify): %v", err)
	}
	checkCCCDWrite(t, srv, 0, 4, cccNotify)

	// A notification for the subscribed value handle reaches the handler.
	srv.conn.in <- []byte{att.HandleValueNotificationCode, 0x03, 0x00, 0xDE, 0xAD}
	select {
	case b := <-notified:
		if !bytes.Equal(b, []byte{0xDE, 0xAD}) {
			t.Fatalf("notification payload = % X, want DE AD", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notification never reached the handler")
	}

	// A notification for an unregistered handle is dropped without crashing;
	// a follow-up registered one proves the loop is still alive (PDUs are
	// dispatched in order).
	srv.conn.in <- []byte{att.HandleValueNotificationCode, 0x99, 0x00, 0x01}
	srv.conn.in <- []byte{att.HandleValueNotificationCode, 0x03, 0x00, 0xBE, 0xEF}
	select {
	case b := <-notified:
		if !bytes.Equal(b, []byte{0xBE, 0xEF}) {
			t.Fatalf("notification payload = % X, want BE EF", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("loop stopped delivering after an unregistered notification")
	}

	indicated := make(chan []byte, 4)
	if err := cln.Subscribe(ctx, c, true, func(b []byte) {
		indicated <- append([]byte(nil), b...)
	}); err != nil {
		t.Fatalf("Subscribe(indicate): %v", err)
	}
	checkCCCDWrite(t, srv, 1, 4, cccNotify|cccIndicate)

	srv.conn.in <- []byte{att.HandleValueIndicationCode, 0x03, 0x00, 0x77}
	select {
	case b := <-indicated:
		if !bytes.Equal(b, []byte{0x77}) {
			t.Fatalf("indication payload = % X, want 77", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("indication never reached the handler")
	}

	if err := cln.Unsubscribe(ctx, c, false); err != nil {
		t.Fatalf("Unsubscribe(notify): %v", err)
	}
	checkCCCDWrite(t, srv, 2, 4, cccIndicate)
	if err := cln.Unsubscribe(ctx, c, true); err != nil {
		t.Fatalf("Unsubscribe(indicate): %v", err)
	}
	checkCCCDWrite(t, srv, 3, 4, 0x0000)
}

func TestClientSubscribeErrorsAndClear(t *testing.T) {
	cln, srv := newTestClient(t)
	ctx := testCtx(t)

	noCCCD := &ble.Characteristic{ValueHandle: 3}
	if err := cln.Subscribe(ctx, noCCCD, false, func([]byte) {}); err == nil {
		t.Fatal("Subscribe on a characteristic without a CCCD returned nil error")
	}
	if err := cln.Unsubscribe(ctx, noCCCD, false); err == nil {
		t.Fatal("Unsubscribe on a characteristic without a CCCD returned nil error")
	}

	c := &ble.Characteristic{ValueHandle: 3, CCCD: &ble.Descriptor{Handle: 4}}
	if err := cln.Subscribe(ctx, c, false, func([]byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := cln.ClearSubscriptions(ctx); err != nil {
		t.Fatalf("ClearSubscriptions: %v", err)
	}
	checkCCCDWrite(t, srv, 1, 4, 0x0000)
	cln.Lock()
	remaining := len(cln.subs)
	cln.Unlock()
	if remaining != 0 {
		t.Fatalf("%d subscriptions remain after ClearSubscriptions", remaining)
	}
}

func TestClientConnPlumbing(t *testing.T) {
	cln, srv := newTestClient(t)

	if cln.Conn() != ble.Conn(srv.conn) {
		t.Fatal("Conn() did not return the underlying connection")
	}
	if got := cln.Addr().String(); got != "22:22:22:22:22:22" {
		t.Fatalf("Addr() = %q, want the conn's remote address", got)
	}
	if got := cln.Name(); got != "" {
		t.Fatalf("Name() = %q, want empty", got)
	}
	if err := cln.CancelConnection(); err != nil {
		t.Fatalf("CancelConnection: %v", err)
	}
	select {
	case <-cln.Disconnected():
	case <-time.After(time.Second):
		t.Fatal("CancelConnection did not close the connection")
	}
}
