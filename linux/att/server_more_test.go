package att

// Wire-level tests for the ATT server's request handlers: exact response
// bytes for the marshal paths, the MTU exchange, the error-response arms,
// and regression tests for the crash and correctness bugs fixed in
// server.go (each of these fails against the pre-fix code).

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/bdstark/ble"
)

// rxMTUConn overrides the fixture's RxMTU so NewServer's validation can be
// exercised.
type rxMTUConn struct {
	*onceConn
	mtu int
}

func (c *rxMTUConn) RxMTU() int { return c.mtu }

// newTestServer builds a Server over a DB of the given services, based at
// handle 1.
func newTestServer(t *testing.T, conn ble.Conn, svcs ...*ble.Service) *Server {
	t.Helper()
	s, err := NewServer(NewDB(svcs, 1), conn)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// rsp drives one PDU through handleRequest.
func rsp(s *Server, pdu ...byte) []byte { return s.handleRequest(pdu) }

func wantBytes(t *testing.T, got []byte, want ...byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("response = [% X], want [% X]", got, want)
	}
}

// staticService returns a service (uuid 0x1815) with one readable
// characteristic (uuid 0x2A56) holding the static value v.
// DB layout at base 1: h1 service decl, h2 char decl, h3 value.
func staticService(v []byte) *ble.Service {
	svc := ble.NewService(ble.UUID16(0x1815))
	ch := svc.NewCharacteristic(ble.UUID16(0x2A56))
	ch.SetValue(v)
	return svc
}

func TestNewServerInvalidMTU(t *testing.T) {
	svc, _ := testService(nil)
	for _, mtu := range []int{ble.DefaultMTU - 1, ble.MaxMTU + 1} {
		_, err := NewServer(NewDB([]*ble.Service{svc}, 1), &rxMTUConn{onceConn: newOnceConn(), mtu: mtu})
		if !errors.Is(err, ErrInvalidMTU) {
			t.Errorf("NewServer(mtu=%d) = %v, want ErrInvalidMTU", mtu, err)
		}
	}
}

func TestServerMTUExchangeWire(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)

	// Malformed: truncated PDU, then an MTU below the spec minimum.
	wantBytes(t, rsp(s, ExchangeMTURequestCode, 0x17), 0x01, 0x02, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, ExchangeMTURequestCode, 0x16, 0x00), 0x01, 0x02, 0x00, 0x00, 0x04)
	if len(s.txBuf) != ble.DefaultMTU {
		t.Fatalf("txBuf resized to %d by a rejected MTU request", len(s.txBuf))
	}

	// Happy path: client MTU 0xB7 (183); we answer with our rx MTU (23) and
	// adopt 183 for tx: txBuf and the notification/indication buffers are
	// reallocated.
	wantBytes(t, rsp(s, ExchangeMTURequestCode, 0xB7, 0x00), ExchangeMTUResponseCode, 0x17, 0x00)
	if len(s.txBuf) != 0xB7 {
		t.Fatalf("txBuf len = %d after MTU exchange, want %d", len(s.txBuf), 0xB7)
	}
	nBuf, iBuf := <-s.chNotBuf, <-s.chIndBuf
	if len(nBuf) != 0xB7 || len(iBuf) != 0xB7 {
		t.Fatalf("notify/indicate buffers = %d/%d after MTU exchange, want %d", len(nBuf), len(iBuf), 0xB7)
	}
	s.chNotBuf <- nBuf
	s.chIndBuf <- iBuf
}

// TestServerMTUExchangeClamped: the peer's advertised MTU must be clamped
// at ble.MaxMTU before it is adopted (fails on the pre-fix code, which
// adopted 65535 wholesale).
func TestServerMTUExchangeClamped(t *testing.T) {
	svc, _ := testService(nil)
	f := &mtuRecordConn{onceConn: newOnceConn()}
	s := newTestServer(t, f, svc)

	wantBytes(t, rsp(s, ExchangeMTURequestCode, 0xFF, 0xFF), ExchangeMTUResponseCode, 0x17, 0x00)
	if got := f.txMTU.Load(); got != ble.MaxMTU {
		t.Fatalf("SetTxMTU got %d, want clamped %d", got, ble.MaxMTU)
	}
	if len(s.txBuf) != ble.MaxMTU {
		t.Fatalf("txBuf len = %d, want %d", len(s.txBuf), ble.MaxMTU)
	}
}

func TestServerFindInformationWire(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)

	// All three attributes, format 0x01 (16-bit types), spec layout:
	// opcode, format, then (handle, type) pairs.
	wantBytes(t, rsp(s, FindInformationRequestCode, 0x01, 0x00, 0xFF, 0xFF),
		FindInformationResponseCode, 0x01,
		0x01, 0x00, 0x00, 0x28, // h1: Primary Service
		0x02, 0x00, 0x03, 0x28, // h2: Characteristic
		0x03, 0x00, 0x56, 0x2A) // h3: value

	// Subrange.
	wantBytes(t, rsp(s, FindInformationRequestCode, 0x03, 0x00, 0xFF, 0xFF),
		FindInformationResponseCode, 0x01, 0x03, 0x00, 0x56, 0x2A)

	// Malformed length; invalid handle range; nothing in range.
	wantBytes(t, rsp(s, FindInformationRequestCode, 0x01, 0x00, 0xFF), 0x01, 0x04, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, FindInformationRequestCode, 0x00, 0x00, 0xFF, 0xFF), 0x01, 0x04, 0x00, 0x00, 0x01)
	wantBytes(t, rsp(s, FindInformationRequestCode, 0x02, 0x00, 0x01, 0x00), 0x01, 0x04, 0x02, 0x00, 0x01)
	wantBytes(t, rsp(s, FindInformationRequestCode, 0x05, 0x00, 0x06, 0x00), 0x01, 0x04, 0x05, 0x00, 0x0A)
}

func TestServerFindInformationFormats(t *testing.T) {
	// One 16-bit char and one 128-bit char: h1 svc, h2/h3 first char,
	// h4 decl + h5 value (128-bit type).
	long := ble.MustParse("12345678-9ABC-DEF0-1234-56789ABCDEF0")
	svc := ble.NewService(ble.UUID16(0x1815))
	svc.NewCharacteristic(ble.UUID16(0x2A56)).SetValue([]byte{1})
	svc.NewCharacteristic(long).SetValue([]byte{2})
	s := newTestServer(t, newOnceConn(), svc)

	// Starting in 16-bit territory: format 0x01, stops before the
	// 128-bit type at h5.
	wantBytes(t, rsp(s, FindInformationRequestCode, 0x01, 0x00, 0xFF, 0xFF),
		FindInformationResponseCode, 0x01,
		0x01, 0x00, 0x00, 0x28,
		0x02, 0x00, 0x03, 0x28,
		0x03, 0x00, 0x56, 0x2A,
		0x04, 0x00, 0x03, 0x28)

	// Starting at the 128-bit attribute: format 0x02, raw 16-byte type.
	want := append([]byte{FindInformationResponseCode, 0x02, 0x05, 0x00}, long...)
	got := rsp(s, FindInformationRequestCode, 0x05, 0x00, 0xFF, 0xFF)
	if !bytes.Equal(got, want) {
		t.Fatalf("format-2 response = [% X], want [% X]", got, want)
	}
}

func TestServerFindInformationCapacity(t *testing.T) {
	// 3 characteristics -> 7 attributes; at MTU 23 only 5 four-byte
	// entries fit ((23-2)/4 = 5).
	svc := ble.NewService(ble.UUID16(0x1815))
	for _, u := range []uint16{0x2A56, 0x2A57, 0x2A58} {
		svc.NewCharacteristic(ble.UUID16(u)).SetValue([]byte{1})
	}
	s := newTestServer(t, newOnceConn(), svc)
	got := rsp(s, FindInformationRequestCode, 0x01, 0x00, 0xFF, 0xFF)
	if len(got) != 2+5*4 {
		t.Fatalf("response holds %d bytes, want 22 (5 entries)", len(got))
	}
}

func TestServerFindByTypeValueWire(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)

	// Find the primary service by its UUID: one handle-range entry.
	wantBytes(t, rsp(s, FindByTypeValueRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28, 0x15, 0x18),
		FindByTypeValueResponseCode, 0x01, 0x00, 0xFF, 0xFF)

	// Value mismatch; malformed length; invalid handle range.
	wantBytes(t, rsp(s, FindByTypeValueRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28, 0x16, 0x18),
		0x01, 0x06, 0x01, 0x00, 0x0A)
	wantBytes(t, rsp(s, FindByTypeValueRequestCode, 0x01, 0x00, 0xFF, 0xFF), 0x01, 0x06, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, FindByTypeValueRequestCode, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x28, 0x15, 0x18),
		0x01, 0x06, 0x00, 0x00, 0x01)
}

// TestServerFindByTypeValueDynamic: a dynamic attribute's handler-produced
// value must participate in the match. The pre-fix code never adopted the
// handler output, so dynamic attributes could not be found.
func TestServerFindByTypeValueDynamic(t *testing.T) {
	svc, _ := testService(nil) // char value handler yields 0x2A at h3
	s := newTestServer(t, newOnceConn(), svc)
	wantBytes(t, rsp(s, FindByTypeValueRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x56, 0x2A, 0x2A),
		FindByTypeValueResponseCode, 0x03, 0x00, 0x03, 0x00)
}

func TestServerReadByTypeWire(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)

	// Characteristic declaration: handle + value (properties 0x0E, value
	// handle 3, uuid 0x2A56), entry length 7.
	wantBytes(t, rsp(s, ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x03, 0x28),
		ReadByTypeResponseCode, 0x07,
		0x02, 0x00, 0x0E, 0x03, 0x00, 0x56, 0x2A)

	// Dynamic value read via its type.
	wantBytes(t, rsp(s, ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x56, 0x2A),
		ReadByTypeResponseCode, 0x03, 0x03, 0x00, 0x2A)

	// Malformed length; invalid handle range; type not present.
	wantBytes(t, rsp(s, ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x03), 0x01, 0x08, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, ReadByTypeRequestCode, 0x02, 0x00, 0x01, 0x00, 0x03, 0x28), 0x01, 0x08, 0x02, 0x00, 0x01)
	wantBytes(t, rsp(s, ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x99, 0x2A), 0x01, 0x08, 0x01, 0x00, 0x0A)
}

func TestServerReadByTypeMixedLengths(t *testing.T) {
	// Two services with same-typed characteristics of different value
	// lengths: the response carries the first and stops at the mismatch.
	svc1 := ble.NewService(ble.UUID16(0x1815))
	svc1.NewCharacteristic(ble.UUID16(0x2A56)).SetValue([]byte{0x61, 0x62})
	svc2 := ble.NewService(ble.UUID16(0x1816))
	svc2.NewCharacteristic(ble.UUID16(0x2A56)).SetValue([]byte{0x61, 0x62, 0x63})
	s := newTestServer(t, newOnceConn(), svc1, svc2)

	wantBytes(t, rsp(s, ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x56, 0x2A),
		ReadByTypeResponseCode, 0x04, 0x03, 0x00, 0x61, 0x62)
}

func TestServerReadByTypeHandlerError(t *testing.T) {
	svc := ble.NewService(ble.UUID16(0x1815))
	ch := svc.NewCharacteristic(ble.UUID16(0x2A56))
	ch.HandleRead(ble.ReadHandlerFunc(func(req ble.Request, w ble.ResponseWriter) {
		w.SetStatus(ble.ErrAuthentication)
	}))
	s := newTestServer(t, newOnceConn(), svc)
	wantBytes(t, rsp(s, ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x56, 0x2A),
		0x01, 0x08, 0x01, 0x00, 0x05)
}

func TestServerReadByTypeOversizedValue(t *testing.T) {
	// A dynamic value longer than one entry can hold is truncated to the
	// data-list capacity (21 bytes at MTU 23 -> 19 value bytes).
	long := bytes.Repeat([]byte{0x5A}, 21)
	svc := ble.NewService(ble.UUID16(0x1815))
	ch := svc.NewCharacteristic(ble.UUID16(0x2A56))
	ch.HandleRead(ble.ReadHandlerFunc(func(req ble.Request, w ble.ResponseWriter) {
		w.Write(long)
	}))
	s := newTestServer(t, newOnceConn(), svc)

	want := append([]byte{ReadByTypeResponseCode, 21, 0x03, 0x00}, long[:19]...)
	got := rsp(s, ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x56, 0x2A)
	if !bytes.Equal(got, want) {
		t.Fatalf("response = [% X], want [% X]", got, want)
	}
}

func TestServerReadByGroupTypeWire(t *testing.T) {
	svc1, _ := testService(nil)
	svc2 := ble.NewService(ble.UUID16(0x1816))
	svc2.NewCharacteristic(ble.UUID16(0x2A57)).SetValue([]byte{9})
	s := newTestServer(t, newOnceConn(), svc1, svc2)

	// Primary-service discovery: two 6-byte entries (handle, end handle,
	// 16-bit service uuid).
	wantBytes(t, rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28),
		ReadByGroupTypeResponseCode, 0x06,
		0x01, 0x00, 0x03, 0x00, 0x15, 0x18,
		0x04, 0x00, 0xFF, 0xFF, 0x16, 0x18)

	// Malformed length; invalid handle range.
	wantBytes(t, rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00), 0x01, 0x10, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, ReadByGroupTypeRequestCode, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x28), 0x01, 0x10, 0x00, 0x00, 0x01)
}

// TestServerReadByGroupTypeFilters: only attributes of the requested group
// type may appear. The pre-fix code returned every attribute in the handle
// range, so a Secondary Service query leaked the primary services.
func TestServerReadByGroupTypeFilters(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)
	wantBytes(t, rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x01, 0x28),
		0x01, 0x10, 0x01, 0x00, 0x0A)
}

func TestServerReadWire(t *testing.T) {
	written := []byte(nil)
	svc, _ := testService(&written)
	s := newTestServer(t, newOnceConn(), svc)

	// Dynamic read; malformed length; invalid handle.
	wantBytes(t, rsp(s, ReadRequestCode, 0x03, 0x00), ReadResponseCode, 0x2A)
	wantBytes(t, rsp(s, ReadRequestCode, 0x03), 0x01, 0x0A, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, ReadRequestCode, 0x63, 0x00), 0x01, 0x0A, 0x63, 0x00, 0x01)
}

// TestServerReadLongStaticValue: a static value longer than ATT_MTU-1 is
// truncated to what the MTU allows. The pre-fix code grew a bytes.Buffer
// past the tx buffer and panicked slicing the response.
func TestServerReadLongStaticValue(t *testing.T) {
	long := make([]byte, 40)
	for i := range long {
		long[i] = byte(i)
	}
	s := newTestServer(t, newOnceConn(), staticService(long))

	want := append([]byte{ReadResponseCode}, long[:22]...)
	got := rsp(s, ReadRequestCode, 0x03, 0x00)
	if !bytes.Equal(got, want) {
		t.Fatalf("read of long value = [% X], want [% X]", got, want)
	}
}

// TestServerReadBlobWire: Read Blob honors the requested offset on static
// values (the pre-fix code ignored it and re-sent the value from the
// start, with the same buffer-growth panic as Read).
func TestServerReadBlobWire(t *testing.T) {
	long := make([]byte, 40)
	for i := range long {
		long[i] = byte(i)
	}
	s := newTestServer(t, newOnceConn(), staticService(long))

	want := append([]byte{ReadBlobResponseCode}, long[5:27]...)
	got := rsp(s, ReadBlobRequestCode, 0x03, 0x00, 0x05, 0x00)
	if !bytes.Equal(got, want) {
		t.Fatalf("blob at offset 5 = [% X], want [% X]", got, want)
	}

	// Offset == len yields an empty part; offset past the end is an error.
	wantBytes(t, rsp(s, ReadBlobRequestCode, 0x03, 0x00, 40, 0x00), ReadBlobResponseCode)
	wantBytes(t, rsp(s, ReadBlobRequestCode, 0x03, 0x00, 41, 0x00), 0x01, 0x0C, 0x03, 0x00, 0x07)

	// Malformed length; invalid handle.
	wantBytes(t, rsp(s, ReadBlobRequestCode, 0x03, 0x00, 0x05), 0x01, 0x0C, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, ReadBlobRequestCode, 0x63, 0x00, 0x00, 0x00), 0x01, 0x0C, 0x63, 0x00, 0x01)
}

func TestServerWriteWire(t *testing.T) {
	var written []byte
	svc, _ := testService(&written)
	s := newTestServer(t, newOnceConn(), svc)

	wantBytes(t, rsp(s, WriteRequestCode, 0x03, 0x00, 'h', 'i'), WriteResponseCode)
	if !bytes.Equal(written, []byte("hi")) {
		t.Fatalf("write delivered %q", written)
	}

	// Malformed length; invalid handle; write to a read-only attribute.
	wantBytes(t, rsp(s, WriteRequestCode, 0x03), 0x01, 0x12, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, WriteRequestCode, 0x63, 0x00, 0x41), 0x01, 0x12, 0x63, 0x00, 0x01)

	ro := newTestServer(t, newOnceConn(), staticService([]byte{1}))
	wantBytes(t, rsp(ro, WriteRequestCode, 0x03, 0x00, 0x41), 0x01, 0x12, 0x03, 0x00, 0x03)
}

func TestServerWriteCommand(t *testing.T) {
	var written []byte
	svc, _ := testService(&written)
	s := newTestServer(t, newOnceConn(), svc)

	// Commands never generate a response, valid or not.
	if got := rsp(s, WriteCommandCode, 0x03, 0x00, 0x41); got != nil {
		t.Fatalf("write command response = [% X], want none", got)
	}
	if !bytes.Equal(written, []byte{0x41}) {
		t.Fatalf("write command delivered %q", written)
	}
	if got := rsp(s, WriteCommandCode, 0x03, 0x00); got != nil {
		t.Fatalf("short write command response = [% X], want none", got)
	}
	if got := rsp(s, WriteCommandCode, 0x63, 0x00, 0x41); got != nil {
		t.Fatalf("bad-handle write command response = [% X], want none", got)
	}
}

func TestServerPrepareExecuteWrite(t *testing.T) {
	var written []byte
	svc, _ := testService(&written)
	s := newTestServer(t, newOnceConn(), svc)

	// Two prepared parts (responses echo the request), then execute.
	wantBytes(t, rsp(s, PrepareWriteRequestCode, 0x03, 0x00, 0x00, 0x00, 'h', 'i'),
		PrepareWriteResponseCode, 0x03, 0x00, 0x00, 0x00, 'h', 'i')
	wantBytes(t, rsp(s, PrepareWriteRequestCode, 0x03, 0x00, 0x02, 0x00, '!'),
		PrepareWriteResponseCode, 0x03, 0x00, 0x02, 0x00, '!')
	wantBytes(t, rsp(s, ExecuteWriteRequestCode, 0x01), ExecuteWriteResponseCode)
	if !bytes.Equal(written, []byte("hi!")) {
		t.Fatalf("executed write delivered %q", written)
	}

	// Cancel discards the staged value: the follow-up execute succeeds
	// trivially and the handler is not invoked again.
	written = nil
	wantBytes(t, rsp(s, PrepareWriteRequestCode, 0x03, 0x00, 0x00, 0x00, 'x'),
		PrepareWriteResponseCode, 0x03, 0x00, 0x00, 0x00, 'x')
	wantBytes(t, rsp(s, ExecuteWriteRequestCode, 0x00), ExecuteWriteResponseCode)
	wantBytes(t, rsp(s, ExecuteWriteRequestCode, 0x01), ExecuteWriteResponseCode)
	if written != nil {
		t.Fatalf("cancelled write still delivered %q", written)
	}

	// Prepare errors: invalid handle; write to a read-only attribute.
	wantBytes(t, rsp(s, PrepareWriteRequestCode, 0x63, 0x00, 0x00, 0x00, 'x'), 0x01, 0x16, 0x63, 0x00, 0x01)
	ro := newTestServer(t, newOnceConn(), staticService([]byte{1}))
	wantBytes(t, rsp(ro, PrepareWriteRequestCode, 0x03, 0x00, 0x00, 0x00, 'x'), 0x01, 0x16, 0x03, 0x00, 0x03)
}

// TestServerPrepareWriteTruncatedPDU: a Prepare Write PDU shorter than its
// 5-byte fixed header must be rejected as an invalid PDU. The pre-fix code
// validated only >= 3 bytes and panicked slicing the part value at r[5:].
func TestServerPrepareWriteTruncatedPDU(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)
	wantBytes(t, rsp(s, PrepareWriteRequestCode, 0x03, 0x00, 0x00), 0x01, 0x16, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, PrepareWriteRequestCode), 0x01, 0x16, 0x00, 0x00, 0x04)
}

// TestServerExecuteWriteWithoutPrepare: executing an empty prepare queue
// succeeds trivially. The pre-fix code passed the nil staged attribute
// into handleATT and panicked.
func TestServerExecuteWriteWithoutPrepare(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)
	wantBytes(t, rsp(s, ExecuteWriteRequestCode, 0x01), ExecuteWriteResponseCode)

	// Malformed: truncated PDU; undefined flags value.
	wantBytes(t, rsp(s, ExecuteWriteRequestCode), 0x01, 0x18, 0x00, 0x00, 0x04)
	wantBytes(t, rsp(s, ExecuteWriteRequestCode, 0x02), 0x01, 0x18, 0x00, 0x00, 0x04)
}

// TestServerPrepareQueueBounded: the staged prepare-write payload is capped;
// past the cap the server answers Prepare Queue Full instead of buffering
// attacker-controlled data without bound.
func TestServerPrepareQueueBounded(t *testing.T) {
	old := maxPreparedWriteBytes
	maxPreparedWriteBytes = 4
	t.Cleanup(func() { maxPreparedWriteBytes = old })

	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)

	wantBytes(t, rsp(s, PrepareWriteRequestCode, 0x03, 0x00, 0x00, 0x00, 1, 2, 3),
		PrepareWriteResponseCode, 0x03, 0x00, 0x00, 0x00, 1, 2, 3)
	wantBytes(t, rsp(s, PrepareWriteRequestCode, 0x03, 0x00, 0x03, 0x00, 4, 5), 0x01, 0x16, 0x03, 0x00, 0x09)
}

// TestServerReadBlobDynamic: a blob read of a handler-backed attribute
// passes the offset through to the handler.
func TestServerReadBlobDynamic(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)
	wantBytes(t, rsp(s, ReadBlobRequestCode, 0x03, 0x00, 0x00, 0x00), ReadBlobResponseCode, 0x2A)
}

// groupServer builds a Server over hand-rolled grouping attributes,
// bypassing NewDB (which only produces static service declarations).
func groupServer(t *testing.T, attrs ...*attr) *Server {
	t.Helper()
	s, err := NewServer(&DB{attrs: attrs, base: 1}, newOnceConn())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestServerReadByGroupTypeDynamic(t *testing.T) {
	// A grouping attribute whose value comes from a read handler.
	dyn := &attr{h: 1, endh: 1, typ: ble.UUID16(0x2800), rh: ble.ReadHandlerFunc(
		func(req ble.Request, w ble.ResponseWriter) { w.Write([]byte{0x15, 0x18}) })}
	s := groupServer(t, dyn)
	wantBytes(t, rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28),
		ReadByGroupTypeResponseCode, 0x06, 0x01, 0x00, 0x01, 0x00, 0x15, 0x18)

	// A dynamic grouping attribute without a read handler errors out.
	s = groupServer(t, &attr{h: 1, endh: 1, typ: ble.UUID16(0x2800)})
	wantBytes(t, rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28),
		0x01, 0x10, 0x01, 0x00, 0x02)
}

func TestServerReadByGroupTypeOversizedValue(t *testing.T) {
	// A grouping value longer than an entry can hold is truncated to the
	// data-list capacity (21 at MTU 23 -> 17 value bytes); a >251-byte
	// value also exercises the 255 length-octet cap.
	long := bytes.Repeat([]byte{0x77}, 300)
	s := groupServer(t, &attr{h: 1, endh: 1, typ: ble.UUID16(0x2800), v: long})

	want := append([]byte{ReadByGroupTypeResponseCode, 21, 0x01, 0x00, 0x01, 0x00}, long[:17]...)
	got := rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28)
	if !bytes.Equal(got, want) {
		t.Fatalf("response = [% X], want [% X]", got, want)
	}
}

func TestServerReadByGroupTypeCapacity(t *testing.T) {
	// Four 6-byte entries need 24 bytes; only 3 fit the 21-byte data list.
	var attrs []*attr
	for h := uint16(1); h <= 4; h++ {
		attrs = append(attrs, &attr{h: h, endh: h, typ: ble.UUID16(0x2800), v: []byte{0x15, 0x18}})
	}
	s := groupServer(t, attrs...)
	got := rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28)
	if len(got) != 2+3*6 {
		t.Fatalf("response holds %d bytes, want 20 (3 entries)", len(got))
	}

	// A dynamic attribute after the list is nearly full must not be probed
	// with a negative-capacity scratch buffer (which would panic make).
	attrs[3] = &attr{h: 4, endh: 4, typ: ble.UUID16(0x2800), rh: ble.ReadHandlerFunc(
		func(req ble.Request, w ble.ResponseWriter) { w.Write([]byte{0x15, 0x18}) })}
	s = groupServer(t, attrs...)
	got = rsp(s, ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28)
	if len(got) != 2+3*6 {
		t.Fatalf("response holds %d bytes, want 20 (3 entries, dynamic tail skipped)", len(got))
	}
}

// TestServerLoopWriteError: a failing response write is logged and the
// loop keeps serving until the bearer read fails.
func TestServerLoopWriteError(t *testing.T) {
	svc, _ := testService(nil)
	f := &failWriteConn{onceConn: newOnceConn()}
	s := newTestServer(t, f, svc)

	done := make(chan struct{})
	go func() { s.Loop(); close(done) }()

	f.in <- []byte{ExchangeMTURequestCode, 0xB7, 0x00} // response write fails
	f.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server Loop did not exit")
	}
}

func TestServerUnsupportedRequests(t *testing.T) {
	svc, _ := testService(nil)
	s := newTestServer(t, newOnceConn(), svc)
	wantBytes(t, rsp(s, ReadMultipleRequestCode, 0x01, 0x00, 0x02, 0x00), 0x01, 0x0E, 0x00, 0x00, 0x06)
	wantBytes(t, rsp(s, SignedWriteCommandCode, 0x03, 0x00), 0x01, 0xD2, 0x00, 0x00, 0x06)
}

func TestServerNotifyWire(t *testing.T) {
	svc, _ := testService(nil)
	f := newOnceConn()
	s := newTestServer(t, f, svc)

	if _, err := s.notify(3, []byte{0xAA, 0xBB}); err != nil {
		t.Fatal(err)
	}
	wantBytes(t, recvWrite(t, f.writes), HandleValueNotificationCode, 0x03, 0x00, 0xAA, 0xBB)

	// Oversized payloads are truncated to ATT_MTU-3.
	long := bytes.Repeat([]byte{0xCC}, 30)
	if _, err := s.notify(3, long); err != nil {
		t.Fatal(err)
	}
	w := recvWrite(t, f.writes)
	want := append([]byte{HandleValueNotificationCode, 0x03, 0x00}, long[:20]...)
	if !bytes.Equal(w, want) {
		t.Fatalf("oversized notification = [% X], want [% X]", w, want)
	}
}

func TestServerIndicateWire(t *testing.T) {
	svc, _ := testService(nil)
	f := newOnceConn()
	s := newTestServer(t, f, svc)

	// Confirmed indication.
	go func() { s.chConfirm <- true }()
	if _, err := s.indicate(3, []byte{0x11}); err != nil {
		t.Fatal(err)
	}
	wantBytes(t, recvWrite(t, f.writes), HandleValueIndicationCode, 0x03, 0x00, 0x11)

	// Unconfirmed indication times out via the package-level ATT timeout.
	old := seqProtoTimeout
	seqProtoTimeout = 50 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })
	if _, err := s.indicate(3, []byte{0x22}); !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("unconfirmed indicate = %v, want ErrSeqProtoTimeout", err)
	}

	// A closed confirmation channel (bearer shut down) surfaces as a
	// closed pipe.
	close(s.chConfirm)
	if _, err := s.indicate(3, []byte{0x33}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("indicate after shutdown = %v, want io.ErrClosedPipe", err)
	}
}
