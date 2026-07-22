package gatt

// Tests for the API repairs: ReadRSSI's (int, error) chain, the real
// DiscoverIncludedServices implementation, the lazy GAP Device Name read
// behind Name(), and the ErrNoCCCD sentinel.

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/att"
)

// serveATT answers each request PDU the client writes with respond's reply
// (nil replies are dropped), until the conn closes. It is the free-form
// sibling of fakeServer for tests that need request-specific responses.
func serveATT(conn *fakeConn, respond func(req []byte) []byte) {
	go func() {
		for {
			var req []byte
			select {
			case req = <-conn.writes:
			case <-conn.closed:
				return
			}
			rsp := respond(req)
			if rsp == nil {
				continue
			}
			select {
			case conn.in <- rsp:
			case <-conn.closed:
				return
			}
		}
	}()
}

func newRespondingClient(t *testing.T, respond func(req []byte) []byte) *Client {
	t.Helper()
	conn := newFakeConn()
	serveATT(conn, respond)
	cln, err := NewClient(conn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return cln
}

// Read By Type request fields [Vol 3, Part F, 3.4.4.1]: opcode(1),
// starting handle(2), ending handle(2), attribute type(2 or 16).
func rbtStart(req []byte) uint16  { return binary.LittleEndian.Uint16(req[1:3]) }
func rbtUUID16(req []byte) uint16 { return binary.LittleEndian.Uint16(req[5:7]) }

// Read request field: opcode(1), attribute handle(2).
func readHandle(req []byte) uint16 { return binary.LittleEndian.Uint16(req[1:3]) }

func attErr(reqOp byte, handle uint16, code ble.ATTError) []byte {
	return []byte{att.ErrorResponseCode, reqOp, byte(handle), byte(handle >> 8), byte(code)}
}

// ---------------------------------------------------------------------------
// ReadRSSI
// ---------------------------------------------------------------------------

// rssiConn overrides fakeConn's canned RSSI with a configurable result.
type rssiConn struct {
	*fakeConn
	rssi int
	err  error
}

func (c *rssiConn) ReadRSSI() (int, error) { return c.rssi, c.err }

// rssiBlockConn parks ReadRSSI until release is closed, emulating an HCI
// exchange bounded only by the HCI layer's own (long) timeout.
type rssiBlockConn struct {
	*fakeConn
	release chan struct{}
}

func (c *rssiBlockConn) ReadRSSI() (int, error) {
	<-c.release
	return -7, nil
}

func TestReadRSSISuccess(t *testing.T) {
	cln, _ := newTestClient(t)
	rssi, err := cln.ReadRSSI(testCtx(t))
	if err != nil || rssi != -42 {
		t.Fatalf("ReadRSSI = (%d, %v), want (-42, nil)", rssi, err)
	}
}

// A failed exchange must surface its error, not a fabricated 0 dBm reading.
func TestReadRSSICommandFailure(t *testing.T) {
	sentinel := errors.New("rssi exchange failed")
	conn := &rssiConn{fakeConn: newFakeConn(), err: sentinel}
	cln, err := NewClient(conn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	rssi, err := cln.ReadRSSI(testCtx(t))
	if rssi != 0 || !errors.Is(err, sentinel) {
		t.Fatalf("ReadRSSI = (%d, %v), want (0, %v)", rssi, err, sentinel)
	}
}

// ctx bounds the caller's wait even though the underlying exchange cannot be
// interrupted: expiry returns ctx.Err() and the exchange's late result is
// discarded.
func TestReadRSSIContextExpiry(t *testing.T) {
	release := make(chan struct{})
	conn := &rssiBlockConn{fakeConn: newFakeConn(), release: release}
	cln, err := NewClient(conn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close(); close(release) })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	rssi, err := cln.ReadRSSI(ctx)
	if rssi != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReadRSSI = (%d, %v), want (0, context.DeadlineExceeded)", rssi, err)
	}
}

// ---------------------------------------------------------------------------
// DiscoverIncludedServices
// ---------------------------------------------------------------------------

func TestDiscoverIncludedServices16Bit(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] != att.ReadByTypeRequestCode || rbtUUID16(req) != 0x2802 {
			return nil
		}
		if rbtStart(req) <= 2 {
			// One include at attribute handle 2: included service
			// 0x20..0x28, 16-bit UUID 0x180F.
			return []byte{att.ReadByTypeResponseCode, 8,
				0x02, 0x00, 0x20, 0x00, 0x28, 0x00, 0x0F, 0x18}
		}
		return errAttrNotFound(req[0]) // walk termination
	})
	ctx := testCtx(t)

	s := &ble.Service{Handle: 1, EndHandle: 0x10}
	got, err := cln.DiscoverIncludedServices(ctx, nil, s)
	if err != nil {
		t.Fatalf("DiscoverIncludedServices: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("discovered %d includes, want 1", len(got))
	}
	is := got[0]
	if !is.UUID.Equal(ble.UUID16(0x180F)) || is.Handle != 0x20 || is.EndHandle != 0x28 {
		t.Fatalf("include = {uuid: %s, h: 0x%X, endh: 0x%X}, want {180f, 0x20, 0x28}", is.UUID, is.Handle, is.EndHandle)
	}
	if len(s.Includes) != 1 || s.Includes[0] != is {
		t.Fatalf("s.Includes = %v, want the discovered include recorded", s.Includes)
	}

	// Filter mismatch: the walk runs but nothing matches.
	s2 := &ble.Service{Handle: 1, EndHandle: 0x10}
	got, err = cln.DiscoverIncludedServices(ctx, []ble.UUID{ble.UUID16(0x1234)}, s2)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("filtered (no match) = (%v, %v), want ([], nil)", got, err)
	}

	// Filter match.
	s3 := &ble.Service{Handle: 1, EndHandle: 0x10}
	got, err = cln.DiscoverIncludedServices(ctx, []ble.UUID{ble.UUID16(0x180F)}, s3)
	if err != nil || len(got) != 1 {
		t.Fatalf("filtered (match) = (%v, %v), want one include", got, err)
	}
}

// A 128-bit included service UUID is not carried in the include declaration;
// the client must follow up with a read of the included service declaration.
func TestDiscoverIncludedServices128Bit(t *testing.T) {
	raw := []byte{0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12,
		0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12}
	cln := newRespondingClient(t, func(req []byte) []byte {
		switch req[0] {
		case att.ReadByTypeRequestCode:
			if rbtUUID16(req) != 0x2802 {
				return nil
			}
			if rbtStart(req) <= 2 {
				// length 6: no UUID in the declaration.
				return []byte{att.ReadByTypeResponseCode, 6,
					0x02, 0x00, 0x30, 0x00, 0x38, 0x00}
			}
			return errAttrNotFound(req[0])
		case att.ReadRequestCode:
			if readHandle(req) != 0x30 {
				return attErr(req[0], readHandle(req), ble.ErrInvalidHandle)
			}
			// The included service declaration's value: its 128-bit UUID.
			return append([]byte{att.ReadResponseCode}, raw...)
		}
		return nil
	})

	s := &ble.Service{Handle: 1, EndHandle: 0x40}
	got, err := cln.DiscoverIncludedServices(testCtx(t), nil, s)
	if err != nil {
		t.Fatalf("DiscoverIncludedServices: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("discovered %d includes, want 1", len(got))
	}
	is := got[0]
	if !is.UUID.Equal(ble.UUID(raw)) || is.Handle != 0x30 || is.EndHandle != 0x38 {
		t.Fatalf("include = {uuid: %s, h: 0x%X, endh: 0x%X}, want {%s, 0x30, 0x38}",
			is.UUID, is.Handle, is.EndHandle, ble.UUID(raw))
	}
}

// A failing follow-up read of the included service declaration surfaces as a
// wrapped error, not a silently skipped include.
func TestDiscoverIncludedServices128BitReadFails(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		switch req[0] {
		case att.ReadByTypeRequestCode:
			if rbtStart(req) <= 2 {
				return []byte{att.ReadByTypeResponseCode, 6,
					0x02, 0x00, 0x30, 0x00, 0x38, 0x00}
			}
			return errAttrNotFound(req[0])
		case att.ReadRequestCode:
			return attErr(req[0], readHandle(req), ble.ErrReadNotPerm)
		}
		return nil
	})

	s := &ble.Service{Handle: 1, EndHandle: 0x40}
	got, err := cln.DiscoverIncludedServices(testCtx(t), nil, s)
	if got != nil || !errors.Is(err, ble.ErrReadNotPerm) {
		t.Fatalf("DiscoverIncludedServices = (%v, %v), want (nil, ErrReadNotPerm)", got, err)
	}
}

// No includes: an empty non-nil slice and a nil error. The old stub returned
// (nil, nil) without asking the peer — fake success; this result means the
// walk ran and the ErrAttrNotFound termination was actually observed.
func TestDiscoverIncludedServicesEmpty(t *testing.T) {
	cln, _ := newTestClient(t) // fakeServer has no includes
	s := &ble.Service{Handle: 1, EndHandle: 5}
	got, err := cln.DiscoverIncludedServices(testCtx(t), nil, s)
	if err != nil {
		t.Fatalf("DiscoverIncludedServices: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("DiscoverIncludedServices = %v, want an empty non-nil slice", got)
	}
}

// An ATT error other than ErrAttrNotFound mid-walk aborts the discovery.
func TestDiscoverIncludedServicesErrorMidWalk(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] != att.ReadByTypeRequestCode {
			return nil
		}
		if rbtStart(req) <= 2 {
			return []byte{att.ReadByTypeResponseCode, 8,
				0x02, 0x00, 0x20, 0x00, 0x28, 0x00, 0x0F, 0x18}
		}
		return attErr(req[0], rbtStart(req), ble.ErrUnlikely)
	})

	s := &ble.Service{Handle: 1, EndHandle: 0x10}
	got, err := cln.DiscoverIncludedServices(testCtx(t), nil, s)
	if got != nil || !errors.Is(err, ble.ErrUnlikely) {
		t.Fatalf("DiscoverIncludedServices = (%v, %v), want (nil, ErrUnlikely)", got, err)
	}
}

// An include entry with an invalid declaration length is rejected.
func TestDiscoverIncludedServicesInvalidLength(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] != att.ReadByTypeRequestCode {
			return nil
		}
		return []byte{att.ReadByTypeResponseCode, 4, 0x02, 0x00, 0x20, 0x00}
	})

	s := &ble.Service{Handle: 1, EndHandle: 0x10}
	got, err := cln.DiscoverIncludedServices(testCtx(t), nil, s)
	if got != nil || err == nil {
		t.Fatalf("DiscoverIncludedServices = (%v, %v), want (nil, invalid-length error)", got, err)
	}
}

// An include entry at attribute handle 0xFFFF ends the walk without the
// start-handle increment overflowing back to 0.
func TestDiscoverIncludedServicesHandleRangeEnd(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] != att.ReadByTypeRequestCode {
			return nil
		}
		return []byte{att.ReadByTypeResponseCode, 8,
			0xFF, 0xFF, 0x20, 0x00, 0x28, 0x00, 0x0F, 0x18}
	})

	s := &ble.Service{Handle: 1, EndHandle: 0xFFFF}
	got, err := cln.DiscoverIncludedServices(testCtx(t), nil, s)
	if err != nil || len(got) != 1 {
		t.Fatalf("DiscoverIncludedServices = (%v, %v), want one include", got, err)
	}
}

// ---------------------------------------------------------------------------
// Name
// ---------------------------------------------------------------------------

// nameResponder serves the GAP Device Name characteristic at value handle 3
// and counts every ATT request, so caching is observable as absent traffic.
type nameResponder struct {
	reqs     atomic.Int32
	failRead atomic.Bool
	name     string
}

func (r *nameResponder) respond(req []byte) []byte {
	r.reqs.Add(1)
	switch req[0] {
	case att.ReadByTypeRequestCode:
		if rbtUUID16(req) != 0x2A00 {
			return errAttrNotFound(req[0])
		}
		// Entry: value handle 3 + a truncated prefix of the value, forcing
		// the follow-up read-by-handle for the full name.
		return []byte{att.ReadByTypeResponseCode, 5, 0x03, 0x00, 'x', 'x', 'x'}
	case att.ReadRequestCode:
		if r.failRead.Load() {
			return attErr(req[0], readHandle(req), ble.ErrReadNotPerm)
		}
		if readHandle(req) != 3 {
			return attErr(req[0], readHandle(req), ble.ErrInvalidHandle)
		}
		return append([]byte{att.ReadResponseCode}, r.name...)
	}
	return nil
}

func TestNameLazyReadAndCache(t *testing.T) {
	r := &nameResponder{name: "minimon"}
	cln := newRespondingClient(t, r.respond)

	if got := cln.Name(); got != "minimon" {
		t.Fatalf("Name() = %q, want \"minimon\"", got)
	}
	n := r.reqs.Load()
	if n == 0 {
		t.Fatal("Name() issued no ATT traffic; it cannot have read the GAP name")
	}
	if got := cln.Name(); got != "minimon" {
		t.Fatalf("second Name() = %q, want \"minimon\"", got)
	}
	if r.reqs.Load() != n {
		t.Fatalf("second Name() issued ATT traffic (%d -> %d requests); the name was not cached", n, r.reqs.Load())
	}
}

// No GAP Device Name on the peer: Name() stays an error-free convenience
// accessor and returns "".
func TestNameNoGAPService(t *testing.T) {
	cln, _ := newTestClient(t) // fakeServer answers 0x2A00 with ErrAttrNotFound
	if got := cln.Name(); got != "" {
		t.Fatalf("Name() = %q, want \"\" when the peer has no GAP device name", got)
	}
}

// A failed value read yields ""; the failure is not cached, so a later call
// retries and can succeed.
func TestNameErrorNotCached(t *testing.T) {
	r := &nameResponder{name: "dev"}
	r.failRead.Store(true)
	cln := newRespondingClient(t, r.respond)

	if got := cln.Name(); got != "" {
		t.Fatalf("Name() with failing read = %q, want \"\"", got)
	}
	r.failRead.Store(false)
	if got := cln.Name(); got != "dev" {
		t.Fatalf("Name() after the peer recovered = %q, want \"dev\"", got)
	}
}

// A syntactically valid but useless Read By Type response (entry too short
// to carry a handle) yields "" rather than a panic or garbage.
func TestNameMalformedResponse(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] != att.ReadByTypeRequestCode {
			return nil
		}
		return []byte{att.ReadByTypeResponseCode, 1, 0x00, 0x00}
	})
	if got := cln.Name(); got != "" {
		t.Fatalf("Name() on malformed response = %q, want \"\"", got)
	}
}

// ---------------------------------------------------------------------------
// DiscoverProfile error wrapping
// ---------------------------------------------------------------------------

// DiscoverProfile's stage errors are wrapped with %w, so the underlying
// typed ATT error stays assertable through the "can't discover ..." prefix.
// failAt controls which discovery stage the fake peer rejects.
func profileFailResponder(failAt byte) func(req []byte) []byte {
	counts := map[byte]int{} // serveATT calls respond from one goroutine
	return func(req []byte) []byte {
		op := req[0]
		if op == failAt {
			return attErr(op, 0, ble.ErrUnlikely)
		}
		counts[op]++
		if counts[op] > 1 {
			return errAttrNotFound(op)
		}
		switch op {
		case att.ReadByGroupTypeRequestCode: // one service, handles 1..5
			return []byte{att.ReadByGroupTypeResponseCode, 6,
				0x01, 0x00, 0x05, 0x00, 0x0F, 0x18}
		case att.ReadByTypeRequestCode: // one characteristic, value handle 3
			return []byte{att.ReadByTypeResponseCode, 7,
				0x02, 0x00, 0x3A, 0x03, 0x00, 0x19, 0x2A}
		case att.FindInformationRequestCode: // the CCCD at handle 4
			return []byte{att.FindInformationResponseCode, 0x01,
				0x04, 0x00, 0x02, 0x29}
		}
		return nil
	}
}

func TestDiscoverProfileWrapsStageErrors(t *testing.T) {
	stages := map[string]byte{
		"services":        att.ReadByGroupTypeRequestCode,
		"characteristics": att.ReadByTypeRequestCode,
		"descriptors":     att.FindInformationRequestCode,
	}
	for name, failAt := range stages {
		t.Run(name, func(t *testing.T) {
			cln := newRespondingClient(t, profileFailResponder(failAt))
			p, err := cln.DiscoverProfile(testCtx(t), false)
			if p != nil || !errors.Is(err, ble.ErrUnlikely) {
				t.Fatalf("DiscoverProfile = (%v, %v), want (nil, wrapped ErrUnlikely)", p, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ErrNoCCCD
// ---------------------------------------------------------------------------

func TestSubscribeNoCCCDSentinel(t *testing.T) {
	cln, _ := newTestClient(t)
	ctx := testCtx(t)

	c := &ble.Characteristic{ValueHandle: 3} // no CCCD discovered
	if err := cln.Subscribe(ctx, c, false, func([]byte) {}); !errors.Is(err, ErrNoCCCD) {
		t.Fatalf("Subscribe without CCCD = %v, want ErrNoCCCD", err)
	}
	if err := cln.Unsubscribe(ctx, c, true); !errors.Is(err, ErrNoCCCD) {
		t.Fatalf("Unsubscribe without CCCD = %v, want ErrNoCCCD", err)
	}
}

// ---------------------------------------------------------------------------
// Peer-controlled entry lengths in discovery responses
// ---------------------------------------------------------------------------

// A Read By Group Type entry length other than 6 (16-bit UUID) or 20 (128-bit
// UUID) cannot be a service entry; slicing on it used to panic.
func TestDiscoverServicesRejectsInvalidEntryLength(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] == att.ReadByGroupTypeRequestCode {
			// length=2 with one complete 2-byte "entry": passes the att
			// layer's divisibility check, reaches the gatt parser.
			return []byte{att.ReadByGroupTypeResponseCode, 0x02, 0xAA, 0xBB}
		}
		return nil
	})
	if _, err := cln.DiscoverServices(testCtx(t), nil); err == nil {
		t.Fatal("DiscoverServices accepted an invalid entry length")
	}
}

// A Read By Type entry length other than 7 or 21 cannot be a characteristic
// declaration; slicing on it used to panic.
func TestDiscoverCharacteristicsRejectsInvalidEntryLength(t *testing.T) {
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] == att.ReadByTypeRequestCode {
			return []byte{att.ReadByTypeResponseCode, 0x03, 0xAA, 0xBB, 0xCC}
		}
		return nil
	})
	svc := &ble.Service{Handle: 1, EndHandle: 10}
	if _, err := cln.DiscoverCharacteristics(testCtx(t), nil, svc); err == nil {
		t.Fatal("DiscoverCharacteristics accepted an invalid entry length")
	}
}

// HandleNotification is an exported entry point: PDUs below the spec minimum
// (opcode + 2-byte handle) must be dropped, not parsed.
func TestHandleNotificationRunt(t *testing.T) {
	cln, _ := newTestClient(t)
	for _, pdu := range [][]byte{{}, {att.HandleValueNotificationCode}, {att.HandleValueIndicationCode, 0x42}} {
		cln.HandleNotification(pdu) // must not panic
	}
}

// TestNameCachesNoNameOutcome: a peer without a GAP Device Name must cost
// at most one pair of round trips — the negative outcome is cached, so a
// Name() in a logging path doesn't re-run ATT exchanges (under the
// client-wide lock) on every call.
func TestNameCachesNoNameOutcome(t *testing.T) {
	var reads atomic.Int64
	cln := newRespondingClient(t, func(req []byte) []byte {
		if req[0] == att.ReadByTypeRequestCode && rbtUUID16(req) == 0x2A00 {
			reads.Add(1)
			return attErr(req[0], rbtStart(req), ble.ErrAttrNotFound)
		}
		return nil
	})

	for i := 0; i < 3; i++ {
		if got := cln.Name(); got != "" {
			t.Fatalf("Name() = %q, want \"\"", got)
		}
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("peer without a name was asked %d times, want 1 (cached)", got)
	}
}
