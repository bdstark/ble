package att

import (
	"testing"
	"time"

	"github.com/go-ble/ble"
)

// TestServerLoop drives requests through Server.Loop: a spurious
// confirmation, an MTU exchange, a read, an unknown opcode, and the
// shutdown cleanup of an armed CCC.
func TestServerLoop(t *testing.T) {
	f := newOnceConn()
	svc, ch := testService(nil)
	db := NewDB([]*ble.Service{svc}, 1)
	s, err := NewServer(db, f)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-arm CCC state so Loop's cleanup pass logs it and closes the
	// notifier. (cccNotify only: cccIndicate cleanup needs conn.in wired.)
	s.conn.cccs[ch.Handle] = cccNotify
	s.conn.nn[ch.Handle] = ble.NewNotifier(func(b []byte) (int, error) { return len(b), nil })

	done := make(chan struct{})
	go func() { s.Loop(); close(done) }()

	// Spurious confirmation: no indication is pending.
	f.in <- []byte{HandleValueConfirmationCode}

	// MTU exchange.
	f.in <- []byte{ExchangeMTURequestCode, 0xB7, 0x00}
	w := recvWrite(t, f.writes)
	if len(w) != 3 || w[0] != ExchangeMTUResponseCode {
		t.Fatalf("MTU response = % X", w)
	}

	// Read the characteristic value (via its read handler).
	vh := ch.ValueHandle
	f.in <- []byte{ReadRequestCode, byte(vh), byte(vh >> 8)}
	w = recvWrite(t, f.writes)
	if len(w) != 2 || w[0] != ReadResponseCode || w[1] != 0x2A {
		t.Fatalf("read response = % X", w)
	}

	// Unknown opcode is answered with an error response.
	f.in <- []byte{0xEE}
	w = recvWrite(t, f.writes)
	if len(w) != 5 || w[0] != ErrorResponseCode || w[1] != 0xEE {
		t.Fatalf("unknown-op error response = % X", w)
	}

	f.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server Loop did not exit")
	}
}

// TestDumpAttributesDisabled covers the early return when debug logging is
// off (the test logger installed by TestMain defaults to info level).
func TestDumpAttributesDisabled(t *testing.T) {
	DumpAttributes([]*attr{{h: 1, typ: ble.UUID16(0x2800), v: []byte{1}}})
}
