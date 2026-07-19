package hci

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/linux/hci/cmd"
	"github.com/go-ble/ble/linux/hci/evt"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// pollWrittenOpcode fails the test unless a command with the given opcode
// reaches the fake socket within the deadline.
func pollWrittenOpcode(t *testing.T, skt *fakeSkt, opcode int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, w := range skt.written() {
			if w[0] == pktTypeCommand && int(w[1])|int(w[2])<<8 == opcode {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("command %04X never reached the socket", opcode)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ltkRequestWirePkt builds a full HCI LE Long Term Key Request event packet
// for the given connection handle (random number and EDIV zeroed).
func ltkRequestWirePkt(handle uint16) []byte {
	b := []byte{
		pktTypeEvent, 0x3E, // LE Meta
		13, // parameter length: subevent + handle + random(8) + EDIV(2)
		evt.LELongTermKeyRequestSubCode,
		byte(handle), byte(handle >> 8),
	}
	return append(b, make([]byte, 10)...)
}

// syncWriter is a goroutine-safe log sink.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// stubAdv is a minimal ble.Advertisement for driving AdvertiseAdv.
type stubAdv struct{ name string }

func (s stubAdv) LocalName() string {
	if s.name != "" {
		return s.name
	}
	return "stub-name"
}
func (stubAdv) ManufacturerData() []byte       { return []byte{0x34, 0x12, 0xAA, 0xBB} }
func (stubAdv) ServiceData() []ble.ServiceData { return nil }
func (stubAdv) Services() []ble.UUID           { return []ble.UUID{ble.UUID16(0x180D)} }
func (stubAdv) OverflowService() []ble.UUID    { return nil }
func (stubAdv) TxPowerLevel() int              { return 0 }
func (stubAdv) Connectable() bool              { return true }
func (stubAdv) SolicitedService() []ble.UUID   { return nil }
func (stubAdv) RSSI() int                      { return 0 }
func (stubAdv) Addr() ble.Addr                 { return nil }

// advertiseCalls enumerates every gap.go advertise method that goes through
// SetAdvertisement.
func advertiseCalls(h *HCI) map[string]func() error {
	u := ble.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	return map[string]func() error{
		"AdvertiseAdv":             func() error { return h.AdvertiseAdv(stubAdv{}) },
		"AdvertiseNameAndServices": func() error { return h.AdvertiseNameAndServices("stub-name", ble.UUID16(0x180D)) },
		"AdvertiseMfgData":         func() error { return h.AdvertiseMfgData(0x1234, []byte{0xAA, 0xBB}) },
		"AdvertiseServiceData16":   func() error { return h.AdvertiseServiceData16(0x180F, []byte{0x64}) },
		"AdvertiseIBeaconData":     func() error { return h.AdvertiseIBeaconData(make([]byte, 23)) },
		"AdvertiseIBeacon":         func() error { return h.AdvertiseIBeacon(u, 1, 2, -59) },
	}
}

// ---------------------------------------------------------------------------
// Slave connection-complete: failed connect must leave no trace
// ---------------------------------------------------------------------------

// TestLEConnCompleteFailedSlaveNoLeak mirrors the master-role fix for the
// slave path: an inbound connection failing with 0x3E must not create a
// Conn, register a handle, deliver anything to chSlaveConn, or invoke
// connectedHandler with a failed event.
func TestLEConnCompleteFailedSlaveNoLeak(t *testing.T) {
	quietLogger(t)
	h := newConnHCI()
	connected := 0
	h.connectedHandler = func(evt.LEConnectionComplete) { connected++ }
	if err := h.handleLEConnectionComplete(leConnCompletePkt(0x3E, 0x0040, roleSlave)); err != nil {
		t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
	}
	if n := len(h.conns); n != 0 {
		t.Fatalf("failed slave connect left %d conns registered, want 0", n)
	}
	select {
	case c := <-h.chSlaveConn:
		t.Fatalf("failed slave connect delivered conn %v", c)
	default:
	}
	if connected != 0 {
		t.Fatalf("connectedHandler fired %d times for a failed connect, want 0", connected)
	}
}

// ---------------------------------------------------------------------------
// Slave connection-complete: no Accept()er must not wedge sktLoop
// ---------------------------------------------------------------------------

// TestLEConnCompleteSlaveNoAcceptorRefused: with nobody parked in Accept()
// (chSlaveConn unbuffered, no receiver), an inbound slave connection must be
// refused cleanly — Disconnect command on the wire, the disconnect event
// completing the teardown — instead of blocking the delivery and wedging the
// adapter forever. sktLoop must stay fully live throughout.
func TestLEConnCompleteSlaveNoAcceptorRefused(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newTeardownHCI(t, skt)
	h.params.advEnable.AdvertisingEnable = 1 // exercise the re-advertise arm
	connected := 0
	h.connectedHandler = func(evt.LEConnectionComplete) { connected++ }

	if err := h.handleLEConnectionComplete(leConnCompletePkt(0x00, 0x0044, roleSlave)); err != nil {
		t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
	}
	// The refused connection is disconnected, and the advertising toggle
	// still goes out from its own goroutine.
	waitDisconnectCmd(t, skt)
	pollWrittenOpcode(t, skt, (&cmd.LESetAdvertiseEnable{}).OpCode())
	if connected != 1 {
		t.Fatalf("connectedHandler fired %d times, want 1", connected)
	}
	// sktLoop is still live: a full command round-trip works.
	if err := h.Send(&cmd.LESetScanEnable{}, nil); err != nil {
		t.Fatalf("Send after refused slave conn: %v (sktLoop wedged?)", err)
	}

	// The disconnect event finishes the teardown of the registered conn.
	h.muConns.Lock()
	c := h.conns[0x0044]
	h.muConns.Unlock()
	if c == nil {
		t.Fatal("refused slave conn was not registered pending its disconnect event")
	}
	skt.rd <- disconnWirePkt(0x0044)
	waitDisconnected(t, c)
	if n := connCount(h); n != 0 {
		t.Fatalf("h.conns holds %d conns after disconnect, want 0", n)
	}
}

// TestAcceptReceivesSlaveConn: with a listener parked in Accept(), the
// non-blocking hand-off still delivers the connection.
func TestAcceptReceivesSlaveConn(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newTeardownHCI(t, skt)

	got := make(chan ble.Conn, 1)
	errc := make(chan error, 1)
	go func() {
		c, err := h.Accept()
		got <- c
		errc <- err
	}()
	// Let Accept park on chSlaveConn, then complete a slave connection.
	// The hand-off races Accept's park; retry until the listener wins.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := h.handleLEConnectionComplete(leConnCompletePkt(0x00, 0x0045, roleSlave)); err != nil {
			t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
		}
		select {
		case c := <-got:
			if err := <-errc; err != nil || c == nil {
				t.Fatalf("Accept = (%v, %v), want the delivered conn", c, err)
			}
			return
		case <-time.After(20 * time.Millisecond):
			// The listener wasn't parked yet and the conn was refused;
			// absorb the teardown and try again.
			h.muConns.Lock()
			if c, ok := h.conns[0x0045]; ok {
				delete(h.conns, 0x0045)
				c.closeChans()
			}
			h.muConns.Unlock()
		}
		if time.Now().After(deadline) {
			t.Fatal("Accept never received the slave connection")
		}
	}
}

// ---------------------------------------------------------------------------
// LE Long Term Key Request must not stall sktLoop
// ---------------------------------------------------------------------------

// TestLTKRequestRepliesWithoutWedging: the negative reply must reach the
// wire and complete without stalling sktLoop for cmdTimeout (the old
// synchronous Send could only be completed by the very loop it was called
// from).
func TestLTKRequestRepliesWithoutWedging(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newLoopedHCIOn(t, skt, func(h *HCI) {
		h.evth[0x3E] = h.handleLEMeta
		h.subh[evt.LELongTermKeyRequestSubCode] = h.handleLELongTermKeyRequest
	})

	start := time.Now()
	skt.rd <- ltkRequestWirePkt(0x0040)
	pollWrittenOpcode(t, skt, (&cmd.LELongTermKeyRequestNegativeReply{}).OpCode())
	// sktLoop is live and the command path is not starved.
	if err := h.Send(&cmd.LESetScanEnable{}, nil); err != nil {
		t.Fatalf("Send after LTK request: %v (sktLoop wedged?)", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("LTK request handling took %v (synchronous cmdTimeout stall?)", elapsed)
	}
}

// TestLTKRequestReplyErrorLogged: a failing negative reply is logged from
// the reply goroutine, and sktLoop keeps serving commands.
func TestLTKRequestReplyErrorLogged(t *testing.T) {
	out := &syncWriter{}
	swapLogger(t, slog.New(slog.NewTextHandler(out, nil)))
	skt := newFakeSkt()
	respondWithStatus(skt, 0x0C) // every command fails with Disallowed
	h := newLoopedHCIOn(t, skt, func(h *HCI) {
		h.evth[0x3E] = h.handleLEMeta
		h.subh[evt.LELongTermKeyRequestSubCode] = h.handleLELongTermKeyRequest
	})

	skt.rd <- ltkRequestWirePkt(0x0041)
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(out.String(), "negative reply failed") {
		if time.Now().After(deadline) {
			t.Fatal("LTK reply failure was never logged")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := h.Send(&cmd.LESetScanEnable{}, nil); err != ErrDisallowed {
		t.Fatalf("Send = %v, want ErrDisallowed (sktLoop wedged?)", err)
	}
}

// ---------------------------------------------------------------------------
// Advertise methods must propagate SetAdvertisement failures
// ---------------------------------------------------------------------------

// TestAdvertiseSetAdvertisementErrorPropagates: every advertise method used
// to swallow a SetAdvertisement failure (`return nil`), reporting success
// without ever advertising. The controller error must surface.
func TestAdvertiseSetAdvertisementErrorPropagates(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x0C)
	h := newLoopedHCI(t, skt)
	for name, call := range advertiseCalls(h) {
		if err := call(); err != ErrDisallowed {
			t.Errorf("%s = %v, want ErrDisallowed (SetAdvertisement error swallowed?)", name, err)
		}
	}
}

// TestAdvertiseRoundTrip: with a healthy controller every advertise method
// succeeds and enables advertising.
func TestAdvertiseRoundTrip(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newLoopedHCI(t, skt)
	for name, call := range advertiseCalls(h) {
		if err := call(); err != nil {
			t.Errorf("%s = %v, want nil", name, err)
		}
	}
	// Name-overflow paths of AdvertiseAdv: a 26-char name no longer fits
	// the AD packet (flags + service UUID) and lands in the scan response
	// as a complete name; a 30-char name overflows even the empty scan
	// response and falls through the ShortName attempt too.
	for _, n := range []int{26, 30} {
		if err := h.AdvertiseAdv(stubAdv{name: strings.Repeat("n", n)}); err != nil {
			t.Errorf("AdvertiseAdv(%d-char name) = %v, want nil", n, err)
		}
	}
	pollWrittenOpcode(t, skt, (&cmd.LESetAdvertiseEnable{}).OpCode())
}

// ---------------------------------------------------------------------------
// Dial cancellation must be errors.Is-distinguishable
// ---------------------------------------------------------------------------

// TestDialCanceledByContext: a dial ended by its context reports
// context.Canceled, distinguishing a requested cancel from a failure.
func TestDialCanceledByContext(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newLoopedHCI(t, skt)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cln, err := h.Dial(ctx, ble.NewAddr("11:22:33:44:55:66"))
	if cln != nil {
		t.Fatalf("Dial returned a client (%v) for a canceled dial", cln)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial = %v, want errors.Is(..., context.Canceled)", err)
	}
	if !strings.Contains(err.Error(), "connection canceled") {
		t.Fatalf("Dial = %v, want the canceled-connection message preserved", err)
	}
}

// TestDialDialerTimeout: a dial ended by the dialer timeout reports
// context.DeadlineExceeded.
func TestDialDialerTimeout(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newLoopedHCI(t, skt)
	h.dialerTmo = 20 * time.Millisecond

	cln, err := h.Dial(context.Background(), ble.NewAddr("11:22:33:44:55:66"))
	if cln != nil {
		t.Fatalf("Dial returned a client (%v) for a timed-out dial", cln)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial = %v, want errors.Is(..., context.DeadlineExceeded)", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("Dial = %v, must not read as a context cancel", err)
	}
}

// TestDialCancelFailurePassesThrough: when the cancel command itself fails,
// the failure must pass through unwrapped — not be dressed up as a
// requested cancel.
func TestDialCancelFailurePassesThrough(t *testing.T) {
	skt := newFakeSkt()
	cancelOp := (&cmd.LECreateConnectionCancel{}).OpCode()
	skt.onWrite = func(b []byte) {
		if b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		st := byte(0x00)
		if op == cancelOp {
			st = 0x01 // Unknown HCI Command
		}
		skt.rd <- cmdCompletePkt(op, st)
	}
	h := newLoopedHCI(t, skt)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cln, err := h.Dial(ctx, ble.NewAddr("11:22:33:44:55:66"))
	if cln != nil {
		t.Fatalf("Dial returned a client (%v) for a failed cancel", cln)
	}
	if err == nil || !strings.Contains(err.Error(), "cancel connection failed") {
		t.Fatalf("Dial = %v, want the wrapped cancel failure", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("Dial = %v, a cancel failure must not read as a requested cancel", err)
	}
}
