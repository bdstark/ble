package hci

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
)

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

// swapLogger installs l as ble.Logger for the duration of the test. Tests
// using it must not run in parallel (package-level state).
func swapLogger(t *testing.T, l *slog.Logger) {
	t.Helper()
	old := ble.Logger
	ble.Logger = l
	t.Cleanup(func() { ble.Logger = old })
}

// quietLogger discards log output at the default (info) level.
func quietLogger(t *testing.T) {
	t.Helper()
	swapLogger(t, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// debugLogger discards output but reports debug as enabled, so the
// logDebugEnabled-gated paths run.
func debugLogger(t *testing.T) {
	t.Helper()
	swapLogger(t, slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

// fakeSkt is an in-memory stand-in for the HCI socket. Reads deliver packets
// queued on rd in order; a nil entry yields io.EOF. Writes are recorded and
// optionally trigger onWrite (called synchronously from the writer's
// goroutine).
type fakeSkt struct {
	mu      sync.Mutex
	wrote   [][]byte
	rd      chan []byte
	closed  chan struct{}
	once    sync.Once
	onWrite func([]byte)
}

func newFakeSkt() *fakeSkt {
	return &fakeSkt{
		rd:     make(chan []byte, 32),
		closed: make(chan struct{}),
	}
}

func (s *fakeSkt) Read(p []byte) (int, error) {
	select {
	case b := <-s.rd:
		if b == nil {
			return 0, io.EOF
		}
		return copy(p, b), nil
	case <-s.closed:
		return 0, io.EOF
	}
}

func (s *fakeSkt) Write(p []byte) (int, error) {
	b := append([]byte(nil), p...)
	s.mu.Lock()
	s.wrote = append(s.wrote, b)
	s.mu.Unlock()
	if s.onWrite != nil {
		s.onWrite(b)
	}
	return len(p), nil
}

func (s *fakeSkt) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func (s *fakeSkt) written() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.wrote))
	copy(out, s.wrote)
	return out
}

// cmdCompletePkt builds a full HCI CommandComplete event packet for opcode
// with a single status byte as return parameters.
func cmdCompletePkt(opcode int, status byte) []byte {
	return []byte{
		pktTypeEvent, evt.CommandCompleteCode,
		4,                               // parameter length
		1,                               // NumHCICommandPackets
		byte(opcode), byte(opcode >> 8), // CommandOpcode
		status, // ReturnParameters
	}
}

// newLoopedHCI returns an HCI wired to skt with its sktLoop running and a
// responder-ready command path (CommandComplete handler + one command buffer).
func newLoopedHCI(t *testing.T, skt *fakeSkt) *HCI {
	t.Helper()
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}
	h.skt = skt
	h.evth[evt.CommandCompleteCode] = h.handleCommandComplete
	h.setAllowedCommands(1)
	go h.sktLoop()
	t.Cleanup(func() {
		h.Close()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("sktLoop did not exit")
		}
	})
	return h
}

// respondWithStatus makes the fake socket answer every written HCI command
// with a CommandComplete carrying the given status.
func respondWithStatus(skt *fakeSkt, status byte) {
	skt.onWrite = func(b []byte) {
		if b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		skt.rd <- cmdCompletePkt(op, status)
	}
}

// advReportPkt builds a single-report LE Advertising Report payload (as
// handed to handleLEAdvertisingReport) for the given event type, low address
// byte, and AD payload.
func advReportPkt(evtTyp byte, addrLo byte, data []byte) []byte {
	b := []byte{
		0x02,                                 // subevent: LE Advertising Report
		0x01,                                 // one report
		evtTyp,                               // event type
		0x00,                                 // public address
		addrLo, 0x02, 0x03, 0x04, 0x05, 0x06, // address
		byte(len(data)),
	}
	b = append(b, data...)
	b = append(b, 0xC8) // RSSI
	return b
}

// ---------------------------------------------------------------------------
// NewHCI
// ---------------------------------------------------------------------------

func TestNewHCIDefaults(t *testing.T) {
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}
	if h.chAdv == nil || cap(h.chAdv) == 0 {
		t.Fatalf("chAdv not initialized with capacity, cap = %d", cap(h.chAdv))
	}
	if h.AdvDropped() != 0 {
		t.Fatalf("AdvDropped = %d on a fresh HCI, want 0", h.AdvDropped())
	}
}

func TestNewHCIOptionError(t *testing.T) {
	boom := errors.New("boom")
	_, err := NewHCI(func(ble.DeviceOption) error { return boom })
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("NewHCI with failing option: err = %v, want wrapped %v", err, boom)
	}
}

// ---------------------------------------------------------------------------
// send: bounded command-buffer acquisition
// ---------------------------------------------------------------------------

// TestSendClosed pins the bounded chCmdBufs wait: with no command buffer
// available and the HCI already shut down, send must fail with ErrClosed
// instead of parking the caller forever.
func TestSendClosed(t *testing.T) {
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}
	close(h.done) // shut down with h.err nil, chCmdBufs empty
	if err := h.Send(&cmd.LESetScanEnable{}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send on closed HCI: err = %v, want errors.Is(..., ErrClosed)", err)
	}
}

// TestSendRoundTrip drives a full command round-trip through sktLoop with a
// fake socket: buffer acquisition, socket write, pooled event handling, and
// the return-params copy in handleCommandComplete. Junk packets queued first
// exercise the vendor-packet and failed-packet log paths of sktLoop.
func TestSendRoundTrip(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	// Processed (and logged) before the command reply below.
	skt.rd <- []byte{pktTypeVendor, 0x01, 0x02} // "unsupported vendor packet" branch
	skt.rd <- []byte{pktTypeCommand, 0xAA}      // "failed to handle packet" branch
	respondWithStatus(skt, 0x00)
	h := newLoopedHCI(t, skt)

	rp := cmd.LESetScanEnableRP{}
	if err := h.Send(&cmd.LESetScanEnable{LEScanEnable: 1}, &rp); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if rp.Status != 0x00 {
		t.Fatalf("Status = %#x, want 0", rp.Status)
	}

	w := skt.written()
	if len(w) != 1 {
		t.Fatalf("wrote %d packets, want 1", len(w))
	}
	want := (&cmd.LESetScanEnable{}).OpCode()
	if got := int(w[0][1]) | int(w[0][2])<<8; w[0][0] != pktTypeCommand || got != want {
		t.Fatalf("command packet = % X, want opcode %04X", w[0], want)
	}
}

// TestSendRoundTripErrorStatus verifies a non-zero status byte surfaces as
// the matching ErrCommand.
func TestSendRoundTripErrorStatus(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x0C)
	h := newLoopedHCI(t, skt)
	if err := h.Send(&cmd.LESetScanEnable{}, nil); err != ErrDisallowed {
		t.Fatalf("Send: err = %v, want ErrDisallowed", err)
	}
}

// TestSktLoopEOF: closing the socket ends sktLoop with io.EOF preserved
// unwrapped (callers depend on detecting it).
func TestSktLoopEOF(t *testing.T) {
	skt := newFakeSkt()
	h := newLoopedHCI(t, skt)
	skt.Close()
	select {
	case <-h.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("sktLoop did not exit on EOF")
	}
	if h.Error() != io.EOF {
		t.Fatalf("Error() = %v, want io.EOF", h.Error())
	}
}

// ---------------------------------------------------------------------------
// handleACL
// ---------------------------------------------------------------------------

func TestHandleACLUnknownHandle(t *testing.T) {
	quietLogger(t)
	h := &HCI{conns: map[uint16]*Conn{}}
	// ACL data for handle 0x0040, which has no connection: warn and drop.
	if err := h.handleACL([]byte{0x40, 0x00, 0x01, 0x00, 0xAA}); err != nil {
		t.Fatalf("handleACL = %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// handleLEAdvertisingReport / adv dispatcher
// ---------------------------------------------------------------------------

func newAdvHCI(chCap int) *HCI {
	h := &HCI{
		adHist: make([]*Advertisement, 8),
		chAdv:  make(chan advDelivery, chCap),
	}
	h.advTgt.Store(&advTarget{fn: func(ble.Advertisement) {}})
	return h
}

// TestAdvReportScanRspPairing: an ADV_IND followed by a SCAN_RSP from the
// same address must produce a fresh paired Advertisement, leaving the stored
// history entry unmutated.
func TestAdvReportScanRspPairing(t *testing.T) {
	h := newAdvHCI(8)
	adData := []byte{0x02, 0x01, 0x06}
	srData := []byte{0x06, 0x09, 'w', 'o', 'r', 'l', 'd'}

	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvInd, 0x01, adData)); err != nil {
		t.Fatalf("AD report: %v", err)
	}
	ad := (<-h.chAdv).a
	if ad.ScanResponse() != nil {
		t.Fatal("AD-only advertisement has a scan response")
	}

	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypScanRsp, 0x01, srData)); err != nil {
		t.Fatalf("SR report: %v", err)
	}
	paired := (<-h.chAdv).a
	if paired == h.adHist[0] {
		t.Fatal("scan response mutated the stored history entry instead of a fresh Advertisement")
	}
	if got := paired.ScanResponse(); string(got) != string(srData) {
		t.Fatalf("ScanResponse = % X, want % X", got, srData)
	}
	if h.adHist[0].sr != nil {
		t.Fatal("stored history entry gained a scan response (must stay immutable)")
	}
}

// TestAdvReportScanRspNoMatch: a SCAN_RSP with no prior AD is an error.
func TestAdvReportScanRspNoMatch(t *testing.T) {
	h := newAdvHCI(8)
	err := h.handleLEAdvertisingReport(advReportPkt(evtTypScanRsp, 0x01, nil))
	if err == nil {
		t.Fatal("orphan SCAN_RSP: err = nil, want error")
	}
}

// TestAdvReportDropCount: when chAdv is full the report is dropped, counted,
// and visible via AdvDropped.
func TestAdvReportDropCount(t *testing.T) {
	h := newAdvHCI(1)
	for i := 0; i < 3; i++ {
		if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvNonconnInd, byte(i+1), nil)); err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
	}
	if got := h.AdvDropped(); got != 2 {
		t.Fatalf("AdvDropped = %d, want 2", got)
	}
	if len(h.chAdv) != 1 {
		t.Fatalf("chAdv holds %d reports, want 1", len(h.chAdv))
	}
}

// TestAdvReportNilHandler: without a handler the report is ignored.
func TestAdvReportNilHandler(t *testing.T) {
	h := newAdvHCI(4)
	h.advTgt.Store(nil)
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvInd, 0x01, nil)); err != nil {
		t.Fatalf("handleLEAdvertisingReport = %v, want nil", err)
	}
}

// TestAdvReportMalformed: a report whose declared lengths don't account for
// the payload is rejected before any accessor runs — the parse is on
// sktLoop, where a panic would take down the adapter.
func TestAdvReportMalformed(t *testing.T) {
	h := newAdvHCI(4)
	cases := map[string][]byte{
		"empty":            {},
		"header only":      {0x02, 0x01},
		"zero reports":     {0x02, 0x00},
		"data len too big": {0x02, 0x01, evtTypAdvInd, 0x00, 1, 2, 3, 4, 5, 6, 0xFF, 0xAA},
		"missing rssi":     {0x02, 0x01, evtTypAdvInd, 0x00, 1, 2, 3, 4, 5, 6, 0x01, 0xAA},
	}
	for name, b := range cases {
		if err := h.handleLEAdvertisingReport(b); err == nil {
			t.Errorf("%s: handleLEAdvertisingReport = nil, want a malformed-report error", name)
		}
		if len(h.chAdv) != 0 {
			t.Fatalf("%s: a malformed report was queued", name)
		}
	}
}

// TestHandleLEMetaEmptyPayload: an LE Meta event with no payload has no
// subevent code; handleLEMeta must reject it, not index b[0] and panic
// sktLoop.
func TestHandleLEMetaEmptyPayload(t *testing.T) {
	h := &HCI{}
	if err := h.handleLEMeta(nil); err == nil {
		t.Fatal("handleLEMeta(nil) = nil, want an empty-payload error")
	}
}

// TestAdvDispatcher: the dispatcher delivers queued advertisements to the
// handler in order and exits when done closes.
func TestAdvDispatcher(t *testing.T) {
	got := make(chan ble.Advertisement, 4)
	h := &HCI{
		chAdv: make(chan advDelivery, 4),
		done:  make(chan bool),
	}
	tgt := &advTarget{fn: func(a ble.Advertisement) { got <- a }}
	h.advTgt.Store(tgt)

	exited := make(chan struct{})
	go func() {
		h.advDispatcher()
		close(exited)
	}()

	want := &Advertisement{}
	h.chAdv <- advDelivery{a: want, t: tgt}
	select {
	case a := <-got:
		if a != want {
			t.Fatalf("handler got %p, want %p", a, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}

	close(h.done)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("advDispatcher did not exit after done closed")
	}
}

// TestAdvQueuedReportsDroppedOnHandlerChange: reports queued while one
// handler was registered must never be delivered to the handler that
// replaced it — a queue drained after a new Scan started would otherwise
// feed the new scan's handler another scan's advertisements.
func TestAdvQueuedReportsDroppedOnHandlerChange(t *testing.T) {
	h := &HCI{
		chAdv: make(chan advDelivery, 4),
		done:  make(chan bool),
	}
	oldGot := make(chan ble.Advertisement, 4)
	oldTgt := &advTarget{fn: func(a ble.Advertisement) { oldGot <- a }}
	h.advTgt.Store(oldTgt)
	stale := &Advertisement{}
	h.chAdv <- advDelivery{a: stale, t: oldTgt}

	// A new scan registers its handler before the dispatcher drains the
	// queue.
	newGot := make(chan ble.Advertisement, 4)
	if err := h.SetAdvHandler(func(a ble.Advertisement) { newGot <- a }); err != nil {
		t.Fatalf("SetAdvHandler: %v", err)
	}

	exited := make(chan struct{})
	go func() {
		h.advDispatcher()
		close(exited)
	}()

	// The dispatcher processes the stale entry first (FIFO), then this one:
	// once the fresh report arrives, the stale entry's fate is decided.
	fresh := &Advertisement{}
	h.chAdv <- advDelivery{a: fresh, t: h.advTgt.Load()}
	select {
	case a := <-newGot:
		if a != fresh {
			t.Fatalf("new handler got %p, want only the fresh report %p", a, fresh)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fresh report was not delivered")
	}
	select {
	case a := <-oldGot:
		t.Fatalf("replaced handler received %p after a new handler was set", a)
	default:
	}

	close(h.done)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("advDispatcher did not exit after done closed")
	}
}

// TestSetAdvHandlerNilStopsDelivery: a nil handler unregisters delivery —
// subsequent reports are ignored without touching the queue.
func TestSetAdvHandlerNilStopsDelivery(t *testing.T) {
	h := newAdvHCI(4)
	if err := h.SetAdvHandler(nil); err != nil {
		t.Fatalf("SetAdvHandler(nil): %v", err)
	}
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvNonconnInd, 0x01, nil)); err != nil {
		t.Fatalf("handleLEAdvertisingReport = %v, want nil", err)
	}
	if n := len(h.chAdv); n != 0 {
		t.Fatalf("report queued with no handler registered: %d entries", n)
	}
}

// TestSetAdvHandlerConcurrentWithReports: SetAdvHandler races sktLoop's
// report handling and the dispatcher by design (a Scan can start while the
// controller still streams the previous scan's reports). Run them together
// so the race detector proves the handler hand-off is synchronized.
func TestSetAdvHandlerConcurrentWithReports(t *testing.T) {
	h := &HCI{
		adHist: make([]*Advertisement, 8),
		chAdv:  make(chan advDelivery, 64),
		done:   make(chan bool),
	}
	h.advTgt.Store(&advTarget{fn: func(ble.Advertisement) {}})
	go h.advDispatcher()
	defer close(h.done)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = h.SetAdvHandler(func(ble.Advertisement) {})
			}
		}
	}()
	for i := 0; i < 200; i++ {
		if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvNonconnInd, byte(i%250+1), nil)); err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
}
