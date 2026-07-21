package hci

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// disconnWirePkt builds a full HCI Disconnection Complete event packet for
// the given connection handle (status 0x00, reason 0x13).
func disconnWirePkt(handle uint16) []byte {
	return []byte{
		pktTypeEvent, evt.DisconnectionCompleteCode,
		4, // parameter length
		0x00,
		byte(handle), byte(handle >> 8),
		0x13,
	}
}

// aclWirePkt wraps an L2CAP pdu in a full HCI ACL data wire packet (with the
// leading packet-type byte, as read from the socket).
func aclWirePkt(handle uint16, p pdu) []byte {
	return append([]byte{pktTypeACLData}, mkACLPkt(handle, p)...)
}

// newTeardownHCI is newLoopedHCIOn with the pieces the teardown paths need:
// a buffer pool (newConn requires one) and the DisconnectionComplete handler.
func newTeardownHCI(t *testing.T, skt *fakeSkt) *HCI {
	t.Helper()
	return newLoopedHCIOn(t, skt, func(h *HCI) {
		h.pool = NewPool(32, 4)
		h.evth[evt.DisconnectionCompleteCode] = h.handleDisconnectionComplete
	})
}

// addConn registers a fresh master connection (with its recombine goroutine)
// under the given handle. The handle is also stamped into the conn's param
// (offset per evt.LEConnectionComplete; Role stays 0 == roleMaster) so paths
// that key on c.param.ConnectionHandle() — Close's force cleanup — find the
// registration.
func addConn(h *HCI, handle uint16) *Conn {
	param := make(evt.LEConnectionComplete, 19)
	binary.LittleEndian.PutUint16(param[2:], handle)
	c := newConn(h, param)
	h.muConns.Lock()
	h.conns[handle] = c
	h.muConns.Unlock()
	return c
}

// waitDisconnected fails the test if c.Disconnected() does not fire.
func waitDisconnected(t *testing.T, c *Conn) {
	t.Helper()
	select {
	case <-c.Disconnected():
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnected() never fired")
	}
}

// waitChInPDUClosed fails the test unless c.chInPDU is closed (draining any
// buffered PDUs first) — the observable sign that the recombine goroutine
// has wound down past its close(c.chInPDU). Returns the number of PDUs
// drained on the way.
func waitChInPDUClosed(t *testing.T, c *Conn) int {
	t.Helper()
	drained := 0
	for {
		select {
		case _, ok := <-c.chInPDU:
			if !ok {
				return drained
			}
			drained++
		case <-time.After(5 * time.Second):
			t.Fatal("chInPDU was not closed (recombine goroutine leaked?)")
		}
	}
}

// waitDisconnectCmd polls the fake socket until at least one HCI Disconnect
// command has been written, and returns how many there are.
func waitDisconnectCmd(t *testing.T, skt *fakeSkt) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n := countDisconnectCmds(skt); n > 0 {
			return n
		}
		if time.Now().After(deadline) {
			t.Fatal("no Disconnect command reached the socket")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func countDisconnectCmds(skt *fakeSkt) int {
	want := (&cmd.Disconnect{}).OpCode()
	n := 0
	for _, w := range skt.written() {
		if w[0] == pktTypeCommand && int(w[1])|int(w[2])<<8 == want {
			n++
		}
	}
	return n
}

func connCount(h *HCI) int {
	h.muConns.Lock()
	defer h.muConns.Unlock()
	return len(h.conns)
}

// ---------------------------------------------------------------------------
// Fix 1: socket death tears down every connection
// ---------------------------------------------------------------------------

// TestSocketDeathTearsDownConns: when the HCI socket dies, cleanupConns must
// close every conn's channels — Disconnected() fires, the recombine
// goroutines wind down (chInPDU closes), h.conns empties, and outstanding TX
// credits go back to the shared pool. Previously nothing iterated h.conns on
// socket death: chDone never closed, so a notify-only device went silent
// forever and reconnect logic never triggered.
func TestSocketDeathTearsDownConns(t *testing.T) {
	skt := newFakeSkt()
	h := newTeardownHCI(t, skt)
	c1 := addConn(h, 0x0040)
	c2 := addConn(h, 0x0041)
	c1.txBuffer.Get() // outstanding TX credit that only teardown can return

	skt.Close()
	select {
	case <-h.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("sktLoop did not exit on socket death")
	}
	for _, c := range []*Conn{c1, c2} {
		waitDisconnected(t, c)
		if n := waitChInPDUClosed(t, c); n != 0 {
			t.Fatalf("drained %d unexpected PDUs", n)
		}
	}
	if n := connCount(h); n != 0 {
		t.Fatalf("h.conns holds %d conns after socket death, want 0", n)
	}
	// The error was recorded before Disconnected() fired.
	if err := h.Error(); err != io.EOF {
		t.Fatalf("Error() = %v, want io.EOF", err)
	}
	// cleanupConns returned c1's credit to the pool.
	if n := len(h.pool.ch); n != 4 {
		t.Fatalf("pool holds %d buffers after teardown, want 4", n)
	}
}

// ---------------------------------------------------------------------------
// Fix 2: a full chInPkt must not park sktLoop
// ---------------------------------------------------------------------------

// TestHandleACLFullChannelDropsAndKills: a connection whose consumer is dead
// (full chInPkt) must not block sktLoop. The packet is dropped and counted,
// exactly one teardown (Disconnect command) is initiated, later events are
// still processed, and the eventual disconnect event completes the teardown.
func TestHandleACLFullChannelDropsAndKills(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00) // answers the Disconnect + later Sends
	h := newTeardownHCI(t, skt)

	// A conn with a dead consumer: no recombine goroutine and a
	// zero-capacity chInPkt, so the first inbound ACL packet already
	// finds it full.
	c := &Conn{
		hci:      h,
		param:    make(evt.LEConnectionComplete, 19),
		chInPkt:  make(chan packet),
		chInPDU:  make(chan pdu),
		chDone:   make(chan struct{}),
		txBuffer: newTxCredits(h.pool),
	}
	h.muConns.Lock()
	h.conns[0x0040] = c
	h.muConns.Unlock()

	skt.rd <- aclWirePkt(0x0040, mkpdu(1, cidLEAtt, []byte{0xAA}))
	skt.rd <- aclWirePkt(0x0040, mkpdu(1, cidLEAtt, []byte{0xBB}))

	waitDisconnectCmd(t, skt)

	// sktLoop is still live: a full command round-trip works. Its reply is
	// queued behind the ACL packets, so once Send returns both drops have
	// been counted.
	if err := h.Send(&cmd.LESetScanEnable{}, nil); err != nil {
		t.Fatalf("Send after ACL drops: %v (sktLoop parked?)", err)
	}
	if got := h.ACLDropped(); got != 2 {
		t.Fatalf("ACLDropped = %d, want 2", got)
	}
	if n := countDisconnectCmds(skt); n != 1 {
		t.Fatalf("wrote %d Disconnect commands, want 1 (kill must be once-per-conn)", n)
	}

	// The disconnect event finishes the teardown.
	skt.rd <- disconnWirePkt(0x0040)
	waitDisconnected(t, c)
	if n := connCount(h); n != 0 {
		t.Fatalf("h.conns holds %d conns after disconnect, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// Fix 3: recombine error tears the connection down and keeps draining
// ---------------------------------------------------------------------------

// TestRecombineErrorTearsDownConn: a malformed fragment stream must kill the
// connection (one Disconnect command) while chInPkt keeps draining, so
// nothing backs up before the disconnect event lands and sktLoop stays live
// throughout.
func TestRecombineErrorTearsDownConn(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newTeardownHCI(t, skt)
	c := addConn(h, 0x0040)

	// LE-ATT fragment claiming a 100-byte payload: larger than rxMPS (23).
	skt.rd <- aclWirePkt(0x0040, mkpdu(100, cidLEAtt, []byte{0xAA}))

	// recombine dies: Reads unblock (chInPDU closes) ...
	select {
	case _, ok := <-c.chInPDU:
		if ok {
			t.Fatal("received a PDU from a malformed stream")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("chInPDU was not closed after recombine error")
	}
	// ... and teardown is initiated.
	waitDisconnectCmd(t, skt)

	// The conn is still in h.conns until the disconnect event, and its
	// chInPkt (cap 16) must keep draining: 30 more packets, none may wedge
	// sktLoop or the connection.
	for i := 0; i < 30; i++ {
		skt.rd <- aclWirePkt(0x0040, mkpdu(1, cidLEAtt, []byte{byte(i)}))
	}
	if err := h.Send(&cmd.LESetScanEnable{}, nil); err != nil {
		t.Fatalf("Send after recombine teardown: %v (sktLoop parked?)", err)
	}
	if n := countDisconnectCmds(skt); n != 1 {
		t.Fatalf("wrote %d Disconnect commands, want 1", n)
	}

	skt.rd <- disconnWirePkt(0x0040)
	waitDisconnected(t, c)
	if n := connCount(h); n != 0 {
		t.Fatalf("h.conns holds %d conns after disconnect, want 0", n)
	}
}

// TestRecombineDeliveryUnblocksOnClose pins the chDone case of recombine's
// PDU delivery: with no reader ever draining chInPDU, closing chDone must
// unblock the delivery with an ErrClosed, instead of parking the recombine
// goroutine forever.
func TestRecombineDeliveryUnblocksOnClose(t *testing.T) {
	c := &Conn{
		chInPkt: make(chan packet, 1),
		chInPDU: make(chan pdu), // unbuffered, no reader: delivery must block
		chDone:  make(chan struct{}),
		rxMPS:   23,
	}
	c.chInPkt <- mkACLPkt(0x0040, mkpdu(1, cidLEAtt, []byte{0xAA}))
	errc := make(chan error, 1)
	go func() { errc <- c.recombine() }()
	close(c.chDone)
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("recombine = %v, want errors.Is(..., ErrClosed)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recombine did not unblock when chDone closed")
	}
}

// TestRecombineWedgedReaderTeardown drives the whole wedged-consumer exit of
// the recombine goroutine: chInPDU full, one more delivery in flight, then
// teardown. The goroutine must unblock via chDone, close chInPDU (buffered
// PDUs stay readable first), drain the already-closed chInPkt, and exit.
func TestRecombineWedgedReaderTeardown(t *testing.T) {
	h := &HCI{pool: NewPool(64, 2)}
	c := newConn(h, make(evt.LEConnectionComplete, 19))

	// 16 PDUs fill chInPDU; the 17th parks the goroutine in the delivery
	// select.
	for i := 0; i < 17; i++ {
		c.chInPkt <- mkACLPkt(0x0040, mkpdu(1, cidLEAtt, []byte{byte(i)}))
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(c.chInPDU) != 16 || len(c.chInPkt) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("recombine did not park: chInPDU %d, chInPkt %d", len(c.chInPDU), len(c.chInPkt))
		}
		time.Sleep(time.Millisecond)
	}

	c.closeChans()
	// Give the goroutine a beat to take the chDone case before we start
	// receiving (a receive would free a chInPDU slot and let the parked
	// delivery race the chDone case — both exits are valid, this just
	// makes the ErrClosed one near-certain).
	time.Sleep(100 * time.Millisecond)
	waitDisconnected(t, c)
	if got := waitChInPDUClosed(t, c); got < 16 || got > 17 {
		t.Fatalf("drained %d PDUs, want 16 (or 17 if delivery won the race)", got)
	}
}

// ---------------------------------------------------------------------------
// Fixes 1+3 interplay: racing teardown paths, double teardown
// ---------------------------------------------------------------------------

// TestTeardownRaces runs a disconnect event, whole-adapter cleanup, and a
// direct closeChans against each other repeatedly. Any interleaving must be
// panic-free (guarded closes) and leave the conn fully torn down. Run with
// -race.
func TestTeardownRaces(t *testing.T) {
	for i := 0; i < 25; i++ {
		h := &HCI{
			muConns: &sync.Mutex{},
			conns:   map[uint16]*Conn{},
			pool:    NewPool(32, 4),
		}
		c := addConn(h, 0x0040)
		c.txBuffer.Get() // let both credit-return paths have work

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			// Loser of the h.conns race returns the invalid-handle error.
			_ = h.handleDisconnectionComplete(disconnWirePkt(0x0040)[3:])
		}()
		go func() {
			defer wg.Done()
			h.cleanupConns()
		}()
		go func() {
			defer wg.Done()
			c.closeChans()
		}()
		wg.Wait()

		waitDisconnected(t, c)
		waitChInPDUClosed(t, c)
		if n := connCount(h); n != 0 {
			t.Fatalf("iteration %d: %d conns left registered", i, n)
		}
		if n := len(h.pool.ch); n != 4 {
			t.Fatalf("iteration %d: pool holds %d buffers, want 4", i, n)
		}
	}
}

// TestDoubleTeardownPanicFree: every teardown entry point must tolerate
// running after the teardown already happened.
func TestDoubleTeardownPanicFree(t *testing.T) {
	h := &HCI{
		muConns: &sync.Mutex{},
		conns:   map[uint16]*Conn{},
		pool:    NewPool(32, 4),
	}
	c := addConn(h, 0x0040)

	if err := h.handleDisconnectionComplete(disconnWirePkt(0x0040)[3:]); err != nil {
		t.Fatalf("handleDisconnectionComplete = %v, want nil", err)
	}
	// A second disconnect event for the same handle: the conn is gone from
	// h.conns, so it errors — and must not double-close anything.
	if err := h.handleDisconnectionComplete(disconnWirePkt(0x0040)[3:]); err == nil {
		t.Fatal("repeat disconnect event returned nil, want invalid-handle error")
	}
	h.cleanupConns()
	h.cleanupConns()
	c.closeChans() // direct repeat: the Once absorbs it

	// kill after teardown: initiates once (Close no-ops on the closed
	// chDone), and never again.
	if !c.kill() {
		t.Fatal("first kill did not report initiating teardown")
	}
	if c.kill() {
		t.Fatal("second kill claimed to initiate teardown again")
	}
	waitDisconnected(t, c)
	waitChInPDUClosed(t, c)
}

// TestRecombineDisconnectMidReassembly: teardown closing chInPkt while a
// segmented PDU is mid-reassembly is a clean shutdown (io.EOF), not stream
// corruption — the wrapper must not log "recombine failed" or re-kill for
// an ordinary disconnect that happened to land between fragments.
func TestRecombineDisconnectMidReassembly(t *testing.T) {
	c := &Conn{
		chInPkt: make(chan packet, 2),
		chInPDU: make(chan pdu, 1),
		chDone:  make(chan struct{}),
		rxMPS:   23,
	}
	// First fragment of a segmented SDU: claims 10 payload bytes, carries 1.
	c.chInPkt <- mkACLPkt(0x0040, mkpdu(10, cidLEAtt, []byte{0xAA}))
	close(c.chInPkt)
	if err := c.recombine(); err != io.EOF {
		t.Fatalf("recombine mid-reassembly close = %v, want io.EOF", err)
	}
}

// TestDialAcceptSurfaceHCIDeath: the <-h.done arms in Dial and Accept must
// return the recorded fatal transport error via Error(), not nil.
func TestDialAcceptSurfaceHCIDeath(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newTeardownHCI(t, skt)

	errc := make(chan error, 1)
	go func() {
		_, err := h.Dial(context.Background(), ble.NewAddr("11:22:33:44:55:66"))
		errc <- err
	}()
	// Let the dial write LECreateConnection and park on the select, then
	// kill the socket: sktLoop exits and h.done closes.
	for i := 0; len(skt.written()) == 0 && i < 400; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	skt.rd <- nil

	select {
	case err := <-errc:
		if err == nil {
			t.Error("Dial after HCI death returned a nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dial did not return after HCI death")
	}
	if _, err := h.Accept(); err == nil {
		t.Error("Accept after HCI death returned a nil error")
	}
}

// TestRecombineDispatchesSignal covers the recombine → handleSignal route:
// an unknown signaling code must produce a Command Reject on the wire.
func TestRecombineDispatchesSignal(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newTeardownHCI(t, skt)
	addConn(h, 0x0040)

	// Signaling PDU: code 0xFF (unknown), id 1, length 0.
	skt.rd <- aclWirePkt(0x0040, mkpdu(4, cidLESignal, []byte{0xFF, 0x01, 0x00, 0x00}))

	for i := 0; i < 400; i++ {
		for _, w := range skt.written() {
			if len(w) > 0 && w[0] == pktTypeACLData {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no ACL response to an unknown signaling code")
}

// ---------------------------------------------------------------------------
// Fix: a lost Disconnect command must not leave a zombie conn
// ---------------------------------------------------------------------------

// TestCloseForceCleansUpLostDisconnect: the controller never answers the
// Disconnect command (the truly-lost variant of the abandoned-command
// family), so no DisconnectionComplete will ever arrive. Close must retry
// once and then force-deregister the conn itself: Disconnected() fires, the
// recombine goroutine winds down, h.conns empties, and the conn's TX
// credits return to the shared pool. Before the fix Close swallowed the
// send error and the conn stayed registered forever, with killOnce already
// spent.
func TestCloseForceCleansUpLostDisconnect(t *testing.T) {
	quietLogger(t)
	oldCmd, oldDisc := cmdTimeout, disconnectTimeout
	cmdTimeout, disconnectTimeout = 50*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { cmdTimeout, disconnectTimeout = oldCmd, oldDisc })

	skt := newFakeSkt()
	h := newTeardownHCI(t, skt)
	// Two command credits so the retry reaches the wire instead of
	// escalating through send's credit-starvation close.
	h.setAllowedCommands(2)
	c := addConn(h, 0x0040)
	c.txBuffer.Get() // in-flight TX credit only teardown can return

	if err := c.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	waitDisconnected(t, c)
	if n := waitChInPDUClosed(t, c); n != 0 {
		t.Fatalf("drained %d unexpected PDUs", n)
	}
	if n := connCount(h); n != 0 {
		t.Fatalf("h.conns holds %d conns after force cleanup, want 0", n)
	}
	if n := len(h.pool.ch); n != 4 {
		t.Fatalf("pool holds %d buffers after force cleanup, want 4", n)
	}
	if n := countDisconnectCmds(skt); n != 2 {
		t.Fatalf("%d Disconnect commands reached the wire, want 2 (original + retry)", n)
	}
}

// TestCloseLateEventBeatsForceCleanup: both command attempts time out, but
// the DisconnectionComplete event straggles in during Close's bounded wait
// (the abandoned command executed late). The event path must win — normal
// teardown, no force-cleanup double work, and Close still returns nil.
func TestCloseLateEventBeatsForceCleanup(t *testing.T) {
	quietLogger(t)
	oldCmd, oldDisc := cmdTimeout, disconnectTimeout
	cmdTimeout, disconnectTimeout = 50*time.Millisecond, 5*time.Second
	t.Cleanup(func() { cmdTimeout, disconnectTimeout = oldCmd, oldDisc })

	skt := newFakeSkt()
	h := newTeardownHCI(t, skt)
	h.setAllowedCommands(2)
	c := addConn(h, 0x0040)

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()

	// Wait for both futile attempts, then deliver the straggling event.
	deadline := time.Now().Add(2 * time.Second)
	for countDisconnectCmds(skt) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("Close did not retry the Disconnect command")
		}
		time.Sleep(5 * time.Millisecond)
	}
	skt.rd <- disconnWirePkt(0x0040)

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the late event")
	}
	waitDisconnected(t, c)
	if n := connCount(h); n != 0 {
		t.Fatalf("h.conns holds %d conns, want 0", n)
	}
}

// TestCleanupConnSkipsStaleConn: cleanupConn keys on conn identity, not just
// handle — a stale *Conn whose handle was since reused by a newer connection
// must not tear the newer one down.
func TestCleanupConnSkipsStaleConn(t *testing.T) {
	skt := newFakeSkt()
	h := newTeardownHCI(t, skt)
	old := addConn(h, 0x0040)
	h.muConns.Lock()
	delete(h.conns, 0x0040) // old was torn down elsewhere...
	h.muConns.Unlock()
	fresh := addConn(h, 0x0040) // ...and the controller reused its handle
	t.Cleanup(func() { old.closeChans() })

	h.cleanupConn(old) // stale identity: must be a no-op

	if n := connCount(h); n != 1 {
		t.Fatalf("h.conns holds %d conns after stale cleanupConn, want 1", n)
	}
	select {
	case <-fresh.Disconnected():
		t.Fatal("cleanupConn tore down the newer conn under a reused handle")
	default:
	}
}
