package hci

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newLoopedHCIOn is newLoopedHCI for an arbitrary socket implementation.
// setup (optional) runs before sktLoop starts, so extra event handlers can
// be registered without racing the dispatcher.
func newLoopedHCIOn(t *testing.T, skt io.ReadWriteCloser, setup func(*HCI)) *HCI {
	t.Helper()
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}
	h.skt = skt
	h.evth[evt.CommandCompleteCode] = h.handleCommandComplete
	if setup != nil {
		setup(h)
	}
	h.setAllowedCommands(1)
	go h.sktLoop()
	t.Cleanup(func() {
		h.Close()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("sktLoop did not exit")
		}
		// Join the conn-disposal goroutines: one still inside Close/Send
		// would race the next test's writes to the tunable timeout vars.
		// They unwind promptly once h is closed (send fails fast on the
		// recorded error; cleanupConns closed every conn's chDone).
		h.wgConnDisposal.Wait()
	})
	return h
}

// marshalErrCmd is a Command whose Marshal always fails.
type marshalErrCmd struct{ err error }

func (c marshalErrCmd) OpCode() int          { return 0x2042 }
func (c marshalErrCmd) Len() int             { return 1 }
func (c marshalErrCmd) Marshal([]byte) error { return c.err }

// wErrSkt wraps fakeSkt, failing or short-changing every write.
type wErrSkt struct {
	*fakeSkt
	werr  error // returned from Write when non-nil
	short bool  // report one byte fewer than requested
}

func (s *wErrSkt) Write(p []byte) (int, error) {
	if s.werr != nil {
		return 0, s.werr
	}
	if s.short {
		return len(p) - 1, nil
	}
	return s.fakeSkt.Write(p)
}

// rErrSkt fails every read with a fixed non-EOF error.
type rErrSkt struct{ err error }

func (s *rErrSkt) Read([]byte) (int, error)    { return 0, s.err }
func (s *rErrSkt) Write(p []byte) (int, error) { return len(p), nil }
func (s *rErrSkt) Close() error                { return nil }

// cmdStatusPkt builds a full HCI CommandStatus event packet for opcode.
func cmdStatusPkt(opcode int, status byte) []byte {
	return []byte{
		pktTypeEvent, evt.CommandStatusCode,
		4, // parameter length
		status,
		1, // NumHCICommandPackets
		byte(opcode), byte(opcode >> 8),
	}
}

// leConnCompletePkt builds an LE Connection Complete payload (as handed to
// handleLEConnectionComplete, subevent code first).
func leConnCompletePkt(status byte, handle uint16, role byte) []byte {
	b := make([]byte, 19)
	b[0] = evt.LEConnectionCompleteSubCode
	b[1] = status
	binary.LittleEndian.PutUint16(b[2:], handle)
	b[4] = role
	return b
}

// newConnHCI returns a minimal HCI able to run handleLEConnectionComplete
// directly, with buffered connection channels so deliveries don't block.
func newConnHCI() *HCI {
	return &HCI{
		conns:        map[uint16]*Conn{},
		chMasterConn: make(chan *Conn, 1),
		chSlaveConn:  make(chan *Conn, 1),
		pool:         NewPool(32, 2),
	}
}

// ---------------------------------------------------------------------------
// Fix 1: completion for a timed-out command must not wedge sktLoop
// ---------------------------------------------------------------------------

// TestStaleCompletionDoesNotWedgeSktLoop simulates a command whose send()
// timed out with its entry still in h.sent (the race window): a completion
// event — and a duplicate — must be absorbed without blocking sktLoop, which
// must then still round-trip a fresh Send.
func TestStaleCompletionDoesNotWedgeSktLoop(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	h := newLoopedHCI(t, skt)

	op := (&cmd.LESetScanEnable{}).OpCode()
	stale := &pkt{&cmd.LESetScanEnable{}, make(chan []byte, 1)}
	h.muSent.Lock()
	h.sent[op] = stale
	h.muSent.Unlock()

	skt.rd <- cmdCompletePkt(op, 0x00) // parks in stale.done's buffer
	skt.rd <- cmdCompletePkt(op, 0x00) // duplicate: dropped, must not block

	respondWithStatus(skt, 0x00)
	done := make(chan error, 1)
	go func() { done <- h.Send(&cmd.LECreateConnectionCancel{}, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Send after stale completions: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sktLoop wedged by a completion event for a timed-out command")
	}
	if len(stale.done) != 1 {
		t.Fatalf("stale done buffer holds %d replies, want 1", len(stale.done))
	}
}

// TestHandleCommandCompleteDropsWhenNoReceiver drives the handler directly:
// the first completion fills the buffered done, the second must take the
// non-blocking default instead of blocking the caller (sktLoop).
func TestHandleCommandCompleteDropsWhenNoReceiver(t *testing.T) {
	h := &HCI{muSent: &sync.Mutex{}, sent: map[int]*pkt{}, chCmdBufs: make(chan []byte, 16)}
	p := &pkt{done: make(chan []byte, 1)}
	h.sent[0x180C] = p

	payload := cmdCompletePkt(0x180C, 0x07)[3:]
	if err := h.handleCommandComplete(payload); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := h.handleCommandComplete(payload); err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	got := <-p.done
	if len(got) != 1 || got[0] != 0x07 {
		t.Fatalf("reply = % X, want 07", got)
	}
	if len(p.done) != 0 {
		t.Fatalf("done buffer holds %d extra replies, want 0", len(p.done))
	}
}

// TestHandleCommandStatusDropsWhenNoReceiver is the CommandStatus twin.
func TestHandleCommandStatusDropsWhenNoReceiver(t *testing.T) {
	h := &HCI{muSent: &sync.Mutex{}, sent: map[int]*pkt{}, chCmdBufs: make(chan []byte, 16)}
	p := &pkt{done: make(chan []byte, 1)}
	h.sent[0x200D] = p

	payload := cmdStatusPkt(0x200D, 0x08)[3:]
	if err := h.handleCommandStatus(payload); err != nil {
		t.Fatalf("first status: %v", err)
	}
	if err := h.handleCommandStatus(payload); err != nil {
		t.Fatalf("duplicate status: %v", err)
	}
	got := <-p.done
	if len(got) != 1 || got[0] != 0x08 {
		t.Fatalf("reply = % X, want 08", got)
	}
}

// ---------------------------------------------------------------------------
// Fix 2: failed LE connection must not register a Conn
// ---------------------------------------------------------------------------

// TestLEConnCompleteFailedMasterNoLeak: a master connect failing with 0x3E
// (connection failed to be established) must not create a Conn, register a
// handle, or deliver anything to chMasterConn.
func TestLEConnCompleteFailedMasterNoLeak(t *testing.T) {
	quietLogger(t)
	h := newConnHCI()
	if err := h.handleLEConnectionComplete(leConnCompletePkt(0x3E, 0x0040, roleMaster)); err != nil {
		t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
	}
	if n := len(h.conns); n != 0 {
		t.Fatalf("failed connect left %d conns registered, want 0", n)
	}
	select {
	case c := <-h.chMasterConn:
		t.Fatalf("failed connect delivered conn %v", c)
	default:
	}
}

// TestLEConnCompleteCanceledMaster: a successful cancel (ErrConnID) is
// silently ignored, registering nothing.
func TestLEConnCompleteCanceledMaster(t *testing.T) {
	h := newConnHCI()
	if err := h.handleLEConnectionComplete(leConnCompletePkt(byte(ErrConnID), 0x0040, roleMaster)); err != nil {
		t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
	}
	if n := len(h.conns); n != 0 {
		t.Fatalf("canceled connect left %d conns registered, want 0", n)
	}
}

// TestLEConnCompleteMasterSuccess: a successful master connect registers the
// conn and delivers it to the dialer.
func TestLEConnCompleteMasterSuccess(t *testing.T) {
	h := newConnHCI()
	if err := h.handleLEConnectionComplete(leConnCompletePkt(0x00, 0x0041, roleMaster)); err != nil {
		t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
	}
	c, ok := h.conns[0x0041]
	if !ok {
		t.Fatal("successful connect did not register the conn")
	}
	select {
	case got := <-h.chMasterConn:
		if got != c {
			t.Fatalf("delivered %p, want the registered conn %p", got, c)
		}
	default:
		t.Fatal("successful connect did not deliver the conn to chMasterConn")
	}
	close(c.chInPkt) // wind down the recombine goroutine
}

// TestLEConnCompleteMasterBusyCloses: with no dialer waiting, a successful
// master connect is closed (Disconnect command) instead of delivered.
func TestLEConnCompleteMasterBusyCloses(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newLoopedHCI(t, skt)
	h.pool = NewPool(32, 2)

	// chMasterConn from NewHCI is unbuffered and nobody is dialing: the
	// select must fall through to closing the connection.
	if err := h.handleLEConnectionComplete(leConnCompletePkt(0x00, 0x0042, roleMaster)); err != nil {
		t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
	}
	want := (&cmd.Disconnect{}).OpCode()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, w := range skt.written() {
			if w[0] == pktTypeCommand && int(w[1])|int(w[2])<<8 == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("undeliverable master conn was never disconnected")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestLEConnCompleteSlaveSuccess: the slave path still registers and
// delivers the conn.
func TestLEConnCompleteSlaveSuccess(t *testing.T) {
	h := newConnHCI()
	if err := h.handleLEConnectionComplete(leConnCompletePkt(0x00, 0x0043, roleSlave)); err != nil {
		t.Fatalf("handleLEConnectionComplete = %v, want nil", err)
	}
	c, ok := h.conns[0x0043]
	if !ok {
		t.Fatal("slave connect did not register the conn")
	}
	select {
	case got := <-h.chSlaveConn:
		if got != c {
			t.Fatalf("delivered %p, want the registered conn %p", got, c)
		}
	default:
		t.Fatal("slave connect did not deliver the conn to chSlaveConn")
	}
	close(c.chInPkt)
}

// ---------------------------------------------------------------------------
// Fix 3: send must return promptly after a fatal marshal/write failure
// ---------------------------------------------------------------------------

// TestSendMarshalErrorFailsFast: a marshal failure must surface immediately
// (not after cmdTimeout), and the root cause must survive the socket
// teardown triggered by close().
func TestSendMarshalErrorFailsFast(t *testing.T) {
	setCmdTimeout(t, time.Second)
	skt := newFakeSkt()
	h := newLoopedHCI(t, skt)

	boom := errors.New("boom")
	start := time.Now()
	_, err := h.send(marshalErrCmd{err: boom})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("send took %v after marshal failure, want a prompt return", elapsed)
	}
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "marshal") {
		t.Fatalf("send = %v, want wrapped marshal failure", err)
	}
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("marshal failure did not close the HCI")
	}
	// sktLoop's teardown EOF must not clobber the recorded root cause.
	if got := h.Error(); !errors.Is(got, boom) {
		t.Fatalf("Error() = %v, want the marshal failure preserved", got)
	}
}

// TestSendWriteErrorFailsFast: a socket write failure must surface
// immediately and remove the orphaned sent-table entry.
func TestSendWriteErrorFailsFast(t *testing.T) {
	setCmdTimeout(t, time.Second)
	boom := errors.New("write exploded")
	skt := &wErrSkt{fakeSkt: newFakeSkt(), werr: boom}
	h := newLoopedHCIOn(t, skt, nil)

	start := time.Now()
	_, err := h.send(&cmd.LECreateConnectionCancel{})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("send took %v after write failure, want a prompt return", elapsed)
	}
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "failed to send cmd") {
		t.Fatalf("send = %v, want wrapped write failure", err)
	}
	h.muSent.Lock()
	n := len(h.sent)
	h.muSent.Unlock()
	if n != 0 {
		t.Fatalf("sent table holds %d orphaned entries, want 0", n)
	}
}

// TestSendShortWriteFailsFast: a short write is fatal and prompt too.
func TestSendShortWriteFailsFast(t *testing.T) {
	setCmdTimeout(t, time.Second)
	skt := &wErrSkt{fakeSkt: newFakeSkt(), short: true}
	h := newLoopedHCIOn(t, skt, nil)

	start := time.Now()
	_, err := h.send(&cmd.LECreateConnectionCancel{})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("send took %v after short write, want a prompt return", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "whole cmd pkt") {
		t.Fatalf("send = %v, want the short-write failure", err)
	}
	h.muSent.Lock()
	n := len(h.sent)
	h.muSent.Unlock()
	if n != 0 {
		t.Fatalf("sent table holds %d orphaned entries, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// Fix 4: h.err hygiene
// ---------------------------------------------------------------------------

// TestSendAfterFatalError: once a fatal error is recorded, send fails fast
// with it.
func TestSendAfterFatalError(t *testing.T) {
	skt := newFakeSkt()
	h := newLoopedHCI(t, skt)
	boom := errors.New("boom")
	h.close(boom)
	<-h.done
	if err := h.Send(&cmd.LESetScanEnable{}, nil); !errors.Is(err, boom) {
		t.Fatalf("Send = %v, want the recorded fatal error", err)
	}
}

// TestSendBufferWaitUnblocksOnFatalClose: a send parked waiting for a
// command buffer must return the fatal error when the HCI closes under it.
func TestSendBufferWaitUnblocksOnFatalClose(t *testing.T) {
	setCmdTimeout(t, 3*time.Second)
	skt := newFakeSkt()
	h := newLoopedHCI(t, skt)
	<-h.chCmdBufs // starve the command buffer

	boom := errors.New("boom")
	errCh := make(chan error, 1)
	go func() {
		_, err := h.send(&cmd.LESetScanEnable{})
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond) // let send park on the buffer wait
	h.close(boom)
	select {
	case err := <-errCh:
		if !errors.Is(err, boom) {
			t.Fatalf("send = %v, want the fatal close error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("send did not unblock on fatal close")
	}
}

// TestSendSocketDiesAwaitingResponse: the command goes out but the socket
// dies before any completion; send must return the recorded error rather
// than sit out cmdTimeout.
func TestSendSocketDiesAwaitingResponse(t *testing.T) {
	setCmdTimeout(t, 3*time.Second)
	skt := newFakeSkt()
	skt.onWrite = func([]byte) { skt.Close() } // controller dies right after the write
	h := newLoopedHCIOn(t, skt, nil)

	_, err := h.send(&cmd.LECreateConnectionCancel{})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("send = %v, want io.EOF from the dead socket", err)
	}
}

// TestSktLoopReadErrorRecorded: a non-EOF socket read error is recorded
// wrapped, with the original error reachable via errors.Is.
func TestSktLoopReadErrorRecorded(t *testing.T) {
	boom := errors.New("bad read")
	h := newLoopedHCIOn(t, &rErrSkt{err: boom}, nil)
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("sktLoop did not exit on read error")
	}
	got := h.Error()
	if !errors.Is(got, boom) || !strings.Contains(got.Error(), "skt:") {
		t.Fatalf("Error() = %v, want wrapped skt read error", got)
	}
}

// TestHandleEvtHandlerErrorNotStored: a non-command event handler error is
// logged, not stored in h.err.
func TestHandleEvtHandlerErrorNotStored(t *testing.T) {
	quietLogger(t)
	calls := 0
	h := &HCI{evth: map[int]handlerFn{0x99: func([]byte) error { calls++; return errors.New("boom") }}}
	if err := h.handleEvt([]byte{0x99, 0x00}); err != nil {
		t.Fatalf("handleEvt = %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
	if got := h.Error(); got != nil {
		t.Fatalf("handler error stored in h.err: %v", got)
	}
}

// TestHandleEvtHandlerSuccessNotStored: a nil handler result must not touch
// h.err either (previously it could wipe a recorded fatal error).
func TestHandleEvtHandlerSuccessNotStored(t *testing.T) {
	boom := errors.New("boom")
	h := &HCI{evth: map[int]handlerFn{0x99: func([]byte) error { return nil }}}
	h.setErr(boom)
	if err := h.handleEvt([]byte{0x99, 0x00}); err != nil {
		t.Fatalf("handleEvt = %v, want nil", err)
	}
	if got := h.Error(); !errors.Is(got, boom) {
		t.Fatalf("Error() = %v, want the recorded fatal error untouched", got)
	}
}

// TestHandlerErrorDoesNotPoisonSend drives the whole loop: an unsupported
// LE Meta subevent makes its handler error, after which a Send must still
// succeed (the old code parked the handler error in h.err).
func TestHandlerErrorDoesNotPoisonSend(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	skt.rd <- []byte{pktTypeEvent, 0x3E, 1, 0x7F} // unsupported LE subevent
	respondWithStatus(skt, 0x00)
	h := newLoopedHCIOn(t, skt, func(h *HCI) {
		h.evth[0x3E] = h.handleLEMeta
	})

	if err := h.Send(&cmd.LECreateConnectionCancel{}, nil); err != nil {
		t.Fatalf("Send after handler error = %v, want nil (h.err poisoned?)", err)
	}
	if got := h.Error(); got != nil {
		t.Fatalf("Error() = %v after a non-fatal handler error, want nil", got)
	}
}

// TestInitReturnsRecordedError: init succeeds when every command exchange
// completes with full-length return parameters and a sane buffer geometry.
// (Now that init propagates per-command errors, the replies must actually
// satisfy each RP's Unmarshal — status-only replies no longer pass.)
func TestInitReturnsRecordedError(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondInitRPs(skt, 0x02)
	h := newLoopedHCI(t, skt)
	if err := h.init(); err != nil {
		t.Fatalf("init = %v, want nil", err)
	}
	if len(skt.written()) == 0 {
		t.Fatal("init wrote no commands")
	}
	if h.bufCnt != 2 || h.bufSize != 0x0202 {
		t.Fatalf("decoded buffer geometry = %d x %d, want 2 x 0x0202", h.bufCnt, h.bufSize)
	}
}

// TestHandleCommandCompleteUnknownOpcode pins the not-found branch: a
// completion for an opcode with no sent entry (e.g. one that arrives after
// the timeout path already removed it) must error, not touch anything.
func TestHandleCommandCompleteUnknownOpcode(t *testing.T) {
	h := &HCI{muSent: &sync.Mutex{}, sent: map[int]*pkt{}, chCmdBufs: make(chan []byte, 16)}
	if err := h.handleCommandComplete(cmdCompletePkt(0x180C, 0x07)[3:]); err == nil {
		t.Fatal("unknown-opcode completion returned nil, want error")
	}
}

// TestHandleCommandStatusUnknownOpcode is the CommandStatus twin.
func TestHandleCommandStatusUnknownOpcode(t *testing.T) {
	h := &HCI{muSent: &sync.Mutex{}, sent: map[int]*pkt{}, chCmdBufs: make(chan []byte, 16)}
	if err := h.handleCommandStatus(cmdStatusPkt(0x200D, 0x08)[3:]); err == nil {
		t.Fatal("unknown-opcode status returned nil, want error")
	}
}

// respondInitRPs answers every command written to skt with a success
// CommandComplete whose return parameters are a 0x00 status followed by 32
// fill bytes — long enough for every init RP's Unmarshal (short status-only
// replies only ever "worked" while init swallowed Unmarshal errors). With a
// nonzero fill the decoded buffer geometry is plausible; with 0x00 it is
// the all-zero geometry of a broken controller. Opcodes in silent get no
// reply at all.
func respondInitRPs(skt *fakeSkt, fill byte, silent ...int) {
	skt.onWrite = func(w []byte) {
		if len(w) < 3 || w[0] != pktTypeCommand {
			return
		}
		op := int(w[1]) | int(w[2])<<8
		for _, s := range silent {
			if op == s {
				return
			}
		}
		rp := make([]byte, 33)
		for i := 1; i < len(rp); i++ {
			rp[i] = fill
		}
		pkt := []byte{pktTypeEvent, evt.CommandCompleteCode, byte(3 + len(rp)), 1, byte(op), byte(op >> 8)}
		skt.rd <- append(pkt, rp...)
	}
}

// TestInitFailsOnCommandTimeout: a completion timeout on one init command
// (here ReadBufferSize, whose reply sizes the TX buffer pool) must fail
// init with a wrapped ErrCommandTimeout. Previously the error was
// swallowed, init returned nil with bufCnt still 0, and Init panicked
// constructing NewPool(sz, -1).
func TestInitFailsOnCommandTimeout(t *testing.T) {
	old := cmdTimeout
	cmdTimeout = 100 * time.Millisecond
	t.Cleanup(func() { cmdTimeout = old })

	skt := newFakeSkt()
	respondInitRPs(skt, 0x02, (&cmd.ReadBufferSize{}).OpCode())
	h := newLoopedHCIOn(t, skt, nil)

	err := h.init()
	if !errors.Is(err, ErrCommandTimeout) {
		t.Fatalf("init() = %v, want a wrapped ErrCommandTimeout", err)
	}
	if h.bufCnt != 0 {
		t.Fatalf("bufCnt = %d after failed init, want 0 (never trusted)", h.bufCnt)
	}
}

// TestInitRejectsUnusableBufferGeometry: a controller that answers all
// init commands but reports zero ACL buffers must fail init instead of
// letting Init build a poolless HCI.
func TestInitRejectsUnusableBufferGeometry(t *testing.T) {
	skt := newFakeSkt()
	respondInitRPs(skt, 0x00) // all-zero RPs: 0 buffers of 0 bytes
	h := newLoopedHCIOn(t, skt, nil)

	err := h.init()
	if err == nil || !strings.Contains(err.Error(), "buffer geometry") {
		t.Fatalf("init() = %v, want a buffer-geometry error", err)
	}
}

// TestInitFailsOnEachCommand: every init command's failure must fail init —
// none may fall through to zero-valued controller state.
func TestInitFailsOnEachCommand(t *testing.T) {
	old := cmdTimeout
	cmdTimeout = 50 * time.Millisecond
	t.Cleanup(func() { cmdTimeout = old })

	silenced := []Command{
		&cmd.Reset{}, &cmd.ReadBDADDR{}, &cmd.ReadBufferSize{},
		&cmd.LEReadBufferSize{}, &cmd.LEReadAdvertisingChannelTxPower{},
		&cmd.LESetEventMask{}, &cmd.SetEventMask{}, &cmd.WriteLEHostSupport{},
	}
	for _, sc := range silenced {
		skt := newFakeSkt()
		respondInitRPs(skt, 0x02, sc.OpCode())
		h := newLoopedHCIOn(t, skt, nil)
		h.setAllowedCommands(16)
		if err := h.init(); !errors.Is(err, ErrCommandTimeout) {
			t.Errorf("init with opcode 0x%04X silenced = %v, want wrapped ErrCommandTimeout", sc.OpCode(), err)
		}
	}
}
