package hci

import (
	"context"
	"encoding/binary"
	"errors"
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

// leMetaCode is the HCI LE Meta Event code (0x3E); every LE subevent, including
// LE Connection Update Complete, arrives under it.
const leMetaCode = 0x3E

// connUpdateCompleteWirePkt builds a full HCI LE Connection Update Complete
// event packet (as read off the socket, with the leading packet-type byte).
func connUpdateCompleteWirePkt(handle uint16, status byte, interval, latency, timeout uint16) []byte {
	return []byte{
		pktTypeEvent, leMetaCode,
		10,   // parameter length
		0x03, // subevent: LE Connection Update Complete
		status,
		byte(handle), byte(handle >> 8),
		byte(interval), byte(interval >> 8),
		byte(latency), byte(latency >> 8),
		byte(timeout), byte(timeout >> 8),
	}
}

// newConnUpdateHCI is a looped HCI wired for the connection-update paths: a
// buffer pool (newConn needs one), the LE-meta dispatch chain, and the
// disconnect handler (teardown races the update).
func newConnUpdateHCI(t *testing.T, skt *fakeSkt) *HCI {
	t.Helper()
	return newLoopedHCIOn(t, skt, func(h *HCI) {
		h.pool = NewPool(32, 4)
		h.evth[leMetaCode] = h.handleLEMeta
		h.evth[evt.DisconnectionCompleteCode] = h.handleDisconnectionComplete
		h.subh[evt.LEConnectionUpdateCompleteSubCode] = h.handleLEConnectionUpdateComplete
	})
}

// addConnWithHandle registers a fresh master conn whose param carries handle,
// so c.param.ConnectionHandle() matches the h.conns key and the completion
// event's handle.
func addConnWithHandle(h *HCI, handle uint16) *Conn {
	p := make(evt.LEConnectionComplete, 19)
	binary.LittleEndian.PutUint16(p[2:], handle) // ConnectionHandle
	c := newConn(h, p)
	h.muConns.Lock()
	h.conns[handle] = c
	h.muConns.Unlock()
	return c
}

// ackAllCommands answers every written command with a CommandComplete (status
// 0), unblocking Send, and emits no meta event.
func ackAllCommands(skt *fakeSkt) {
	skt.onWrite = func(b []byte) {
		if len(b) == 0 || b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		skt.rd <- cmdCompletePkt(op, 0x00)
	}
}

// ackAndCompleteUpdate acks every command and, for the LE Connection Update
// command specifically, follows the ack with an LE Connection Update Complete
// event carrying evtStatus.
func ackAndCompleteUpdate(skt *fakeSkt, handle uint16, evtStatus byte) {
	updOp := (&cmd.LEConnectionUpdate{}).OpCode()
	skt.onWrite = func(b []byte) {
		if len(b) == 0 || b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		skt.rd <- cmdCompletePkt(op, 0x00)
		if op == updOp {
			skt.rd <- connUpdateCompleteWirePkt(handle, evtStatus, 24, 0, 200)
		}
	}
}

// findCmd returns the first written packet for opcode, or nil.
func findCmd(skt *fakeSkt, opcode int) []byte {
	for _, w := range skt.written() {
		if len(w) >= 3 && w[0] == pktTypeCommand && int(w[1])|int(w[2])<<8 == opcode {
			return w
		}
	}
	return nil
}

// waitWaiter blocks until c has a registered update waiter, i.e. UpdateParams
// has sent the command and parked.
func waitWaiter(t *testing.T, c *Conn) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.muUpdate.Lock()
		w := c.updateWaiter
		c.muUpdate.Unlock()
		if w != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("UpdateParams never registered a waiter")
		}
		time.Sleep(time.Millisecond)
	}
}

// relaxParams is a valid "relax the link" request: 30ms/50ms interval, no
// latency, 4s supervision timeout. Encoded: imin 24, imax 40, latency 0,
// timeout 400.
var relaxParams = ble.ConnParams{
	IntervalMin: 30 * time.Millisecond,
	IntervalMax: 50 * time.Millisecond,
	Latency:     0,
	Timeout:     4 * time.Second,
}

// ---------------------------------------------------------------------------
// ble.ConnParams.Encode: conversion and validation
// ---------------------------------------------------------------------------

func TestConnParamsEncodeSuccess(t *testing.T) {
	imin, imax, lat, tmo, err := relaxParams.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if imin != 24 || imax != 40 || lat != 0 || tmo != 400 {
		t.Fatalf("Encode = (%d, %d, %d, %d), want (24, 40, 0, 400)", imin, imax, lat, tmo)
	}
}

func TestConnParamsEncodeRounding(t *testing.T) {
	// 7.4ms rounds to 6 units (7.5ms); 104ms rounds to 10 units (100ms).
	p := ble.ConnParams{
		IntervalMin: 7400 * time.Microsecond,
		IntervalMax: 7400 * time.Microsecond,
		Latency:     0,
		Timeout:     104 * time.Millisecond,
	}
	imin, imax, _, tmo, err := p.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if imin != 6 || imax != 6 || tmo != 10 {
		t.Fatalf("Encode = (imin %d, imax %d, tmo %d), want (6, 6, 10)", imin, imax, tmo)
	}
}

func TestConnParamsEncodeInvalid(t *testing.T) {
	base := relaxParams
	cases := []struct {
		name string
		p    ble.ConnParams
	}{
		{"interval min too small", ble.ConnParams{IntervalMin: time.Millisecond, IntervalMax: 50 * time.Millisecond, Timeout: 4 * time.Second}},
		{"interval min negative", ble.ConnParams{IntervalMin: -1, IntervalMax: 50 * time.Millisecond, Timeout: 4 * time.Second}},
		{"interval max too large", ble.ConnParams{IntervalMin: 30 * time.Millisecond, IntervalMax: 5 * time.Second, Timeout: 4 * time.Second}},
		{"min exceeds max", ble.ConnParams{IntervalMin: 60 * time.Millisecond, IntervalMax: 30 * time.Millisecond, Timeout: 4 * time.Second}},
		{"latency too large", ble.ConnParams{IntervalMin: 30 * time.Millisecond, IntervalMax: 50 * time.Millisecond, Latency: 500, Timeout: 4 * time.Second}},
		{"latency negative", ble.ConnParams{IntervalMin: 30 * time.Millisecond, IntervalMax: 50 * time.Millisecond, Latency: -1, Timeout: 4 * time.Second}},
		{"timeout too small", ble.ConnParams{IntervalMin: 30 * time.Millisecond, IntervalMax: 50 * time.Millisecond, Timeout: 50 * time.Millisecond}},
		{"timeout too large", ble.ConnParams{IntervalMin: 30 * time.Millisecond, IntervalMax: 50 * time.Millisecond, Timeout: 40 * time.Second}},
		{"timeout too small for latency", ble.ConnParams{IntervalMin: 30 * time.Millisecond, IntervalMax: 4 * time.Second, Latency: 400, Timeout: 200 * time.Millisecond}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, _, err := tc.p.Encode()
			if !errors.Is(err, ble.ErrInvalidConnParams) {
				t.Fatalf("Encode(%+v) err = %v, want ErrInvalidConnParams", tc.p, err)
			}
		})
	}
	// Sanity: the base case those are perturbed from is itself valid.
	if _, _, _, _, err := base.Encode(); err != nil {
		t.Fatalf("base params unexpectedly invalid: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Conn.UpdateParams: happy path and the command on the wire
// ---------------------------------------------------------------------------

func TestUpdateParamsSuccess(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAndCompleteUpdate(skt, 0x0040, 0x00)
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	if err := c.UpdateParams(context.Background(), relaxParams); err != nil {
		t.Fatalf("UpdateParams: %v", err)
	}

	// The LE Connection Update command reached the wire with converted fields.
	w := findCmd(skt, (&cmd.LEConnectionUpdate{}).OpCode())
	if w == nil {
		t.Fatal("no LE Connection Update command on the wire")
	}
	params := w[4:]
	got := struct{ handle, imin, imax, lat, tmo uint16 }{
		binary.LittleEndian.Uint16(params[0:]),
		binary.LittleEndian.Uint16(params[2:]),
		binary.LittleEndian.Uint16(params[4:]),
		binary.LittleEndian.Uint16(params[6:]),
		binary.LittleEndian.Uint16(params[8:]),
	}
	if got.handle != 0x0040 || got.imin != 24 || got.imax != 40 || got.lat != 0 || got.tmo != 400 {
		t.Fatalf("command params = %+v, want {handle 0x40 imin 24 imax 40 lat 0 tmo 400}", got)
	}
	// The waiter was cleared on return.
	c.muUpdate.Lock()
	leftover := c.updateWaiter
	c.muUpdate.Unlock()
	if leftover != nil {
		t.Fatal("update waiter not cleared after UpdateParams returned")
	}
}

// TestUpdateParamsErrorStatus: a non-zero status in the completion event
// surfaces as the matching ErrCommand.
func TestUpdateParamsErrorStatus(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAndCompleteUpdate(skt, 0x0040, 0x1F) // Unspecified Error
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	err := c.UpdateParams(context.Background(), relaxParams)
	if !errors.Is(err, ErrUnspecified) {
		t.Fatalf("UpdateParams err = %v, want ErrUnspecified (0x1F)", err)
	}
}

// TestUpdateParamsCommandRejected: when the controller rejects the command
// itself (a non-zero command status, before any completion event), Send's
// error is wrapped and returned, and no waiter is left registered.
func TestUpdateParamsCommandRejected(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	// Answer the LE Connection Update command with a non-zero status; every
	// other command (e.g. the teardown Disconnect) is acked cleanly.
	updOp := (&cmd.LEConnectionUpdate{}).OpCode()
	skt.onWrite = func(b []byte) {
		if len(b) == 0 || b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		status := byte(0x00)
		if op == updOp {
			status = 0x0C // Command Disallowed
		}
		skt.rd <- cmdCompletePkt(op, status)
	}
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	if err := c.UpdateParams(context.Background(), relaxParams); !errors.Is(err, ErrDisallowed) {
		t.Fatalf("UpdateParams err = %v, want ErrDisallowed", err)
	}
	c.muUpdate.Lock()
	leftover := c.updateWaiter
	c.muUpdate.Unlock()
	if leftover != nil {
		t.Fatal("update waiter not cleared after a rejected command")
	}
}

// TestUpdateParamsInvalid: bad params are rejected before any command is sent.
func TestUpdateParamsInvalid(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAllCommands(skt)
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	bad := ble.ConnParams{IntervalMin: time.Millisecond, IntervalMax: time.Millisecond, Timeout: time.Second}
	if err := c.UpdateParams(context.Background(), bad); !errors.Is(err, ble.ErrInvalidConnParams) {
		t.Fatalf("UpdateParams(bad) err = %v, want ErrInvalidConnParams", err)
	}
	if w := findCmd(skt, (&cmd.LEConnectionUpdate{}).OpCode()); w != nil {
		t.Fatal("invalid params still put an LE Connection Update command on the wire")
	}
}

// TestUpdateParamsContextCancel: with the completion event withheld, an
// expiring context unblocks the wait with ctx.Err().
func TestUpdateParamsContextCancel(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAllCommands(skt) // acks the command, never completes the update
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err := c.UpdateParams(ctx, relaxParams)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("UpdateParams err = %v, want context.DeadlineExceeded", err)
	}
}

// TestUpdateParamsTimeout: with the completion event withheld and no ctx
// deadline, the package-var connUpdateTimeout bounds the wait.
func TestUpdateParamsTimeout(t *testing.T) {
	quietLogger(t)
	old := connUpdateTimeout
	connUpdateTimeout = 50 * time.Millisecond
	defer func() { connUpdateTimeout = old }()

	skt := newFakeSkt()
	ackAllCommands(skt)
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	err := c.UpdateParams(context.Background(), relaxParams)
	if !errors.Is(err, ErrConnUpdateTimeout) {
		t.Fatalf("UpdateParams err = %v, want ErrConnUpdateTimeout", err)
	}
}

// TestUpdateParamsDisconnectMidWait: a disconnect that lands while UpdateParams
// is parked unblocks it with ErrClosed rather than hanging.
func TestUpdateParamsDisconnectMidWait(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAllCommands(skt)
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	errc := make(chan error, 1)
	go func() { errc <- c.UpdateParams(context.Background(), relaxParams) }()
	waitWaiter(t, c) // command sent, parked in the wait select

	skt.rd <- disconnWirePkt(0x0040)
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("UpdateParams err = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("UpdateParams did not return after disconnect")
	}
}

// TestUpdateParamsOnClosedConn: an update on an already-torn-down link is
// refused up front with ErrClosed and never touches the controller.
func TestUpdateParamsOnClosedConn(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAllCommands(skt)
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)
	c.closeChans() // chDone closed

	if err := c.UpdateParams(context.Background(), relaxParams); !errors.Is(err, ErrClosed) {
		t.Fatalf("UpdateParams on closed conn err = %v, want ErrClosed", err)
	}
	if w := findCmd(skt, (&cmd.LEConnectionUpdate{}).OpCode()); w != nil {
		t.Fatal("UpdateParams on a closed conn still sent a command")
	}
}

// TestUpdateParamsConcurrent: a second update while one is pending is refused
// with ErrUpdateInProgress; no command is sent for it.
func TestUpdateParamsConcurrent(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAllCommands(skt) // first update parks (no completion event)
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.UpdateParams(ctx, relaxParams) }()
	waitWaiter(t, c)

	if err := c.UpdateParams(context.Background(), relaxParams); !errors.Is(err, ErrUpdateInProgress) {
		t.Fatalf("second UpdateParams err = %v, want ErrUpdateInProgress", err)
	}

	cancel() // let the first one finish
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("first UpdateParams err = %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// handleLEConnectionUpdateComplete: no-waiter completions are harmless
// ---------------------------------------------------------------------------

// TestConnUpdateCompleteNoWaiter: a completion for a registered conn with no
// waiter (the slave-forwarded path from signal.go) is a no-op, not a panic or
// a block.
func TestConnUpdateCompleteNoWaiter(t *testing.T) {
	quietLogger(t)
	h := &HCI{conns: map[uint16]*Conn{}, pool: NewPool(32, 4)}
	addConnWithHandle(h, 0x0040)

	payload := connUpdateCompleteWirePkt(0x0040, 0x00, 24, 0, 400)[3:] // from the subevent code
	if err := h.handleLEConnectionUpdateComplete(payload); err != nil {
		t.Fatalf("handleLEConnectionUpdateComplete (no waiter) = %v, want nil", err)
	}
}

// TestConnUpdateCompleteUnknownHandle: a completion for a handle with no conn
// (a completion racing teardown) is a harmless no-op.
func TestConnUpdateCompleteUnknownHandle(t *testing.T) {
	debugLogger(t) // exercise the debug-gated log path
	h := &HCI{conns: map[uint16]*Conn{}}

	payload := connUpdateCompleteWirePkt(0x0099, 0x00, 24, 0, 400)[3:]
	if err := h.handleLEConnectionUpdateComplete(payload); err != nil {
		t.Fatalf("handleLEConnectionUpdateComplete (unknown handle) = %v, want nil", err)
	}
}

// TestDeliverConnUpdateDuplicate: a second delivery to an already-full waiter
// channel is dropped, never blocking sktLoop.
func TestDeliverConnUpdateDuplicate(t *testing.T) {
	c := &Conn{updateWaiter: make(chan leConnUpdate, 1)}
	c.deliverConnUpdate(leConnUpdate{status: 0})
	c.deliverConnUpdate(leConnUpdate{status: 1}) // must not block or panic
	if len(c.updateWaiter) != 1 {
		t.Fatalf("waiter holds %d results, want 1 (duplicate dropped)", len(c.updateWaiter))
	}
}

// ---------------------------------------------------------------------------
// Race: an update driven while a disconnect races it (-race)
// ---------------------------------------------------------------------------

// TestUpdateParamsDisconnectRace runs UpdateParams and a disconnect against
// each other repeatedly. Any interleaving must be panic-free and leave
// UpdateParams returning (nil on a delivered completion, ErrClosed on
// teardown, or ctx.Err()), never hanging. Run with -race.
func TestUpdateParamsDisconnectRace(t *testing.T) {
	quietLogger(t)
	for i := 0; i < 50; i++ {
		skt := newFakeSkt()
		ackAndCompleteUpdate(skt, 0x0040, 0x00) // ack + completion event
		h := newConnUpdateHCI(t, skt)
		c := addConnWithHandle(h, 0x0040)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		errc := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			errc <- c.UpdateParams(ctx, relaxParams)
		}()
		go func() {
			defer wg.Done()
			skt.rd <- disconnWirePkt(0x0040)
		}()

		select {
		case err := <-errc:
			if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("iteration %d: UpdateParams err = %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: UpdateParams hung", i)
		}
		wg.Wait()
		cancel()
	}
}

// ---------------------------------------------------------------------------
// Peer-requested updates share the single per-conn update slot
// ---------------------------------------------------------------------------

// TestPeerUpdateCompletionSkipsAppWaiter: a completion owed to a
// peer-requested (signal-path) update must be consumed by the pending flag,
// never delivered to an app waiter — LE Connection Update Complete has no
// correlation ID, so ownership is tracked host-side.
func TestPeerUpdateCompletionSkipsAppWaiter(t *testing.T) {
	skt := newFakeSkt()
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	ch := make(chan leConnUpdate, 1)
	c.muUpdate.Lock()
	c.updateWaiter = ch
	c.updateSignalPending = true
	c.muUpdate.Unlock()

	c.deliverConnUpdate(leConnUpdate{status: 0x3B}) // the peer update's completion
	select {
	case u := <-ch:
		t.Fatalf("app waiter received the peer update's completion (status 0x%02X)", u.status)
	default:
	}
	c.muUpdate.Lock()
	cleared := !c.updateSignalPending
	c.muUpdate.Unlock()
	if !cleared {
		t.Fatal("updateSignalPending not cleared by its completion")
	}

	// The next completion is the app's own.
	c.deliverConnUpdate(leConnUpdate{status: 0x00})
	select {
	case u := <-ch:
		if u.status != 0x00 {
			t.Fatalf("app waiter got status 0x%02X, want 0x00", u.status)
		}
	default:
		t.Fatal("app waiter never received its own completion")
	}
}

// TestUpdateParamsBlockedDuringPeerUpdate: while a peer-requested update is
// in flight, UpdateParams must refuse with ErrUpdateInProgress instead of
// racing it for the uncorrelated completion.
func TestUpdateParamsBlockedDuringPeerUpdate(t *testing.T) {
	skt := newFakeSkt()
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	c.muUpdate.Lock()
	c.updateSignalPending = true
	c.muUpdate.Unlock()

	err := c.UpdateParams(context.Background(), ble.ConnParams{
		IntervalMin: 50 * time.Millisecond, IntervalMax: 50 * time.Millisecond,
		Timeout: 4 * time.Second,
	})
	if !errors.Is(err, ErrUpdateInProgress) {
		t.Fatalf("UpdateParams during peer update = %v, want ErrUpdateInProgress", err)
	}
}

// TestPeerUpdateRejectedWhileAppUpdateInFlight: a Connection Parameter
// Update Request arriving while an app UpdateParams waits must be rejected
// (Result 0x0001) without firing a second LE Connection Update command.
func TestPeerUpdateRejectedWhileAppUpdateInFlight(t *testing.T) {
	skt := newFakeSkt()
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	c.muUpdate.Lock()
	c.updateWaiter = make(chan leConnUpdate, 1) // an app update is in flight
	c.muUpdate.Unlock()

	// CPUP request: code, id, len(8), then IntervalMin/Max, Latency, Timeout.
	sig := make(sigCmd, 4+8)
	sig[0] = SignalConnectionParameterUpdateRequest
	sig[1] = 0x01
	binary.LittleEndian.PutUint16(sig[2:4], 8)
	binary.LittleEndian.PutUint16(sig[4:6], 40)   // IntervalMin
	binary.LittleEndian.PutUint16(sig[6:8], 56)   // IntervalMax
	binary.LittleEndian.PutUint16(sig[8:10], 0)   // SlaveLatency
	binary.LittleEndian.PutUint16(sig[10:12], 42) // TimeoutMultiplier
	c.handleConnectionParameterUpdateRequest(sig)

	wantOp := (&cmd.LEConnectionUpdate{}).OpCode()
	var rsp []byte
	for _, w := range skt.written() {
		if w[0] == pktTypeCommand && int(w[1])|int(w[2])<<8 == wantOp {
			t.Fatal("a second LE Connection Update command was fired while one was in flight")
		}
		if w[0] == pktTypeACLData {
			rsp = w
		}
	}
	if rsp == nil {
		t.Fatal("no L2CAP response reached the wire")
	}
	// The response's Result field is the packet's final uint16: 1 = rejected.
	if got := binary.LittleEndian.Uint16(rsp[len(rsp)-2:]); got != 1 {
		t.Fatalf("Connection Parameter Update Response result = %d, want 1 (rejected)", got)
	}
}

// TestPeerUpdateAcceptedWhenSlotFree: with no app update in flight, the
// peer's request is forwarded (one LE Connection Update command), answered
// with Result 0, the slot is held until the completion arrives, and the
// completion is consumed without touching any later app waiter.
func TestPeerUpdateAcceptedWhenSlotFree(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	sig := make(sigCmd, 4+8)
	sig[0] = SignalConnectionParameterUpdateRequest
	sig[1] = 0x01
	binary.LittleEndian.PutUint16(sig[2:4], 8)
	binary.LittleEndian.PutUint16(sig[4:6], 40)
	binary.LittleEndian.PutUint16(sig[6:8], 56)
	binary.LittleEndian.PutUint16(sig[8:10], 0)
	binary.LittleEndian.PutUint16(sig[10:12], 42)
	c.handleConnectionParameterUpdateRequest(sig)

	wantOp := (&cmd.LEConnectionUpdate{}).OpCode()
	cmds, rsps := 0, [][]byte{}
	for _, w := range skt.written() {
		if w[0] == pktTypeCommand && int(w[1])|int(w[2])<<8 == wantOp {
			cmds++
		}
		if w[0] == pktTypeACLData {
			rsps = append(rsps, w)
		}
	}
	if cmds != 1 || len(rsps) != 1 {
		t.Fatalf("accept path wrote %d update commands and %d responses, want 1 and 1", cmds, len(rsps))
	}
	if got := binary.LittleEndian.Uint16(rsps[0][len(rsps[0])-2:]); got != 0 {
		t.Fatalf("response result = %d, want 0 (accepted)", got)
	}
	c.muUpdate.Lock()
	pending := c.updateSignalPending
	c.muUpdate.Unlock()
	if !pending {
		t.Fatal("update slot not held while the peer update awaits completion")
	}
	c.deliverConnUpdate(leConnUpdate{status: 0x00})
	c.muUpdate.Lock()
	pending = c.updateSignalPending
	c.muUpdate.Unlock()
	if pending {
		t.Fatal("update slot not released by the peer update's completion")
	}
}

// TestPeerUpdateSendFailureReleasesSlot: when forwarding the update to the
// controller fails, the slot is released (no completion is owed) and the
// peer gets a rejection instead of silence.
func TestPeerUpdateSendFailureReleasesSlot(t *testing.T) {
	quietLogger(t)
	oldCmd := cmdTimeout
	cmdTimeout = 50 * time.Millisecond
	t.Cleanup(func() { cmdTimeout = oldCmd })

	skt := newFakeSkt() // never answers: the forward times out
	h := newConnUpdateHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	sig := make(sigCmd, 4+8)
	sig[0] = SignalConnectionParameterUpdateRequest
	sig[1] = 0x01
	binary.LittleEndian.PutUint16(sig[2:4], 8)
	binary.LittleEndian.PutUint16(sig[4:6], 40)
	binary.LittleEndian.PutUint16(sig[6:8], 56)
	binary.LittleEndian.PutUint16(sig[8:10], 0)
	binary.LittleEndian.PutUint16(sig[10:12], 42)
	c.handleConnectionParameterUpdateRequest(sig)

	c.muUpdate.Lock()
	pending := c.updateSignalPending
	c.muUpdate.Unlock()
	if pending {
		t.Fatal("update slot still held after the forward failed")
	}
	var rsp []byte
	for _, w := range skt.written() {
		if w[0] == pktTypeACLData {
			rsp = w
		}
	}
	if rsp == nil {
		t.Fatal("no rejection reached the peer after the forward failed")
	}
	if got := binary.LittleEndian.Uint16(rsp[len(rsp)-2:]); got != 1 {
		t.Fatalf("response result = %d, want 1 (rejected)", got)
	}
}
