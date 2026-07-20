package hci

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bdstark/ble"
)

// A send() completion timeout abandons the command, but the controller may
// still execute it once the stall clears — its straggling completion is
// dropped and the host never records the state change. The controller then
// answers every retry of the same state-transition with Command Disallowed.
// Production incident 2026-07-19: a Set Scan Enable abandoned this way left
// the controller scanning while the host believed it was not, and every
// scan attempt failed with Disallowed for 20 hours. These tests cover the
// reconciliation paths that recover from that divergence.

const (
	opScanEnable = 0x200C
	opCreateConn = 0x200D
	opConnCancel = 0x200E
)

// scriptedResponder answers each written HCI command with a CommandComplete
// whose status comes from the per-opcode script, consumed in order; opcodes
// past the end of their script get success.
func scriptedResponder(skt *fakeSkt, script map[int][]byte) *[][]byte {
	var mu sync.Mutex
	var cmds [][]byte
	skt.onWrite = func(b []byte) {
		if b[0] != pktTypeCommand {
			return
		}
		mu.Lock()
		cmds = append(cmds, b)
		op := int(b[1]) | int(b[2])<<8
		status := byte(0x00)
		if s := script[op]; len(s) > 0 {
			status = s[0]
			script[op] = s[1:]
		}
		mu.Unlock()
		skt.rd <- cmdCompletePkt(op, status)
	}
	return &cmds
}

func opcodeOf(pkt []byte) int { return int(pkt[1]) | int(pkt[2])<<8 }

// TestScanDisallowedReconciles: the first enable is rejected with Command
// Disallowed (controller already scanning, host unaware); Scan must stop the
// unknown scan, re-enable, and succeed.
func TestScanDisallowedReconciles(t *testing.T) {
	skt := newFakeSkt()
	cmds := scriptedResponder(skt, map[int][]byte{opScanEnable: {0x0C, 0x00, 0x00}})
	h := newLoopedHCI(t, skt)

	if err := h.Scan(false); err != nil {
		t.Fatalf("Scan = %v, want reconciled success", err)
	}
	got := *cmds
	if len(got) != 3 {
		t.Fatalf("wrote %d commands, want 3 (enable, stop, enable)", len(got))
	}
	// All three are Set Scan Enable; the LEScanEnable field (first parameter)
	// must go 1 (rejected), 0 (reconciling stop), 1 (retry).
	for i, wantEnable := range []byte{1, 0, 1} {
		if opcodeOf(got[i]) != opScanEnable || got[i][4] != wantEnable {
			t.Fatalf("command %d = opcode %#x enable %d, want scanEnable enable %d",
				i, opcodeOf(got[i]), got[i][4], wantEnable)
		}
	}
}

// TestScanDisallowedStopFails: if the reconciling stop itself fails, Scan
// reports that failure instead of silently retrying.
func TestScanDisallowedStopFails(t *testing.T) {
	skt := newFakeSkt()
	// enable: Disallowed; stop: Unknown HCI Command (0x01).
	scriptedResponder(skt, map[int][]byte{opScanEnable: {0x0C, 0x01}})
	h := newLoopedHCI(t, skt)

	err := h.Scan(false)
	if err == nil || !errors.Is(err, ErrCommand(0x01)) {
		t.Fatalf("Scan = %v, want wrapped stop failure", err)
	}
}

// TestStopScanningDisallowedIsSuccess: Command Disallowed on the disable
// means the controller is not scanning — the desired state.
func TestStopScanningDisallowedIsSuccess(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x0C)
	h := newLoopedHCI(t, skt)

	if err := h.StopScanning(); err != nil {
		t.Fatalf("StopScanning = %v, want nil for not-scanning controller", err)
	}
}

// TestDialDisallowedReconciles: the first create-connection is rejected with
// Command Disallowed (controller stuck initiating a forgotten create); Dial
// must cancel it and retry. The retried create succeeds; the connection
// never arrives, so the context deadline cancels the dial — proving the
// retry got past the Disallowed wall.
func TestDialDisallowedReconciles(t *testing.T) {
	skt := newFakeSkt()
	cmds := scriptedResponder(skt, map[int][]byte{opCreateConn: {0x0C, 0x00}})
	h := newLoopedHCI(t, skt)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	cln, err := h.Dial(ctx, ble.NewAddr("11:22:33:44:55:66"))
	if cln != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial = (%v, %v), want canceled dial wrapping DeadlineExceeded", cln, err)
	}
	got := *cmds
	if len(got) < 3 {
		t.Fatalf("wrote %d commands, want at least 3 (create, cancel, create)", len(got))
	}
	for i, want := range []int{opCreateConn, opConnCancel, opCreateConn} {
		if opcodeOf(got[i]) != want {
			t.Fatalf("command %d = opcode %#x, want %#x", i, opcodeOf(got[i]), want)
		}
	}
}
