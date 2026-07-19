package hci

import (
	"io"
	"strings"
	"testing"
	"time"
)

// cancelDial's bounded chMasterConn wait (after LECreateConnectionCancel is
// rejected with ErrDisallowed because the connection already completed) has
// three exits: the master connection arrives, the HCI shuts down, or a 5s
// timeout. The first two are covered here; the timeout branch is skipped to
// keep the suite fast.

// TestCancelDialSuccess: the cancel command succeeds — the pending
// connection was canceled.
func TestCancelDialSuccess(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newLoopedHCI(t, skt)

	cln, err := h.cancelDial()
	if cln != nil || err == nil || !strings.Contains(err.Error(), "connection canceled") {
		t.Fatalf("cancelDial = (%v, %v), want (nil, connection canceled)", cln, err)
	}
}

// TestCancelDialConnArrives: cancel is disallowed because the connection
// completed; the connection then arrives on chMasterConn and a GATT client
// is returned.
func TestCancelDialConnArrives(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x0C) // ErrDisallowed
	h := newLoopedHCI(t, skt)

	// A connection functional enough for gatt.NewClient: its att loop parks
	// in Conn.Read (nil chInPDU never delivers).
	c := &Conn{hci: h, txMTU: 23}
	h.chMasterConn = make(chan *Conn, 1)
	h.chMasterConn <- c

	cln, err := h.cancelDial()
	if err != nil {
		t.Fatalf("cancelDial: %v", err)
	}
	if cln == nil {
		t.Fatal("cancelDial returned a nil client for an arrived connection")
	}
}

// TestCancelDialDone: cancel is disallowed but the HCI shuts down while
// waiting for the connection; the transport error is returned instead of
// parking forever.
func TestCancelDialDone(t *testing.T) {
	skt := newFakeSkt()
	skt.onWrite = func(b []byte) {
		if b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		skt.rd <- cmdCompletePkt(op, 0x0C) // ErrDisallowed
		skt.rd <- nil                      // then the transport dies (EOF)
	}
	h := newLoopedHCI(t, skt)

	start := time.Now()
	cln, err := h.cancelDial()
	if cln != nil || err != io.EOF {
		t.Fatalf("cancelDial = (%v, %v), want (nil, io.EOF)", cln, err)
	}
	if time.Since(start) > 4*time.Second {
		t.Fatal("cancelDial waited for the timeout instead of the done channel")
	}
}

// TestCancelDialOtherError: any other cancel failure is wrapped and
// returned.
func TestCancelDialOtherError(t *testing.T) {
	skt := newFakeSkt()
	respondWithStatus(skt, 0x01) // Unknown HCI Command
	h := newLoopedHCI(t, skt)

	cln, err := h.cancelDial()
	if cln != nil || err == nil || !strings.Contains(err.Error(), "cancel connection failed") {
		t.Fatalf("cancelDial = (%v, %v), want wrapped cancel failure", cln, err)
	}
}
