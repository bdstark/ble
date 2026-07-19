package hci

// Tests for Conn.ReadRSSI's repaired (int, error) contract: the Send error
// and the command status are no longer swallowed into a fabricated 0 dBm
// reading. Reuses the fake-socket fixtures from hci_more_test.go
// (fakeSkt, newLoopedHCI) and teardown_test.go (addConn).

import (
	"errors"
	"testing"

	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
)

// readRSSICompletePkt builds a full HCI CommandComplete event packet for the
// Read RSSI command, with the RP's status, connection handle, and RSSI.
func readRSSICompletePkt(status byte, handle uint16, rssi int8) []byte {
	op := (&cmd.ReadRSSI{}).OpCode()
	return []byte{
		pktTypeEvent, evt.CommandCompleteCode,
		7, // parameter length
		1, // NumHCICommandPackets
		byte(op), byte(op >> 8),
		status,
		byte(handle), byte(handle >> 8),
		byte(rssi),
	}
}

// newRSSIHCI is newLoopedHCI plus the buffer pool addConn's recombine
// goroutine needs (same wiring as newTeardownHCI).
func newRSSIHCI(t *testing.T, skt *fakeSkt) *HCI {
	t.Helper()
	return newLoopedHCIOn(t, skt, func(h *HCI) {
		h.pool = NewPool(32, 4)
	})
}

// respondToReadRSSI makes the fake socket answer each Read RSSI command with
// the given completion, echoing the handle from the conn under test.
func respondToReadRSSI(skt *fakeSkt, status byte, rssi int8) {
	want := (&cmd.ReadRSSI{}).OpCode()
	skt.onWrite = func(b []byte) {
		if b[0] != pktTypeCommand || int(b[1])|int(b[2])<<8 != want {
			return
		}
		handle := uint16(b[4]) | uint16(b[5])<<8
		skt.rd <- readRSSICompletePkt(status, handle, rssi)
	}
}

func TestConnReadRSSISuccess(t *testing.T) {
	skt := newFakeSkt()
	respondToReadRSSI(skt, 0x00, -60)
	h := newRSSIHCI(t, skt)
	c := addConn(h, 0x0040)

	rssi, err := c.ReadRSSI()
	if err != nil || rssi != -60 {
		t.Fatalf("ReadRSSI = (%d, %v), want (-60, nil)", rssi, err)
	}
}

// A non-zero command status must surface as the typed ErrCommand — under the
// old signature this case silently returned 0.
func TestConnReadRSSICommandFailure(t *testing.T) {
	skt := newFakeSkt()
	respondToReadRSSI(skt, byte(ErrConnID), -60)
	h := newRSSIHCI(t, skt)
	c := addConn(h, 0x0040)

	rssi, err := c.ReadRSSI()
	if rssi != 0 || !errors.Is(err, ErrConnID) {
		t.Fatalf("ReadRSSI = (%d, %v), want (0, ErrConnID)", rssi, err)
	}
}

// A dead HCI (Send fails outright) must also surface an error.
func TestConnReadRSSISendFailure(t *testing.T) {
	skt := newFakeSkt()
	h := newRSSIHCI(t, skt)
	c := addConn(h, 0x0040)
	h.Close()
	<-h.done

	rssi, err := c.ReadRSSI()
	if rssi != 0 || err == nil {
		t.Fatalf("ReadRSSI on a closed HCI = (%d, %v), want (0, non-nil error)", rssi, err)
	}
}
