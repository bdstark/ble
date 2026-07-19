package hci

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/go-ble/ble/linux/hci/evt"
)

// smpFrame wraps an SMP command in an L2CAP basic frame on the SMP channel,
// exactly as recombine hands it to handleSMP.
func smpFrame(cmd []byte) pdu {
	return mkpdu(len(cmd), cidSMP, cmd)
}

// smpTestConn returns a Conn wired to a fakeSkt so frames written by sendSMP
// can be inspected.
func smpTestConn() (*Conn, *fakeSkt) {
	skt := newFakeSkt()
	c := &Conn{
		hci:      &HCI{skt: skt},
		txBuffer: newTxCredits(NewPool(64, 1)),
		chDone:   make(chan struct{}),
		param:    make(evt.LEConnectionComplete, 19),
	}
	return c, skt
}

// assertPairingFailed checks that exactly one packet was written and that it
// is a well-formed Pairing Failed / Pairing Not Supported L2CAP frame:
// dlen matches the actual SMP payload length, CID is the SMP fixed channel,
// and the command is {pairingFailed, smpPairingNotSupported}.
func assertPairingFailed(t *testing.T, w [][]byte) {
	t.Helper()
	if len(w) != 1 {
		t.Fatalf("wrote %d packets, want 1 Pairing Failed response", len(w))
	}
	// HCI ACL header: packet type (1), handle|flags (2), data length (2).
	pkt := w[0]
	if len(pkt) < 5 {
		t.Fatalf("ACL packet too short: [%X]", pkt)
	}
	frame := pkt[5:]
	if got, want := int(binary.LittleEndian.Uint16(pkt[3:5])), len(frame); got != want {
		t.Fatalf("ACL data length = %d, want %d", got, want)
	}
	// L2CAP basic header: length (2), CID (2), then the SMP command.
	if len(frame) < 4 {
		t.Fatalf("L2CAP frame too short: [%X]", frame)
	}
	payload := frame[4:]
	if got, want := int(binary.LittleEndian.Uint16(frame[0:2])), len(payload); got != want {
		t.Fatalf("L2CAP dlen = %d, want %d (actual payload length)", got, want)
	}
	if got := binary.LittleEndian.Uint16(frame[2:4]); got != cidSMP {
		t.Fatalf("L2CAP CID = 0x%04X, want 0x%04X (SMP)", got, cidSMP)
	}
	if want := []byte{pairingFailed, smpPairingNotSupported}; !bytes.Equal(payload, want) {
		t.Fatalf("SMP command = [%X], want [%X] (Pairing Failed / Pairing Not Supported)", payload, want)
	}
}

// TestSendSMPFrameLength pins the L2CAP length field: it must be the SMP
// command length, not command length + 4 (that off-by-header bug made every
// outgoing Pairing Failed frame declare 4 bytes more than it carried).
func TestSendSMPFrameLength(t *testing.T) {
	c, skt := smpTestConn()
	if err := c.sendSMP([]byte{pairingFailed, smpPairingNotSupported}); err != nil {
		t.Fatalf("sendSMP = %v, want nil", err)
	}
	assertPairingFailed(t, skt.written())
}

// TestHandleSMPClassifiesByOpcode pins that the SMP opcode is read from the
// L2CAP payload, not from the frame header. A Pairing Public Key command has
// a 65-byte payload; code that misreads the length byte (0x41) as the opcode
// classifies it as reserved and stays silent instead of answering.
func TestHandleSMPClassifiesByOpcode(t *testing.T) {
	c, skt := smpTestConn()
	cmd := make([]byte, 65) // opcode + 64-byte public key
	cmd[0] = pairingPublicKey
	if err := c.handleSMP(smpFrame(cmd)); err != nil {
		t.Fatalf("handleSMP = %v, want nil", err)
	}
	assertPairingFailed(t, skt.written())
}

// TestHandleSMPReservedOpcodeIgnored pins the flip side: a reserved opcode
// must be ignored even when the frame's length byte (5 here) happens to be a
// valid opcode (pairingFailed). Misclassifying by the length byte answers a
// frame the spec says to drop.
func TestHandleSMPReservedOpcodeIgnored(t *testing.T) {
	c, skt := smpTestConn()
	if err := c.handleSMP(smpFrame([]byte{0x80, 0x00, 0x00, 0x00, 0x00})); err != nil {
		t.Fatalf("handleSMP reserved opcode = %v, want nil", err)
	}
	if w := skt.written(); len(w) != 0 {
		t.Fatalf("wrote %d packets for a reserved opcode, want 0", len(w))
	}
}

// TestHandleSMPSecurityRequest pins the intended behavior for a peripheral's
// Security Request: like every other recognized SMP command, it is answered
// with Pairing Failed / Pairing Not Supported.
func TestHandleSMPSecurityRequest(t *testing.T) {
	c, skt := smpTestConn()
	if err := c.handleSMP(smpFrame([]byte{securityRequest, 0x01})); err != nil {
		t.Fatalf("handleSMP = %v, want nil", err)
	}
	assertPairingFailed(t, skt.written())
}

// TestHandleSMPAllOpcodes: every recognized opcode (0x01-0x0E) gets the same
// well-formed Pairing Failed response — except Pairing Failed itself, which
// terminates the procedure and must not be answered [Vol 3, Part H, 3.5.5]
// (two rejecting stacks would ping-pong), and the informational Keypress
// notification.
func TestHandleSMPAllOpcodes(t *testing.T) {
	for code := byte(pairingRequest); code <= pairingKeypress; code++ {
		c, skt := smpTestConn()
		if err := c.handleSMP(smpFrame([]byte{code, 0x00})); err != nil {
			t.Fatalf("handleSMP(opcode %#02x) = %v, want nil", code, err)
		}
		if code == pairingFailed || code == pairingKeypress {
			if w := skt.written(); len(w) != 0 {
				t.Fatalf("opcode %#02x got %d responses, want none", code, len(w))
			}
			continue
		}
		assertPairingFailed(t, skt.written())
	}
}

// TestHandleSMPTruncated: frames too short to carry an SMP opcode — from an
// empty pdu up to a header-only frame — are dropped without a panic and
// without a response.
func TestHandleSMPTruncated(t *testing.T) {
	quietLogger(t)
	for _, p := range []pdu{
		{},                    // empty
		{0x01},                // partial header
		{0x01, 0x00, 0x06},    // partial header
		mkpdu(1, cidSMP, nil), // header only, lying dlen
		mkpdu(0, cidSMP, nil), // header only
	} {
		c, skt := smpTestConn()
		if err := c.handleSMP(p); err != nil {
			t.Fatalf("handleSMP(truncated [%X]) = %v, want nil", p, err)
		}
		if w := skt.written(); len(w) != 0 {
			t.Fatalf("wrote %d packets for truncated frame [%X], want 0", len(w), p)
		}
	}
}
