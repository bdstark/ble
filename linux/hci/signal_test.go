package hci

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/go-ble/ble/linux/hci/evt"
)

// TestCommandRejectRoundTrip pins the Command Reject wire format [Vol 3,
// Part A, 4.1]: 2-byte little-endian Reason followed by the raw Data bytes,
// and Marshal/Unmarshal as inverses.
func TestCommandRejectRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		sig  CommandReject
		want []byte
	}{
		{
			name: "not understood, no data",
			sig:  CommandReject{Reason: 0x0000},
			want: []byte{0x00, 0x00},
		},
		{
			name: "MTU exceeded with actual MTUsig",
			sig:  CommandReject{Reason: 0x0001, Data: []byte{0x17, 0x00}},
			want: []byte{0x01, 0x00, 0x17, 0x00},
		},
		{
			name: "invalid CID with endpoints",
			sig:  CommandReject{Reason: 0x0002, Data: []byte{0x40, 0x00, 0x41, 0x00}},
			want: []byte{0x02, 0x00, 0x40, 0x00, 0x41, 0x00},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.sig.Marshal()
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if !bytes.Equal(b, tc.want) {
				t.Fatalf("Marshal = [% X], want [% X]", b, tc.want)
			}
			var got CommandReject
			if err := got.Unmarshal(b); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if got.Reason != tc.sig.Reason || !bytes.Equal(got.Data, tc.sig.Data) {
				t.Fatalf("round trip = %+v, want %+v", got, tc.sig)
			}
		})
	}
}

func TestCommandRejectUnmarshalShort(t *testing.T) {
	var s CommandReject
	if err := s.Unmarshal([]byte{0x01}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Unmarshal of 1 byte: err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestHandleSignalMTUExceededSendsReject verifies the oversized-command path
// actually puts a Command Reject on the air: handleSignal writes a single
// ACL packet whose L2CAP payload is the reject with reason 0x0001 and the
// actual MTUsig, echoing the request's identifier.
func TestHandleSignalMTUExceededSendsReject(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	c := &Conn{
		hci:      &HCI{skt: skt},
		sigRxMTU: 23,
		txBuffer: newTxCredits(NewPool(64, 1)),
		chDone:   make(chan struct{}),
		param:    make(evt.LEConnectionComplete, 19),
	}
	// Declared length 24 exceeds MTUsig 23; payload carries code+id so
	// sigCmd.id() finds the identifier to echo.
	payload := make([]byte, 24)
	payload[0] = SignalConnectionParameterUpdateRequest
	payload[1] = 0x2A
	if err := c.handleSignal(mkpdu(24, cidLESignal, payload)); err != nil {
		t.Fatalf("handleSignal = %v, want nil", err)
	}
	w := skt.written()
	if len(w) != 1 {
		t.Fatalf("wrote %d packets, want 1 command reject", len(w))
	}
	if w[0][0] != pktTypeACLData {
		t.Fatalf("packet type = %#x, want ACL data", w[0][0])
	}
	want := []byte{
		0x08, 0x00, // L2CAP length: 4-byte sig header + 4-byte reject
		byte(cidLESignal), 0x00,
		SignalCommandReject,
		0x2A,       // identifier echoed from the request
		0x04, 0x00, // data length
		0x01, 0x00, // reason: signaling MTU exceeded
		0x17, 0x00, // actual MTUsig (23)
	}
	if got := w[0][5:]; !bytes.Equal(got, want) {
		t.Fatalf("reject pdu = [% X], want [% X]", got, want)
	}
}
