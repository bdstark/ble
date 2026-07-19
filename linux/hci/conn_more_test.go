package hci

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-ble/ble/linux/hci/evt"
)

// mkpdu builds an L2CAP pdu with the given declared payload length, CID, and
// actual payload bytes.
func mkpdu(dlen int, cid uint16, payload []byte) pdu {
	p := make(pdu, 4+len(payload))
	binary.LittleEndian.PutUint16(p[0:2], uint16(dlen))
	binary.LittleEndian.PutUint16(p[2:4], cid)
	copy(p[4:], payload)
	return p
}

// mkACLPkt wraps an L2CAP pdu in an HCI ACL data packet body (as delivered
// on chInPkt, i.e. without the 1-byte HCI packet-type header).
func mkACLPkt(handle uint16, p pdu) packet {
	b := make([]byte, 4+len(p))
	binary.LittleEndian.PutUint16(b[0:2], handle)
	binary.LittleEndian.PutUint16(b[2:4], uint16(len(p)))
	copy(b[4:], p)
	return b
}

func closedDone() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// ---------------------------------------------------------------------------
// Conn.Read error paths
// ---------------------------------------------------------------------------

func TestReadEmptyPacket(t *testing.T) {
	c := &Conn{chInPDU: make(chan pdu, 1)}
	c.chInPDU <- pdu{}
	_, err := c.Read(make([]byte, 64))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Read of empty packet: err = %v, want errors.Is(..., io.ErrUnexpectedEOF)", err)
	}
}

func TestReadShortBuffer(t *testing.T) {
	c := &Conn{chInPDU: make(chan pdu, 1)}
	c.chInPDU <- mkpdu(10, cidLEAtt, make([]byte, 10))
	_, err := c.Read(make([]byte, 4))
	if !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("Read into short buffer: err = %v, want errors.Is(..., io.ErrShortBuffer)", err)
	}
}

// ---------------------------------------------------------------------------
// Conn.Write / writePDU
// ---------------------------------------------------------------------------

func TestWriteExceedsMTU(t *testing.T) {
	c := &Conn{txMTU: 23}
	_, err := c.Write(make([]byte, 24))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Write beyond MTU: err = %v, want errors.Is(..., io.ErrShortWrite)", err)
	}
}

// TestWritePDUClosedFastPath: a connection already closed fails immediately
// with ErrClosed, before touching the buffer pool.
func TestWritePDUClosedFastPath(t *testing.T) {
	c := &Conn{
		txBuffer: newTxCredits(NewPool(32, 1)),
		chDone:   closedDone(),
	}
	if _, err := c.writePDU([]byte{1, 2, 3}); err != ErrClosed {
		t.Fatalf("writePDU on closed conn: err = %v, want ErrClosed", err)
	}
}

// TestWritePDUCreditTimeout pins the bounded credit wait: with the pool
// exhausted and no NumberOfCompletedPackets ever coming back, writePDU must
// give up with ErrCreditTimeout instead of parking forever.
func TestWritePDUCreditTimeout(t *testing.T) {
	old := ACLWriteTimeout
	ACLWriteTimeout = 30 * time.Millisecond
	defer func() { ACLWriteTimeout = old }()

	c := &Conn{
		txBuffer: newTxCredits(NewPool(32, 1)),
		chDone:   make(chan struct{}),
	}
	c.txBuffer.Get() // exhaust the pool; credits never return

	if _, err := c.writePDU([]byte{1, 2, 3}); !errors.Is(err, ErrCreditTimeout) {
		t.Fatalf("writePDU without credits: err = %v, want errors.Is(..., ErrCreditTimeout)", err)
	}
}

// TestWriteFragmented drives the full happy path of Write/writePDU with a
// fake socket: credit acquisition (GetTimeout success), header assembly, and
// fragment flushing with continuing-fragment flags.
func TestWriteFragmented(t *testing.T) {
	skt := newFakeSkt()
	c := &Conn{
		hci:      &HCI{skt: skt},
		txBuffer: newTxCredits(NewPool(9, 2)), // fragment payload capacity: 9-1-4 = 4 bytes
		chDone:   make(chan struct{}),
		param:    make(evt.LEConnectionComplete, 19),
		txMTU:    23,
	}
	n, err := c.Write([]byte{1, 2, 3}) // 4-byte L2CAP header + 3 bytes = 7-byte PDU, 2 fragments
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 7 {
		t.Fatalf("Write sent %d bytes, want 7", n)
	}
	w := skt.written()
	if len(w) != 2 {
		t.Fatalf("wrote %d fragments, want 2", len(w))
	}
	if w[0][0] != pktTypeACLData || w[1][0] != pktTypeACLData {
		t.Fatalf("fragments are not ACL data packets: % X / % X", w[0], w[1])
	}
	if pbf := (w[0][2] >> 4) & 0x3; pbf != pbfHostToControllerStart {
		t.Fatalf("first fragment pbf = %d, want %d", pbf, pbfHostToControllerStart)
	}
	if pbf := (w[1][2] >> 4) & 0x3; pbf != pbfContinuing {
		t.Fatalf("second fragment pbf = %d, want %d", pbf, pbfContinuing)
	}
}

// TestWritePDUClosedAtFlush covers the pre-flush chDone check: the
// connection closes after a fragment is in flight, and writePDU must notice
// before writing the next fragment to the socket. The disconnect lands
// between GetTimeout and the flush; GetTimeout's select picks randomly
// between the returned credit and the closed done channel, so retry until
// the flush-path outcome (identity ErrClosed) is observed. Both outcomes are
// valid behavior.
func TestWritePDUClosedAtFlush(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		skt := newFakeSkt()
		done := make(chan struct{})
		c := &Conn{
			hci:      &HCI{skt: skt},
			txBuffer: newTxCredits(NewPool(9, 1)), // one credit: 4-byte fragments
			chDone:   done,
			param:    make(evt.LEConnectionComplete, 19),
		}
		var once sync.Once
		skt.onWrite = func([]byte) {
			once.Do(func() {
				close(done)       // disconnect after the first fragment...
				c.txBuffer.Put()  // ...which also returns its credit
			})
		}
		_, err := c.writePDU(make([]byte, 6)) // two fragments: 4 + 2
		if err == ErrClosed {
			return // flush-path check fired (identity ErrClosed)
		}
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("attempt %d: err = %v, want an ErrClosed", attempt, err)
		}
		// GetTimeout's done case won the race; try again.
	}
	t.Fatal("flush-path close check never selected in 100 attempts")
}

// ---------------------------------------------------------------------------
// buffer.go: GetTimeout success branch
// ---------------------------------------------------------------------------

func TestGetTimeoutSuccess(t *testing.T) {
	c := newTxCredits(NewPool(4, 1))
	b, err := c.GetTimeout(make(chan struct{}), time.Second)
	if err != nil || b == nil {
		t.Fatalf("GetTimeout with credit available = (%v, %v), want a buffer", b, err)
	}
	if b.Len() != 0 {
		t.Fatalf("buffer not reset: len = %d", b.Len())
	}
	c.Put()
	if b2, err := c.GetTimeout(make(chan struct{}), time.Second); err != nil || b2 == nil {
		t.Fatalf("GetTimeout after Put = (%v, %v), want the recycled buffer", b2, err)
	}
}

// ---------------------------------------------------------------------------
// recombine
// ---------------------------------------------------------------------------

// TestNewConnRecombineFailedLog: a malformed inbound packet makes recombine
// fail; newConn's goroutine must log it and close chInPDU (unblocking
// readers) instead of dying silently.
func TestNewConnRecombineFailedLog(t *testing.T) {
	quietLogger(t)
	h := &HCI{pool: NewPool(64, 2)}
	c := newConn(h, make(evt.LEConnectionComplete, 19))

	// LE-ATT fragment claiming a 100-byte payload: larger than rxMPS (23).
	c.chInPkt <- mkACLPkt(0x0040, mkpdu(100, cidLEAtt, []byte{0xAA}))

	select {
	case _, ok := <-c.chInPDU:
		if ok {
			t.Fatal("received a PDU from a malformed packet")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("chInPDU was not closed after recombine error")
	}
}

// TestRecombineUnknownCID: a well-formed PDU on an unrecognized channel is
// logged and dropped, not delivered and not an error.
func TestRecombineUnknownCID(t *testing.T) {
	quietLogger(t)
	c := &Conn{
		chInPkt: make(chan packet, 1),
		chInPDU: make(chan pdu, 1),
		rxMPS:   23,
	}
	c.chInPkt <- mkACLPkt(0x0040, mkpdu(2, 0x0040, []byte{0xAA, 0xBB}))
	if err := c.recombine(); err != nil {
		t.Fatalf("recombine = %v, want nil", err)
	}
	if len(c.chInPDU) != 0 {
		t.Fatal("PDU on unknown CID was delivered to chInPDU")
	}
}

// ---------------------------------------------------------------------------
// signal.go / smp.go debug gates and error paths
// ---------------------------------------------------------------------------

func TestLogDebugEnabled(t *testing.T) {
	quietLogger(t)
	if logDebugEnabled() {
		t.Fatal("logDebugEnabled = true at info level")
	}
	debugLogger(t)
	if !logDebugEnabled() {
		t.Fatal("logDebugEnabled = false at debug level")
	}
}

// TestHandleSignalMTUExceeded exercises handleSignal's oversized-command
// error path with debug logging on: the recv/send debug dumps run, and the
// Command Reject send failing on the closed connection (writePDU returns
// ErrClosed) hits the send-response-failed error log. The success path is
// covered by TestHandleSignalMTUExceededSendsReject in signal_test.go.
func TestHandleSignalMTUExceeded(t *testing.T) {
	debugLogger(t)
	c := &Conn{
		sigRxMTU: 1, // anything is oversized
		txBuffer: newTxCredits(NewPool(32, 1)),
		chDone:   closedDone(), // sendResponse's writePDU fails with ErrClosed
	}
	// dlen 4 > sigRxMTU 1; payload holds code+id so sigCmd.id() works.
	p := mkpdu(4, cidLESignal, []byte{0x01, 0x2A, 0x00, 0x00})
	if err := c.handleSignal(p); err != nil {
		t.Fatalf("handleSignal = %v, want nil", err)
	}
}

// TestHandleSignalDisconnectRequest drives handleSignal through a
// well-formed Disconnect Request, whose Disconnect Response marshals
// successfully — so sendResponse reaches its debug dump and the socket
// write.
func TestHandleSignalDisconnectRequest(t *testing.T) {
	debugLogger(t)
	skt := newFakeSkt()
	c := &Conn{
		hci:      &HCI{skt: skt},
		sigRxMTU: 512,
		txBuffer: newTxCredits(NewPool(64, 1)),
		chDone:   make(chan struct{}),
		param:    make(evt.LEConnectionComplete, 19),
	}
	// sigCmd: code=DisconnectRequest, id=0x2A, len=4, DCID=cidLEAtt, SCID=cidLEAtt.
	sig := []byte{SignalDisconnectRequest, 0x2A, 0x04, 0x00, 0x04, 0x00, 0x04, 0x00}
	if err := c.handleSignal(mkpdu(len(sig), cidLESignal, sig)); err != nil {
		t.Fatalf("handleSignal = %v, want nil", err)
	}
	w := skt.written()
	if len(w) != 1 {
		t.Fatalf("wrote %d packets, want 1 disconnect response", len(w))
	}
}

// TestHandleSMP: any known SMP opcode answers with Pairing Failed via
// sendSMP; with debug logging on, both smp debug dumps run. The closed
// connection makes the underlying write fail, which sendSMP must return.
// The pdu is a full L2CAP frame, as recombine delivers it off the wire.
func TestHandleSMP(t *testing.T) {
	debugLogger(t)
	c := &Conn{
		txBuffer: newTxCredits(NewPool(32, 1)),
		chDone:   closedDone(),
	}
	req := smpFrame([]byte{pairingRequest, 0x04, 0x00, 0x01, 0x10, 0x00, 0x00})
	if err := c.handleSMP(req); err != ErrClosed {
		t.Fatalf("handleSMP = %v, want ErrClosed from the failed send", err)
	}
}

// TestHandleSMPReservedCode: reserved opcodes are ignored per spec. The
// payload length (3) collides with a valid opcode (pairingConfirm), so this
// also pins that classification uses the opcode byte, not the length byte.
func TestHandleSMPReservedCode(t *testing.T) {
	c := &Conn{}
	if err := c.handleSMP(smpFrame([]byte{0xF0, 0x00, 0x00})); err != nil {
		t.Fatalf("handleSMP reserved code = %v, want nil", err)
	}
}

// TestHandleSMPSendSuccess drives sendSMP through a successful write so the
// smp send debug dump runs on the success path too, and checks the exact
// bytes of the Pairing Failed frame put on the wire.
func TestHandleSMPSendSuccess(t *testing.T) {
	debugLogger(t)
	skt := newFakeSkt()
	c := &Conn{
		hci:      &HCI{skt: skt},
		txBuffer: newTxCredits(NewPool(64, 1)),
		chDone:   make(chan struct{}),
		param:    make(evt.LEConnectionComplete, 19),
	}
	req := smpFrame([]byte{pairingRequest, 0x04, 0x00, 0x01, 0x10, 0x00, 0x00})
	if err := c.handleSMP(req); err != nil {
		t.Fatalf("handleSMP = %v, want nil", err)
	}
	assertPairingFailed(t, skt.written())
}
