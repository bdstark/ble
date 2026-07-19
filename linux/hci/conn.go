package hci

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/linux/hci/cmd"
	"github.com/go-ble/ble/linux/hci/evt"
)

// ACLWriteTimeout bounds how long a write waits for ACL buffer
// credits from the controller. Credits are only returned by
// NumberOfCompletedPackets events; a connection that died without a
// processed disconnect event never returns them, and unbounded waits
// park the writer goroutine forever (observed in the field: a
// write-without-response blocked for five hours). Long enough that a
// merely congested link never trips it.
var ACLWriteTimeout = 20 * time.Second

// Conn ...
type Conn struct {
	hci *HCI
	ctx context.Context

	param evt.LEConnectionComplete

	// While MTU is the maximum size of payload data that the upper layer (ATT)
	// can accept, the MPS is the maximum PDU payload size this L2CAP implementation
	// supports. When segmantation is not used, the MPS should be made to the same
	// values of MTUs [Vol 3, Part A, 1.4].
	//
	// For LE-U logical transport, the L2CAP implementations should support
	// a minimum of 23 bytes, which are also the default values before the
	// upper layer (ATT) optionally reconfigures them [Vol 3, Part A, 3.2.8].
	rxMTU int
	txMTU int
	rxMPS int

	// Signaling MTUs are The maximum size of command information that the
	// L2CAP layer entity is capable of accepting.
	// A L2CAP implementations supporting LE-U should support at least 23 bytes.
	// Currently, we support 512 bytes, which should be more than sufficient.
	// The sigTxMTU is discovered via when we sent a signaling pkt that is
	// larger thean the remote device can handle, and get a response of "Command
	// Reject" indicating "Signaling MTU exceeded" along with the actual
	// signaling MTU [Vol 3, Part A, 4.1].
	sigRxMTU int
	sigTxMTU int

	sigSent chan []byte
	// smpSent chan []byte

	chInPkt chan packet
	chInPDU chan pdu

	chDone chan struct{}

	// closeOnce guards the closes of chInPkt and chDone (see closeChans):
	// teardown can be initiated by a disconnect event, by whole-adapter
	// cleanup after socket death, or (indirectly, via kill) by a wedged
	// consumer or a corrupt inbound stream — any interleaving must close
	// each channel exactly once.
	closeOnce sync.Once

	// killOnce bounds teardown initiation (see kill): repeated triggers —
	// e.g. one dropped ACL packet per would-block hit in handleACL — must
	// spawn exactly one Close goroutine.
	killOnce sync.Once

	// Host to Controller Data Flow Control pkt-based Data flow control for LE-U [Vol 2, Part E, 4.1.1]
	// chSentBufs tracks the HCI buffer occupied by this connection.
	txBuffer *txCredits

	// sigID is used to match responses with signaling requests.
	// The requesting device sets this field and the responding device uses the
	// same value in its response. Within each signalling channel a different
	// Identifier shall be used for each successive command. [Vol 3, Part A, 4]
	sigID uint8

	// leFrame is set to be true when the LE Credit based flow control is used.
	leFrame bool

	// muUpdate guards updateWaiter and serializes UpdateParams: at most one
	// LE Connection Update may be in flight per connection. updateWaiter is
	// the buffered (cap 1) channel a pending UpdateParams parks on; it is
	// non-nil only while an update waits, and only UpdateParams ever
	// registers or clears it. handleLEConnectionUpdateComplete (on sktLoop)
	// reads it under muUpdate and does a non-blocking send — never a close —
	// so a stale, duplicate, or slave-forwarded completion can neither block
	// sktLoop nor send on a closed channel. See UpdateParams for the full
	// interleaving analysis.
	muUpdate     sync.Mutex
	updateWaiter chan leConnUpdate

	// muDataLen guards dataLen, the last data-length maximums negotiated for
	// this connection. handleLEDataLengthChange (on sktLoop) writes it; the
	// DataLength getter reads it from arbitrary goroutines (telemetry), so the
	// mutex — not a bare field — keeps that race clean. Unlike updateWaiter
	// there is no per-call waiter: an LE Data Length Change event is
	// asynchronous, optional, and may be peer-initiated, so it is never
	// delivered to a specific SetDataLength call, only stored here.
	muDataLen sync.Mutex
	dataLen   dataLength
}

// dataLength holds the effective LE data-length maximums for a connection: the
// largest link-layer payload (and its air time) the local controller will use
// for transmit and receive on this link. Before any LE Data Length Change
// event it holds the BLE defaults (27 octets / 328 µs) [Vol 6, Part B, 4.5.10].
type dataLength struct {
	maxTxOctets uint16
	maxTxTime   uint16
	maxRxOctets uint16
	maxRxTime   uint16
}

// leConnUpdate carries the fields of an LE Connection Update Complete meta
// event from sktLoop to a waiting UpdateParams. It is a value copy (not the
// pooled/aliased event buffer) so its lifetime is independent of sktLoop.
type leConnUpdate struct {
	status   uint8
	interval uint16
	latency  uint16
	timeout  uint16
}

var (
	// ErrUpdateInProgress is returned by Conn.UpdateParams when an LE
	// Connection Update is already in flight on the same connection. Only one
	// update may be outstanding at a time; callers should serialize or retry.
	ErrUpdateInProgress = errors.New("hci: connection parameter update already in progress")

	// ErrConnUpdateTimeout is returned when the controller accepts the LE
	// Connection Update command (a command status) but never delivers the
	// LE Connection Update Complete meta event within connUpdateTimeout.
	ErrConnUpdateTimeout = errors.New("hci: timed out waiting for LE connection update completion")
)

// connUpdateTimeout bounds how long UpdateParams waits for the controller's
// LE Connection Update Complete meta event after the command is accepted.
// Longer than the maximum supervision timeout (32 s) so a legitimately slow
// update is not cut short; a package var so tests can lower it.
var connUpdateTimeout = 40 * time.Second

func newConn(h *HCI, param evt.LEConnectionComplete) *Conn {
	c := &Conn{
		hci:   h,
		ctx:   context.Background(),
		param: param,

		rxMTU: ble.DefaultMTU,
		txMTU: ble.DefaultMTU,

		rxMPS: ble.DefaultMTU,

		sigRxMTU: ble.MaxMTU,
		sigTxMTU: ble.DefaultMTU,

		chInPkt: make(chan packet, 16),
		chInPDU: make(chan pdu, 16),

		txBuffer: newTxCredits(h.pool),

		chDone: make(chan struct{}),

		// Default LE data length before any negotiation [Vol 6, Part B, 4.5.10].
		dataLen: dataLength{
			maxTxOctets: ble.DataLengthMinTxOctets,
			maxTxTime:   ble.DataLengthMinTxTime,
			maxRxOctets: ble.DataLengthMinTxOctets,
			maxRxTime:   ble.DataLengthMinTxTime,
		},
	}

	go func() {
		for {
			err := c.recombine()
			if err == nil {
				continue
			}
			// io.EOF is the clean exit (chInPkt closed by closeChans);
			// ErrClosed means teardown landed while a PDU delivery was
			// blocked on a reader that had already stopped. Anything
			// else — oversized fragment, missing continuation flag —
			// means the inbound L2CAP stream is corrupt and unrecoverable,
			// so tear the whole connection down rather than go silent
			// while handleACL keeps feeding a stream nobody can parse.
			// (Log before the close below: the close is what observers
			// wait on, so anything sequenced after it may outrun them.)
			if err != io.EOF && !errors.Is(err, ErrClosed) {
				ble.Logger.Error("recombine failed, closing connection", "err", err)
				c.kill()
			}
			// Unblock current and future Reads: once chInPDU is closed,
			// Read returns ErrClosed (wrapping io.ErrClosedPipe).
			close(c.chInPDU)
			// Keep draining chInPkt until closeChans closes it (the
			// disconnect event answering kill's Disconnect, or
			// HCI.cleanupConns on adapter death), so inbound ACL packets
			// cannot back up — or trip handleACL's drop-and-kill path —
			// in the interim. This goroutine is chInPkt's only consumer,
			// so the drain cannot race another receive. On the io.EOF
			// and ErrClosed paths chInPkt is already closed (closeChans
			// closes it before chDone) and the loop exits immediately.
			for range c.chInPkt {
			}
			return
		}
	}()
	return c
}

// Context returns the context that is used by this Conn.
func (c *Conn) Context() context.Context {
	return c.ctx
}

// SetContext sets the context that is used by this Conn.
func (c *Conn) SetContext(ctx context.Context) {
	c.ctx = ctx
}

// Read copies re-assembled L2CAP PDUs into sdu.
func (c *Conn) Read(sdu []byte) (n int, err error) {
	p, ok := <-c.chInPDU
	if !ok {
		return 0, fmt.Errorf("input channel closed: %w", ErrClosed)
	}
	if len(p) == 0 {
		return 0, fmt.Errorf("received empty packet: %w", io.ErrUnexpectedEOF)
	}

	// Assume it's a B-Frame.
	slen := p.dlen()
	data := p.payload()
	if c.leFrame {
		// LE-Frame.
		slen = leFrameHdr(p).slen()
		data = leFrameHdr(p).payload()
	}
	if cap(sdu) < slen {
		return 0, fmt.Errorf("payload received exceeds sdu buffer: %w", io.ErrShortBuffer)
	}
	buf := bytes.NewBuffer(sdu)
	buf.Reset()
	buf.Write(data)
	for buf.Len() < slen {
		p, ok := <-c.chInPDU
		if !ok {
			// Disconnect closed chInPDU mid-reassembly. Without the ok
			// check p is a nil pdu and p.payload() panics — a crash that
			// took down the whole unattended gateway when a link died
			// between fragments of a segmented SDU.
			return 0, fmt.Errorf("input channel closed during reassembly: %w", ErrClosed)
		}
		buf.Write(p.payload())
	}
	return slen, nil
}

// Write breaks down a L2CAP SDU into segmants [Vol 3, Part A, 7.3.1]
func (c *Conn) Write(sdu []byte) (int, error) {
	if len(sdu) > c.txMTU {
		return 0, fmt.Errorf("payload exceeds mtu: %w", io.ErrShortWrite)
	}

	plen := len(sdu)
	if plen > c.txMTU {
		plen = c.txMTU
	}
	b := make([]byte, 4+plen)
	binary.LittleEndian.PutUint16(b[0:2], uint16(len(sdu)))
	binary.LittleEndian.PutUint16(b[2:4], cidLEAtt)
	if c.leFrame {
		binary.LittleEndian.PutUint16(b[4:6], uint16(len(sdu)))
		copy(b[6:], sdu)
	} else {
		copy(b[4:], sdu)
	}
	sent, err := c.writePDU(b)
	if err != nil {
		return sent, err
	}
	sdu = sdu[plen:]

	for len(sdu) > 0 {
		plen := len(sdu)
		if plen > c.txMTU {
			plen = c.txMTU
		}
		n, err := c.writePDU(sdu[:plen])
		sent += n
		if err != nil {
			return sent, err
		}
		sdu = sdu[plen:]
	}
	return sent, nil
}

// writePDU breaks down a L2CAP PDU into fragments if it's larger than the HCI buffer size. [Vol 3, Part A, 7.2.1]
func (c *Conn) writePDU(pdu []byte) (int, error) {
	sent := 0
	flags := uint16(pbfHostToControllerStart << 4) // ACL boundary flags

	// All L2CAP fragments associated with an L2CAP PDU shall be processed for
	// transmission by the Controller before any other L2CAP PDU for the same
	// logical transport shall be processed.
	c.txBuffer.lock()
	defer c.txBuffer.unlock()

	// Fail immediately if the connection is already closed
	// Check this with the pool locked to avoid race conditions
	// with handleDisconnectionComplete
	select {
	case <-c.chDone:
		return 0, ErrClosed
	default:
	}

	for len(pdu) > 0 {
		// Get a buffer from our pre-allocated and flow-controlled pool.
		// Bounded: credits come back only via NumberOfCompletedPackets,
		// which a dead link never sends.
		pkt, err := c.txBuffer.GetTimeout(c.chDone, ACLWriteTimeout) // ACL pkt
		if err != nil {
			return sent, err
		}
		flen := len(pdu) // fragment length
		if flen > pkt.Cap()-1-4 {
			flen = pkt.Cap() - 1 - 4
		}

		// HCI ACL Data header + fragment payload. Assembled with direct byte
		// writes: binary.Write pays a reflection pass per call, four times
		// per fragment on this path.
		writeACLData(pkt, c.param.ConnectionHandle()|(flags<<8), pdu[:flen])

		// Flush the pkt to HCI
		select {
		case <-c.chDone:
			return 0, ErrClosed
		default:
		}

		if _, err := c.hci.skt.Write(pkt.Bytes()); err != nil {
			return sent, err
		}
		sent += flen

		flags = (pbfContinuing << 4) // Set "continuing" in the boundary flags for the rest of fragments, if any.
		pdu = pdu[flen:]             // Advence the point
	}
	return sent, nil
}

// writeACLData appends one HCI ACL Data packet to pkt [Vol 2, Part E, 5.4.2]:
// packet type (1 byte), handle|flags (2 bytes LE), data length (2 bytes LE),
// then the payload. bytes.Buffer writes never fail, so no error is returned.
func writeACLData(pkt *bytes.Buffer, handleFlags uint16, payload []byte) {
	var hdr [5]byte
	hdr[0] = pktTypeACLData
	binary.LittleEndian.PutUint16(hdr[1:3], handleFlags)
	binary.LittleEndian.PutUint16(hdr[3:5], uint16(len(payload)))
	pkt.Write(hdr[:])
	pkt.Write(payload)
}

// Recombines fragments into a L2CAP PDU. [Vol 3, Part A, 7.2.2]
func (c *Conn) recombine() error {
	pkt, ok := <-c.chInPkt
	if !ok {
		return io.EOF
	}

	p := pdu(pkt.data())

	// Currently, check for LE-U only. For channels that we don't recognizes,
	// re-combine them anyway, and discard them later when we dispatch the PDU
	// according to CID.
	if p.cid() == cidLEAtt && p.dlen() > c.rxMPS {
		return fmt.Errorf("fragment size (%d) larger than rxMPS (%d)", p.dlen(), c.rxMPS)
	}

	// If this pkt is not a complete PDU, and we'll be receiving more
	// fragments, re-allocate the whole PDU (including Header).
	if len(p.payload()) < p.dlen() {
		p = make([]byte, 0, 4+p.dlen())
		p = append(p, pdu(pkt.data())...)
	}
	for len(p) < 4+p.dlen() {
		if pkt, ok = <-c.chInPkt; !ok {
			// Teardown closed chInPkt mid-reassembly: a disconnect, not a
			// framing error from the peer. Report the clean-shutdown error
			// so the recombine wrapper doesn't log corruption and re-kill.
			return io.EOF
		} else if (pkt.pbf() & pbfContinuing) == 0 {
			return io.ErrUnexpectedEOF
		}
		p = append(p, pdu(pkt.data())...)
	}

	// TODO: support dynamic or assigned channels for LE-Frames.
	switch p.cid() {
	case cidLEAtt:
		// Bounded by the connection's lifetime: if the application stops
		// calling Read, chInPDU fills and this send blocks. chDone
		// unblocks it on teardown; a bare send would leak the recombine
		// goroutine (parked here forever) every time a connection died
		// with an unread PDU in flight.
		select {
		case c.chInPDU <- p:
		case <-c.chDone:
			return fmt.Errorf("connection closed while delivering PDU: %w", ErrClosed)
		}
	case cidLESignal:
		c.handleSignal(p)
	case cidSMP:
		c.handleSMP(p)
	default:
		// Abnormal path (unknown channel); eager formatting is acceptable here.
		ble.Logger.Info("recombine: unrecognized CID", "cid", fmt.Sprintf("%04X", p.cid()), "pdu", fmt.Sprintf("[%X]", p))
	}
	return nil
}

// Disconnected returns a receiving channel, which is closed when the connection disconnects.
func (c *Conn) Disconnected() <-chan struct{} {
	return c.chDone
}

// ReadRSSI retrieves the current RSSI value of the remote peripheral, in
// dBm. [Vol 2, Part E, 7.5.4] The exchange is bounded by the HCI command
// timeout. Send surfaces a non-zero command status as ErrCommand before
// unmarshaling the return parameters (the codebase-wide status check), so
// rp is only consulted on success and a failure can never be mistaken for
// a zero-dBm reading.
func (c *Conn) ReadRSSI() (int, error) {
	rp := new(cmd.ReadRSSIRP)
	if err := c.hci.Send(&cmd.ReadRSSI{
		Handle: c.param.ConnectionHandle(),
	}, rp); err != nil {
		return 0, fmt.Errorf("hci: read RSSI: %w", err)
	}
	return int(rp.RSSI), nil
}

// UpdateParams issues an LE Connection Update (0x08|0x0013) on this central
// link and blocks until the controller reports it complete. The daemon uses
// this to connect fast (a short interval for quick GATT discovery) then relax
// each of its concurrent links to reduce 2.4 GHz contention.
//
// p is validated and converted by ble.ConnParams.Encode; an out-of-range
// field returns ErrInvalidConnParams (wrapped) without touching the
// controller. c.hci.Send returns once the controller acknowledges the command
// with a *command status* — the command was accepted, not that the parameters
// changed. The actual result arrives LATER as an LE Connection Update Complete
// meta event, delivered by sktLoop → handleLEConnectionUpdateComplete →
// deliverConnUpdate to the waiter registered below. A non-zero status in that
// event is returned as the matching ErrCommand.
//
// Only one update may be in flight per connection: a second concurrent call
// returns ErrUpdateInProgress rather than racing the first for the single
// completion event.
//
// Concurrency & teardown. muUpdate serializes registration and guards
// updateWaiter; chDone is closed exactly once by closeChans on teardown. The
// waiter channel is created fresh here, is buffered (cap 1), and is never
// closed — deliverConnUpdate only ever does a non-blocking send — so no
// interleaving can send on a closed channel or block sktLoop. The relevant
// interleavings:
//
//  1. Normal: register waiter, Send accepted, meta event → deliverConnUpdate
//     sends the result, the select below receives it, defer clears the waiter.
//  2. ctx cancel / timeout mid-wait: the select returns ctx.Err() /
//     ErrConnUpdateTimeout and defer clears the waiter. A late meta event
//     finds the waiter either still registered (buffered send, then cleared
//     and GC'd) or already nil (skipped) — both harmless.
//  3. Disconnect mid-wait: closeChans closes chDone (the select's teardown
//     arm fires, ErrClosed) and handleDisconnectionComplete removes the conn
//     from h.conns; both run on sktLoop, so a later meta event's handle
//     lookup misses and is a no-op. Any meta event that landed first does a
//     buffered/dropped send to a waiter that has moved on — harmless.
//  4. No-waiter completion: a slave-forwarded update (signal.go, fire and
//     forget) or a completion racing teardown finds updateWaiter nil and is a
//     no-op — it never wedges sktLoop.
//  5. Teardown racing registration: the chDone check under muUpdate below
//     rejects a registration once teardown started; if chDone closes just
//     after, the select's teardown arm handles it.
//
// The LE Connection Update Complete event carries only the handle, not a
// command correlation ID, so in principle a completion that straggles in
// after call A times out and call B has since registered would be read by
// B as its own. This is inherent to the event and shared by every host
// stack; here it is effectively unreachable — connUpdateTimeout (40s)
// exceeds the 32s max supervision timeout, and ErrUpdateInProgress permits
// only one update per link at a time.
func (c *Conn) UpdateParams(ctx context.Context, p ble.ConnParams) error {
	imin, imax, latency, timeout, err := p.Encode()
	if err != nil {
		return err
	}

	ch := make(chan leConnUpdate, 1)
	c.muUpdate.Lock()
	if c.updateWaiter != nil {
		c.muUpdate.Unlock()
		return ErrUpdateInProgress
	}
	// Refuse to register on an already-torn-down link: without this the Send
	// below would fail with ErrClosed anyway, but checking here keeps the
	// waiter map honest and avoids a pointless command attempt.
	select {
	case <-c.chDone:
		c.muUpdate.Unlock()
		return ErrClosed
	default:
	}
	c.updateWaiter = ch
	c.muUpdate.Unlock()

	defer func() {
		c.muUpdate.Lock()
		c.updateWaiter = nil
		c.muUpdate.Unlock()
	}()

	// Send returns the command STATUS (command accepted), not the result.
	if err := c.hci.Send(&cmd.LEConnectionUpdate{
		ConnectionHandle:   c.param.ConnectionHandle(),
		ConnIntervalMin:    imin,
		ConnIntervalMax:    imax,
		ConnLatency:        latency,
		SupervisionTimeout: timeout,
		MinimumCELength:    0, // Informational; spec doesn't specify the use.
		MaximumCELength:    0, // Informational; spec doesn't specify the use.
	}, nil); err != nil {
		return fmt.Errorf("hci: LE connection update: %w", err)
	}

	select {
	case u := <-ch:
		if u.status != 0x00 {
			return fmt.Errorf("hci: LE connection update failed: %w", ErrCommand(u.status))
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.chDone:
		return ErrClosed
	case <-time.After(connUpdateTimeout):
		return fmt.Errorf("hci: LE connection update: %w", ErrConnUpdateTimeout)
	}
}

// deliverConnUpdate hands an LE Connection Update Complete result to a waiting
// UpdateParams, if one is registered. It runs on the sktLoop goroutine (via
// handleLEConnectionUpdateComplete) and must never block it: the send is
// non-blocking onto a cap-1 channel that is never closed, so a duplicate
// completion, a completion for a slave-forwarded update with no waiter, or one
// racing the waiter's own cleanup are all harmless.
func (c *Conn) deliverConnUpdate(u leConnUpdate) {
	c.muUpdate.Lock()
	ch := c.updateWaiter
	c.muUpdate.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- u:
	default:
	}
}

// SetDataLength issues an LE Set Data Length (0x08|0x0022) on this central
// link, asking the controller to use up to txOctets-octet link-layer payloads
// (txTime µs of air time) for this connection. The daemon uses this to cut the
// packet count — and thus 2.4 GHz airtime — of large-MTU GATT transfers as
// more devices share the adapter.
//
// txOctets/txTime are validated by ble.ValidateDataLength; an out-of-range
// value returns ErrInvalidDataLength (wrapped) without touching the controller.
//
// Design: unlike UpdateParams, this waits ONLY on the command's Command
// Complete, not on a meta event. LE Set Data Length returns a Command Complete
// carrying a status immediately (accepted/rejected); c.hci.Send surfaces a
// non-zero status as ErrCommand before it unmarshals the return parameters, so
// the returned error already distinguishes rejection from success. The
// negotiated length arrives LATER — if it changes at all — as an asynchronous
// LE Data Length Change event that may also be triggered by the peer, so it is
// not 1:1 with this call and must not be waited on here (that would hang
// whenever the effective length is unchanged). handleLEDataLengthChange records
// the result on the Conn for DataLength() to read. The command itself is
// bounded by the HCI command timeout inside Send; ctx only short-circuits an
// already-cancelled caller before the command is sent.
func (c *Conn) SetDataLength(ctx context.Context, txOctets, txTime uint16) error {
	if err := ble.ValidateDataLength(txOctets, txTime); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	rp := cmd.LESetDataLengthRP{}
	if err := c.hci.Send(&cmd.LESetDataLength{
		ConnectionHandle: c.param.ConnectionHandle(),
		TxOctets:         txOctets,
		TxTime:           txTime,
	}, &rp); err != nil {
		return fmt.Errorf("hci: LE set data length: %w", err)
	}
	return nil
}

// DataLength returns the effective LE data-length maximums last negotiated for
// this connection: the largest link-layer payload (octets) and its air time
// (µs) the controller will use to transmit and receive. Before any LE Data
// Length Change event it returns the BLE defaults (27 octets / 328 µs).
//
// It is safe to call from any goroutine (telemetry reads it while sktLoop
// writes it); the read is guarded by muDataLen. This is linux-concrete rather
// than on the ble.Conn interface because CoreBluetooth exposes no equivalent.
func (c *Conn) DataLength() (maxTxOctets, maxTxTime, maxRxOctets, maxRxTime uint16) {
	c.muDataLen.Lock()
	defer c.muDataLen.Unlock()
	return c.dataLen.maxTxOctets, c.dataLen.maxTxTime, c.dataLen.maxRxOctets, c.dataLen.maxRxTime
}

// setDataLength records the negotiated data-length maximums from an LE Data
// Length Change event. It runs on the sktLoop goroutine (via
// handleLEDataLengthChange) and only takes muDataLen for the brief store, so it
// never blocks sktLoop.
func (c *Conn) setDataLength(dl dataLength) {
	c.muDataLen.Lock()
	c.dataLen = dl
	c.muDataLen.Unlock()
}

// closeChans closes chInPkt and chDone, exactly once, in that order.
//
// Teardown of a connection can be initiated by any of:
//  1. handleDisconnectionComplete — a disconnect event, on the sktLoop
//     goroutine;
//  2. HCI.cleanupConns — socket death, on the sktLoop goroutine after its
//     read loop has exited;
//  3. Conn.Close / Conn.kill (handleACL overflow, recombine error) — these
//     only *send* a Disconnect command; the closes themselves still land via
//     1 (the resulting disconnect event) or 2 (if the adapter dies first).
//
// 1 and 2 cannot both reach the same conn: each takes the conn out of
// h.conns under muConns before closing, and both run on the sktLoop
// goroutine anyway (2 strictly after the last possible 1). The Once still
// matters — it makes every interleaving, including a caller-driven double
// teardown or a future third closer, panic-free by construction rather than
// by that reasoning.
//
// Ordering matters twice:
//   - chInPkt before chDone: anything that observes chDone closed (the
//     recombine delivery select, writePDU) may rely on chInPkt already
//     being closed, so the recombine goroutine's drain terminates.
//   - callers must remove the conn from h.conns (under muConns) before
//     calling closeChans, so handleACL — which runs on the same sktLoop
//     goroutine as both closers — can never look up a conn whose chInPkt
//     is closed and panic sending on it.
func (c *Conn) closeChans() {
	c.closeOnce.Do(func() {
		close(c.chInPkt)
		close(c.chDone)
	})
}

// kill initiates asynchronous teardown of the connection, at most once, and
// reports whether this call was the initiating one. The goroutine is
// mandatory, not a convenience: Close sends a Disconnect command whose
// completion event only sktLoop can process, so calling it synchronously
// from sktLoop's own call path (handleACL) would self-deadlock.
func (c *Conn) kill() bool {
	initiated := false
	c.killOnce.Do(func() {
		initiated = true
		go c.Close()
	})
	return initiated
}

// Close disconnects the connection by sending hci disconnect command to the device.
func (c *Conn) Close() error {
	select {
	case <-c.chDone:
		// Return if it's already closed.
		return nil
	default:
		c.hci.Send(&cmd.Disconnect{
			ConnectionHandle: c.param.ConnectionHandle(),
			Reason:           0x13,
		}, nil)
		return nil
	}
}

// LocalAddr returns local device's MAC address.
func (c *Conn) LocalAddr() ble.Addr { return c.hci.Addr() }

// RemoteAddr returns remote device's MAC address.
func (c *Conn) RemoteAddr() ble.Addr {
	a := c.param.PeerAddress()
	return net.HardwareAddr([]byte{a[5], a[4], a[3], a[2], a[1], a[0]})
}

// RxMTU returns the MTU which the upper layer is capable of accepting.
func (c *Conn) RxMTU() int { return c.rxMTU }

// SetRxMTU sets the MTU which the upper layer is capable of accepting.
func (c *Conn) SetRxMTU(mtu int) { c.rxMTU, c.rxMPS = mtu, mtu }

// TxMTU returns the MTU which the remote device is capable of accepting.
func (c *Conn) TxMTU() int { return c.txMTU }

// SetTxMTU sets the MTU which the remote device is capable of accepting.
func (c *Conn) SetTxMTU(mtu int) { c.txMTU = mtu }

// pkt implements HCI ACL Data Packet [Vol 2, Part E, 5.4.2]
// Packet boundary flags , bit[5:6] of handle field's MSB
// Broadcast flags. bit[7:8] of handle field's MSB
// Not used in LE-U. Leave it as 0x00 (Point-to-Point).
// Broadcasting in LE uses ADVB logical transport.
type packet []byte

func (a packet) handle() uint16 { return uint16(a[0]) | (uint16(a[1]&0x0f) << 8) }
func (a packet) pbf() int       { return (int(a[1]) >> 4) & 0x3 }
func (a packet) bcf() int       { return (int(a[1]) >> 6) & 0x3 }
func (a packet) dlen() int      { return int(a[2]) | (int(a[3]) << 8) }
func (a packet) data() []byte   { return a[4:] }

type pdu []byte

func (p pdu) dlen() int       { return int(binary.LittleEndian.Uint16(p[0:2])) }
func (p pdu) cid() uint16     { return binary.LittleEndian.Uint16(p[2:4]) }
func (p pdu) payload() []byte { return p[4:] }

type leFrameHdr pdu

func (f leFrameHdr) slen() int       { return int(binary.LittleEndian.Uint16(f[4:6])) }
func (f leFrameHdr) payload() []byte { return f[6:] }
