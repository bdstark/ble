package hci

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/linux/hci/cmd"
	"github.com/go-ble/ble/linux/hci/evt"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// dataLengthChangeWirePkt builds a full HCI LE Data Length Change event packet
// (as read off the socket, with the leading packet-type byte). Note the
// subevent carries NO status byte: the handle is at offset 1.
func dataLengthChangeWirePkt(handle, txOctets, txTime, rxOctets, rxTime uint16) []byte {
	return []byte{
		pktTypeEvent, leMetaCode,
		11,                              // parameter length
		evt.LEDataLengthChangeSubCode,   // subevent: LE Data Length Change (0x07)
		byte(handle), byte(handle >> 8), // ConnectionHandle
		byte(txOctets), byte(txOctets >> 8),
		byte(txTime), byte(txTime >> 8),
		byte(rxOctets), byte(rxOctets >> 8),
		byte(rxTime), byte(rxTime >> 8),
	}
}

// setDataLengthCompletePkt builds a CommandComplete carrying the full LE Set
// Data Length return parameters (status + connection handle, 3 bytes) so
// Send's rp Unmarshal has enough bytes. The shared cmdCompletePkt only carries
// a single status byte, which is fine for the error path (Send rejects on the
// status before unmarshaling) but not the success path.
func setDataLengthCompletePkt(opcode int, status byte, handle uint16) []byte {
	return []byte{
		pktTypeEvent, evt.CommandCompleteCode,
		6,                               // parameter length
		1,                               // NumHCICommandPackets
		byte(opcode), byte(opcode >> 8), // CommandOpcode
		status,                          // ReturnParameters: Status
		byte(handle), byte(handle >> 8), // ReturnParameters: ConnectionHandle
	}
}

// newDataLengthHCI is a looped HCI wired for the data-length paths: a buffer
// pool (newConn needs one), the LE-meta dispatch chain, the data-length
// subevent handler, and the disconnect handler (teardown races the change).
func newDataLengthHCI(t *testing.T, skt *fakeSkt) *HCI {
	t.Helper()
	return newLoopedHCIOn(t, skt, func(h *HCI) {
		h.pool = NewPool(32, 4)
		h.evth[leMetaCode] = h.handleLEMeta
		h.evth[evt.DisconnectionCompleteCode] = h.handleDisconnectionComplete
		h.subh[evt.LEDataLengthChangeSubCode] = h.handleLEDataLengthChange
	})
}

// waitDataLength polls DataLength() until maxTxOctets reaches want, or fails.
func waitDataLength(t *testing.T, c *Conn, want uint16) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if tx, _, _, _ := c.DataLength(); tx == want {
			return
		}
		if time.Now().After(deadline) {
			gotTx, _, _, _ := c.DataLength()
			t.Fatalf("DataLength maxTxOctets = %d, never reached %d", gotTx, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// ble.ValidateDataLength: range validation
// ---------------------------------------------------------------------------

func TestValidateDataLength(t *testing.T) {
	// Valid: the boundaries and a mid-range request.
	for _, ok := range []struct{ octets, time uint16 }{
		{27, 328},    // minimums
		{251, 17040}, // maximums ("the controller's ceiling")
		{251, 2120},  // typical max-octets request on 1M PHY
		{100, 1000},  // mid-range
	} {
		if err := ble.ValidateDataLength(ok.octets, ok.time); err != nil {
			t.Fatalf("ValidateDataLength(%d, %d) = %v, want nil", ok.octets, ok.time, err)
		}
	}
	// Invalid: each field just outside its range.
	for _, bad := range []struct {
		name         string
		octets, time uint16
	}{
		{"octets too small", 26, 1000},
		{"octets too large", 252, 1000},
		{"time too small", 100, 327},
		{"time too large", 100, 17041},
	} {
		t.Run(bad.name, func(t *testing.T) {
			if err := ble.ValidateDataLength(bad.octets, bad.time); !errors.Is(err, ble.ErrInvalidDataLength) {
				t.Fatalf("ValidateDataLength(%d, %d) = %v, want ErrInvalidDataLength", bad.octets, bad.time, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Codecs: hand-written command/return-parameter/event marshaling
// ---------------------------------------------------------------------------

// TestDataLengthCodecs exercises the hand-written codecs directly: opcodes,
// lengths, the LE Set Data Length marshaling, the LE Read Maximum Data Length
// return-parameter unmarshaling (the "discover the controller's ceiling"
// convenience), and the event accessors.
func TestDataLengthCodecs(t *testing.T) {
	// LE Set Data Length command: opcode, length, and the 6-byte little-endian
	// parameter layout handle/txOctets/txTime.
	sdl := &cmd.LESetDataLength{ConnectionHandle: 0x0040, TxOctets: 251, TxTime: 2120}
	if sdl.OpCode() != 0x08<<10|0x0022 {
		t.Fatalf("LESetDataLength OpCode = %#x, want %#x", sdl.OpCode(), 0x08<<10|0x0022)
	}
	if sdl.Len() != 6 {
		t.Fatalf("LESetDataLength Len = %d, want 6", sdl.Len())
	}
	if s := sdl.String(); s != "LE Set Data Length (0x08|0x0022)" {
		t.Fatalf("LESetDataLength String = %q", s)
	}
	buf := make([]byte, 64)
	if err := sdl.Marshal(buf); err != nil {
		t.Fatalf("LESetDataLength Marshal: %v", err)
	}
	if got := buf[:6]; binary.LittleEndian.Uint16(got[0:]) != 0x0040 ||
		binary.LittleEndian.Uint16(got[2:]) != 251 ||
		binary.LittleEndian.Uint16(got[4:]) != 2120 {
		t.Fatalf("LESetDataLength marshaled = % X, want handle 0x40 / 251 / 2120", got)
	}

	// LE Set Data Length return parameters: status + connection handle.
	sdlRP := &cmd.LESetDataLengthRP{}
	if err := sdlRP.Unmarshal([]byte{0x00, 0x40, 0x00}); err != nil {
		t.Fatalf("LESetDataLengthRP Unmarshal: %v", err)
	}
	if sdlRP.Status != 0x00 || sdlRP.ConnectionHandle != 0x0040 {
		t.Fatalf("LESetDataLengthRP = %+v, want {0 0x40}", sdlRP)
	}

	// LE Read Maximum Data Length command: no params, opcode/length.
	rmdl := &cmd.LEReadMaximumDataLength{}
	if rmdl.OpCode() != 0x08<<10|0x002F {
		t.Fatalf("LEReadMaximumDataLength OpCode = %#x, want %#x", rmdl.OpCode(), 0x08<<10|0x002F)
	}
	if rmdl.Len() != 0 {
		t.Fatalf("LEReadMaximumDataLength Len = %d, want 0", rmdl.Len())
	}
	if s := rmdl.String(); s != "LE Read Maximum Data Length (0x08|0x002F)" {
		t.Fatalf("LEReadMaximumDataLength String = %q", s)
	}
	if err := rmdl.Marshal(make([]byte, 64)); err != nil {
		t.Fatalf("LEReadMaximumDataLength Marshal: %v", err)
	}

	// LE Read Maximum Data Length return parameters: status + four uint16
	// ceilings, little-endian.
	rmdlRP := &cmd.LEReadMaximumDataLengthRP{}
	raw := []byte{
		0x00,       // status
		0xFB, 0x00, // SupportedMaxTxOctets 251
		0x48, 0x08, // SupportedMaxTxTime 2120
		0xFB, 0x00, // SupportedMaxRxOctets 251
		0x48, 0x08, // SupportedMaxRxTime 2120
	}
	if err := rmdlRP.Unmarshal(raw); err != nil {
		t.Fatalf("LEReadMaximumDataLengthRP Unmarshal: %v", err)
	}
	if rmdlRP.Status != 0 || rmdlRP.SupportedMaxTxOctets != 251 || rmdlRP.SupportedMaxTxTime != 2120 ||
		rmdlRP.SupportedMaxRxOctets != 251 || rmdlRP.SupportedMaxRxTime != 2120 {
		t.Fatalf("LEReadMaximumDataLengthRP = %+v, want {0 251 2120 251 2120}", rmdlRP)
	}

	// LE Data Length Change event accessors read from the subevent payload.
	e := evt.LEDataLengthChange(dataLengthChangeWirePkt(0x0040, 251, 2120, 100, 1000)[3:])
	if e.SubeventCode() != evt.LEDataLengthChangeSubCode {
		t.Fatalf("SubeventCode = %#x, want %#x", e.SubeventCode(), evt.LEDataLengthChangeSubCode)
	}
	if e.ConnectionHandle() != 0x0040 || e.MaxTxOctets() != 251 || e.MaxTxTime() != 2120 ||
		e.MaxRxOctets() != 100 || e.MaxRxTime() != 1000 {
		t.Fatalf("LEDataLengthChange accessors = (h %#x, %d, %d, %d, %d), want (0x40, 251, 2120, 100, 1000)",
			e.ConnectionHandle(), e.MaxTxOctets(), e.MaxTxTime(), e.MaxRxOctets(), e.MaxRxTime())
	}
}

// ---------------------------------------------------------------------------
// Conn.SetDataLength: happy path, the command on the wire, and errors
// ---------------------------------------------------------------------------

func TestSetDataLengthSuccess(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	sdlOp := (&cmd.LESetDataLength{}).OpCode()
	skt.onWrite = func(b []byte) {
		if len(b) == 0 || b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		if op == sdlOp {
			skt.rd <- setDataLengthCompletePkt(op, 0x00, 0x0040)
			return
		}
		skt.rd <- cmdCompletePkt(op, 0x00)
	}
	h := newDataLengthHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	if err := c.SetDataLength(context.Background(), 251, 2120); err != nil {
		t.Fatalf("SetDataLength: %v", err)
	}

	// The LE Set Data Length command reached the wire with the right fields.
	w := findCmd(skt, sdlOp)
	if w == nil {
		t.Fatal("no LE Set Data Length command on the wire")
	}
	params := w[4:]
	got := struct{ handle, txOctets, txTime uint16 }{
		binary.LittleEndian.Uint16(params[0:]),
		binary.LittleEndian.Uint16(params[2:]),
		binary.LittleEndian.Uint16(params[4:]),
	}
	if got.handle != 0x0040 || got.txOctets != 251 || got.txTime != 2120 {
		t.Fatalf("command params = %+v, want {handle 0x40 txOctets 251 txTime 2120}", got)
	}
}

// TestSetDataLengthCommandRejected: a non-zero command-complete status is
// surfaced as the matching ErrCommand (rejected, not accepted).
func TestSetDataLengthCommandRejected(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	sdlOp := (&cmd.LESetDataLength{}).OpCode()
	skt.onWrite = func(b []byte) {
		if len(b) == 0 || b[0] != pktTypeCommand {
			return
		}
		op := int(b[1]) | int(b[2])<<8
		status := byte(0x00)
		if op == sdlOp {
			status = 0x0C // Command Disallowed
		}
		skt.rd <- cmdCompletePkt(op, status)
	}
	h := newDataLengthHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	if err := c.SetDataLength(context.Background(), 251, 2120); !errors.Is(err, ErrDisallowed) {
		t.Fatalf("SetDataLength err = %v, want ErrDisallowed", err)
	}
}

// TestSetDataLengthInvalid: out-of-range params are rejected before any
// command is sent.
func TestSetDataLengthInvalid(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAllCommands(skt)
	h := newDataLengthHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	for _, bad := range []struct{ octets, time uint16 }{
		{26, 2120},  // octets too small
		{252, 2120}, // octets too large
		{251, 327},  // time too small
		{251, 17041},
	} {
		if err := c.SetDataLength(context.Background(), bad.octets, bad.time); !errors.Is(err, ble.ErrInvalidDataLength) {
			t.Fatalf("SetDataLength(%d, %d) err = %v, want ErrInvalidDataLength", bad.octets, bad.time, err)
		}
	}
	if w := findCmd(skt, (&cmd.LESetDataLength{}).OpCode()); w != nil {
		t.Fatal("invalid params still put an LE Set Data Length command on the wire")
	}
}

// TestSetDataLengthContextCancelled: an already-cancelled context short-circuits
// the call before any command is sent.
func TestSetDataLengthContextCancelled(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	ackAllCommands(skt)
	h := newDataLengthHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.SetDataLength(ctx, 251, 2120); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetDataLength err = %v, want context.Canceled", err)
	}
	if w := findCmd(skt, (&cmd.LESetDataLength{}).OpCode()); w != nil {
		t.Fatal("cancelled context still put an LE Set Data Length command on the wire")
	}
}

// ---------------------------------------------------------------------------
// Conn.DataLength / handleLEDataLengthChange
// ---------------------------------------------------------------------------

// TestDataLengthDefault: before any event the getter reports the BLE defaults.
func TestDataLengthDefault(t *testing.T) {
	h := &HCI{muConns: &sync.Mutex{}, conns: map[uint16]*Conn{}, pool: NewPool(32, 4)}
	c := addConnWithHandle(h, 0x0040)
	tx, txt, rx, rxt := c.DataLength()
	if tx != ble.DataLengthMinTxOctets || txt != ble.DataLengthMinTxTime ||
		rx != ble.DataLengthMinTxOctets || rxt != ble.DataLengthMinTxTime {
		t.Fatalf("DataLength default = (%d, %d, %d, %d), want (27, 328, 27, 328)", tx, txt, rx, rxt)
	}
}

// TestDataLengthChangeUpdatesGetter: an LE Data Length Change event fed over
// the socket updates what DataLength() reports.
func TestDataLengthChangeUpdatesGetter(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	h := newDataLengthHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	skt.rd <- dataLengthChangeWirePkt(0x0040, 251, 2120, 251, 2120)
	waitDataLength(t, c, 251)

	tx, txt, rx, rxt := c.DataLength()
	if tx != 251 || txt != 2120 || rx != 251 || rxt != 2120 {
		t.Fatalf("DataLength after change = (%d, %d, %d, %d), want (251, 2120, 251, 2120)", tx, txt, rx, rxt)
	}
}

// TestDataLengthChangeUnsolicited: a peer-initiated change (no preceding
// SetDataLength call) still updates the stored state — the event is not
// correlated with any call.
func TestDataLengthChangeUnsolicited(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	h := newDataLengthHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	// Asymmetric maximums, as a peer might negotiate.
	skt.rd <- dataLengthChangeWirePkt(0x0040, 100, 1000, 200, 2000)
	waitDataLength(t, c, 100)

	tx, txt, rx, rxt := c.DataLength()
	if tx != 100 || txt != 1000 || rx != 200 || rxt != 2000 {
		t.Fatalf("DataLength after unsolicited change = (%d, %d, %d, %d), want (100, 1000, 200, 2000)", tx, txt, rx, rxt)
	}
}

// TestDataLengthChangeUnknownHandle: a change for a handle with no conn (an
// event racing teardown) is a harmless no-op.
func TestDataLengthChangeUnknownHandle(t *testing.T) {
	debugLogger(t) // exercise the debug-gated log path
	h := &HCI{muConns: &sync.Mutex{}, conns: map[uint16]*Conn{}}

	payload := dataLengthChangeWirePkt(0x0099, 251, 2120, 251, 2120)[3:] // from the subevent code
	if err := h.handleLEDataLengthChange(payload); err != nil {
		t.Fatalf("handleLEDataLengthChange (unknown handle) = %v, want nil", err)
	}
}

// TestDataLengthChangeLoggedForKnownHandle: the debug-gated success log path is
// exercised for a registered conn (and the state still lands).
func TestDataLengthChangeLoggedForKnownHandle(t *testing.T) {
	debugLogger(t)
	h := &HCI{muConns: &sync.Mutex{}, conns: map[uint16]*Conn{}, pool: NewPool(32, 4)}
	c := addConnWithHandle(h, 0x0040)

	payload := dataLengthChangeWirePkt(0x0040, 251, 2120, 251, 2120)[3:]
	if err := h.handleLEDataLengthChange(payload); err != nil {
		t.Fatalf("handleLEDataLengthChange = %v, want nil", err)
	}
	if tx, _, _, _ := c.DataLength(); tx != 251 {
		t.Fatalf("DataLength maxTxOctets = %d, want 251", tx)
	}
}

// ---------------------------------------------------------------------------
// Race: data-length events fed while the getter is read (-race)
// ---------------------------------------------------------------------------

// TestDataLengthChangeGetterRace feeds LE Data Length Change events over the
// socket (sktLoop writes the conn state) while another goroutine reads
// DataLength(). Any interleaving must be race-free. Run with -race.
func TestDataLengthChangeGetterRace(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	h := newDataLengthHCI(t, skt)
	c := addConnWithHandle(h, 0x0040)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _, _, _ = c.DataLength()
			}
		}
	}()

	for i := 0; i < 200; i++ {
		octets := uint16(27 + i%224) // 27..250
		skt.rd <- dataLengthChangeWirePkt(0x0040, octets, 2120, octets, 2120)
	}
	waitDataLength(t, c, uint16(27+199%224))
	close(stop)
	wg.Wait()
}
