package hci

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
	"github.com/bdstark/ble/linux/hci/socket"
)

// Command ...
type Command interface {
	OpCode() int
	Len() int
	Marshal([]byte) error
}

// CommandRP ...
type CommandRP interface {
	Unmarshal(b []byte) error
}

type handlerFn func(b []byte) error

type pkt struct {
	cmd  Command
	done chan []byte
}

// NewHCI returns a hci device.
func NewHCI(opts ...ble.Option) (*HCI, error) {
	h := &HCI{
		id: -1,

		chCmdPkt:  make(chan *pkt),
		chCmdBufs: make(chan []byte, 16),
		sent:      make(map[int]*pkt),
		muSent:    &sync.Mutex{},

		evth: map[int]handlerFn{},
		subh: map[int]handlerFn{},

		conns:        make(map[uint16]*Conn),
		chMasterConn: make(chan *Conn),
		chSlaveConn:  make(chan *Conn),

		chAdv: make(chan *Advertisement, 64),

		done: make(chan bool),
	}
	h.params.init()
	if err := h.Option(opts...); err != nil {
		return nil, fmt.Errorf("can't set options: %w", err)
	}

	return h, nil
}

// HCI ...
type HCI struct {
	sync.Mutex

	params params

	skt io.ReadWriteCloser
	id  int

	// wgConnDisposal tracks the goroutines that Close undeliverable or
	// killed conns (dispose). Purely a join handle: production teardown
	// never blocks on it, but the tests join it between cases so a
	// disposal mid-Send cannot outlive its test and race the package's
	// tunable timeout vars.
	wgConnDisposal sync.WaitGroup

	// Host to Controller command flow control [Vol 2, Part E, 4.4]
	chCmdPkt  chan *pkt
	chCmdBufs chan []byte
	muSent    *sync.Mutex
	sent      map[int]*pkt

	// evtHub
	evth map[int]handlerFn
	subh map[int]handlerFn

	// aclHandler
	bufSize int
	bufCnt  int

	// Device information or status.
	addr    net.HardwareAddr
	txPwrLv int

	// adHist and adLast track the history of past scannable advertising packets.
	// Controller delivers AD(Advertising Data) and SR(Scan Response) separately
	// through HCI. Upon receiving an AD, no matter it's scannable or not, we
	// pass a Advertisement (AD only) to advHandler immediately.
	// Upon receiving a SR, we search the AD history for the AD from the same
	// device, and pass the Advertisiement (AD+SR) to advHandler.
	// The adHist and adLast are allocated in the Scan().
	// muAdHist guards adHist/adLast: handleLEAdvertisingReport reads and
	// writes them on sktLoop while Scan() — notably its Disallowed
	// reconciliation retries, which run precisely when the controller is
	// still streaming reports — reallocates both from the caller's
	// goroutine.
	advHandler ble.AdvHandler
	muAdHist   sync.Mutex
	adHist     []*Advertisement
	adLast     int

	// chAdv feeds the single adv dispatcher goroutine. Scanning is lossy by
	// nature: when the handler can't keep up the report is dropped rather
	// than blocking sktLoop or spawning a goroutine per report, but every
	// drop is counted (see AdvDropped).
	chAdv      chan *Advertisement
	advDropped atomic.Uint64

	// srOrphaned counts scan-response reports that matched no AD in the
	// scan history (see handleLEAdvertisingReport); each one is dropped
	// without disturbing the other reports in the same HCI event.
	srOrphaned atomic.Uint64

	// aclDropped counts inbound ACL data packets dropped because the
	// owning connection's chInPkt was full (see handleACL); each drop
	// coincides with that connection being torn down.
	aclDropped atomic.Uint64

	// Host to Controller Data Flow Control Packet-based Data flow control for LE-U [Vol 2, Part E, 4.1.1]
	// Minimum 27 bytes. 4 bytes of L2CAP Header, and 23 bytes Payload from upper layer (ATT)
	pool *Pool

	// L2CAP connections
	muConns      sync.Mutex
	conns        map[uint16]*Conn
	chMasterConn chan *Conn // Dial returns master connections.
	chSlaveConn  chan *Conn // Peripheral accept slave connections.

	connectedHandler    func(evt.LEConnectionComplete)
	disconnectedHandler func(evt.DisconnectionComplete)

	dialerTmo   time.Duration
	listenerTmo time.Duration

	// err records the fatal transport error, guarded by muErr: sktLoop,
	// close, and send all touch it from different goroutines. <-h.done
	// does NOT make lock-free reads safe: close() is reachable after done
	// is already closed (e.g. a send() write failure racing shutdown), so
	// all reads go through Error().
	muErr sync.Mutex
	err   error
	done  chan bool
}

// Init ...
func (h *HCI) Init() error {
	h.evth[0x3E] = h.handleLEMeta
	h.evth[evt.CommandCompleteCode] = h.handleCommandComplete
	h.evth[evt.CommandStatusCode] = h.handleCommandStatus
	h.evth[evt.DisconnectionCompleteCode] = h.handleDisconnectionComplete
	h.evth[evt.NumberOfCompletedPacketsCode] = h.handleNumberOfCompletedPackets

	h.subh[evt.LEAdvertisingReportSubCode] = h.handleLEAdvertisingReport
	h.subh[evt.LEConnectionCompleteSubCode] = h.handleLEConnectionComplete
	h.subh[evt.LEConnectionUpdateCompleteSubCode] = h.handleLEConnectionUpdateComplete
	h.subh[evt.LEDataLengthChangeSubCode] = h.handleLEDataLengthChange
	h.subh[evt.LELongTermKeyRequestSubCode] = h.handleLELongTermKeyRequest
	// evt.EncryptionChangeCode:                     todo),
	// evt.ReadRemoteVersionInformationCompleteCode: todo),
	// evt.HardwareErrorCode:                        todo),
	// evt.DataBufferOverflowCode:                   todo),
	// evt.EncryptionKeyRefreshCompleteCode:         todo),
	// evt.AuthenticatedPayloadTimeoutExpiredCode:   todo),
	// evt.LEReadRemoteUsedFeaturesCompleteSubCode:   todo),
	// evt.LERemoteConnectionParameterRequestSubCode: todo),

	skt, err := socket.NewSocket(h.id)
	if err != nil {
		return err
	}
	h.skt = skt

	h.setAllowedCommands(1)

	go h.sktLoop()
	go h.advDispatcher()
	if err := h.init(); err != nil {
		return err
	}

	// Pre-allocate buffers with additional head room for lower layer headers.
	// HCI header (1 Byte) + ACL Data Header (4 bytes) + L2CAP PDU (or fragment)
	h.pool = NewPool(1+4+h.bufSize, h.bufCnt-1)

	h.Send(&h.params.advParams, nil)
	h.Send(&h.params.scanParams, nil)
	return nil
}

// Close ...
func (h *HCI) Close() error {
	return h.close(nil)
}

// Done ...
func (h *HCI) Done() <-chan bool {
	return h.done
}

// Error ...
func (h *HCI) Error() error {
	h.muErr.Lock()
	defer h.muErr.Unlock()
	return h.err
}

// setErr records err as the fatal HCI error, replacing any previous one.
func (h *HCI) setErr(err error) {
	h.muErr.Lock()
	h.err = err
	h.muErr.Unlock()
}

// setErrIfAbsent records err only when no fatal error is recorded yet, so
// the socket-teardown error that follows a close(err) cannot clobber the
// root cause.
func (h *HCI) setErrIfAbsent(err error) {
	h.muErr.Lock()
	if h.err == nil {
		h.err = err
	}
	h.muErr.Unlock()
}

// Option sets the options specified.
// Option applies every option and reports all failures joined, so an
// option that succeeds after a failing one no longer masks the error.
func (h *HCI) Option(opts ...ble.Option) error {
	return ble.ApplyOptions(h, opts...)
}

func (h *HCI) init() error {
	// Every command here establishes prerequisite state for a usable HCI,
	// so each failure — including a completion timeout, which is
	// survivable elsewhere because callers retry — must fail Init instead
	// of silently leaving zero-valued controller parameters behind.
	// bufCnt in particular feeds NewPool(sz, bufCnt-1) in Init: with the
	// error swallowed, a timed-out ReadBufferSize left bufCnt at 0 and
	// Init panicked on a negative channel capacity.
	if err := h.Send(&cmd.Reset{}, nil); err != nil {
		return fmt.Errorf("hci init: reset: %w", err)
	}

	ReadBDADDRRP := cmd.ReadBDADDRRP{}
	if err := h.Send(&cmd.ReadBDADDR{}, &ReadBDADDRRP); err != nil {
		return fmt.Errorf("hci init: read BD_ADDR: %w", err)
	}

	a := ReadBDADDRRP.BDADDR
	h.addr = net.HardwareAddr([]byte{a[5], a[4], a[3], a[2], a[1], a[0]})

	ReadBufferSizeRP := cmd.ReadBufferSizeRP{}
	if err := h.Send(&cmd.ReadBufferSize{}, &ReadBufferSizeRP); err != nil {
		return fmt.Errorf("hci init: read buffer size: %w", err)
	}

	// Assume the buffers are shared between ACL-U and LE-U.
	h.bufCnt = int(ReadBufferSizeRP.HCTotalNumACLDataPackets)
	h.bufSize = int(ReadBufferSizeRP.HCACLDataPacketLength)

	LEReadBufferSizeRP := cmd.LEReadBufferSizeRP{}
	if err := h.Send(&cmd.LEReadBufferSize{}, &LEReadBufferSizeRP); err != nil {
		return fmt.Errorf("hci init: LE read buffer size: %w", err)
	}

	if LEReadBufferSizeRP.HCTotalNumLEDataPackets != 0 {
		// Okay, LE-U do have their own buffers.
		h.bufCnt = int(LEReadBufferSizeRP.HCTotalNumLEDataPackets)
		h.bufSize = int(LEReadBufferSizeRP.HCLEDataPacketLength)
	}

	LEReadAdvertisingChannelTxPowerRP := cmd.LEReadAdvertisingChannelTxPowerRP{}
	if err := h.Send(&cmd.LEReadAdvertisingChannelTxPower{}, &LEReadAdvertisingChannelTxPowerRP); err != nil {
		return fmt.Errorf("hci init: LE read advertising tx power: %w", err)
	}

	h.txPwrLv = int(LEReadAdvertisingChannelTxPowerRP.TransmitPowerLevel)

	LESetEventMaskRP := cmd.LESetEventMaskRP{}
	if err := h.Send(&cmd.LESetEventMask{LEEventMask: 0x000000000000001F}, &LESetEventMaskRP); err != nil {
		return fmt.Errorf("hci init: LE set event mask: %w", err)
	}

	SetEventMaskRP := cmd.SetEventMaskRP{}
	if err := h.Send(&cmd.SetEventMask{EventMask: 0x3dbff807fffbffff}, &SetEventMaskRP); err != nil {
		return fmt.Errorf("hci init: set event mask: %w", err)
	}

	WriteLEHostSupportRP := cmd.WriteLEHostSupportRP{}
	if err := h.Send(&cmd.WriteLEHostSupport{LESupportedHost: 1, SimultaneousLEHost: 0}, &WriteLEHostSupportRP); err != nil {
		return fmt.Errorf("hci init: write LE host support: %w", err)
	}

	// A controller that answered everything but reported no usable ACL
	// buffer geometry still cannot carry traffic — Init sizes the TX pool
	// as NewPool(..., bufCnt-1), so bufCnt must leave at least one credit
	// (bufCnt of 0 panicked the pool construction on a negative capacity).
	if h.bufCnt < 2 || h.bufSize < 1 {
		return fmt.Errorf("hci init: controller reported unusable ACL buffer geometry (%d buffers x %d bytes)", h.bufCnt, h.bufSize)
	}

	return h.Error()
}

// Send ...
func (h *HCI) Send(c Command, r CommandRP) error {
	// Only allow one send after another to prevent race condition
	h.Mutex.Lock()
	b, err := h.send(c)
	h.Mutex.Unlock()
	if err != nil {
		return err
	}
	if len(b) > 0 && b[0] != 0x00 {
		return ErrCommand(b[0])
	}
	if r != nil {
		return r.Unmarshal(b)
	}
	return nil
}

// cmdTimeout bounds both waits in the command path: acquiring a command
// buffer and waiting for the controller's completion event. Commands
// normally complete in milliseconds. The two timeouts differ in severity:
// running out of command buffers means the controller stopped returning
// credits entirely, so send closes the HCI; a missed completion is
// survivable (under multi-link RF load a stall past cmdTimeout with the
// completion straggling in late is a few-times-a-day event) and send just
// returns ErrCommandTimeout for the caller to retry. The abandoned command
// may still have executed, leaving controller state the host never
// recorded; the state-bearing commands reconcile that divergence when the
// controller answers a retry with Command Disallowed — see Scan,
// StopScanning, and Dial in gap.go.
var cmdTimeout = 10 * time.Second

func (h *HCI) send(c Command) ([]byte, error) {
	if err := h.Error(); err != nil {
		return nil, err
	}
	// done is buffered so a completion event that races with our
	// cmdTimeout below (the handler finds this entry in h.sent after we
	// stopped listening) can be parked in the buffer instead of blocking
	// sktLoop forever.
	p := &pkt{c, make(chan []byte, 1)}
	// Bounded: command buffers are returned by CommandComplete/Status
	// events, which a wedged controller stops sending — a bare receive
	// here parks the caller (including CancelConnection, the usual
	// recovery path) forever.
	var b []byte
	select {
	case b = <-h.chCmdBufs:
	case <-h.done:
		if err := h.Error(); err != nil {
			return nil, err
		}
		return nil, ErrClosed
	case <-time.After(cmdTimeout):
		err := fmt.Errorf("no command buffer available (controller not completing commands): %w", ErrCommandTimeout)
		h.close(err)
		return nil, err
	}
	b[0] = byte(pktTypeCommand) // HCI header
	b[1] = byte(c.OpCode())
	b[2] = byte(c.OpCode() >> 8)
	b[3] = byte(c.Len())
	if err := c.Marshal(b[4:]); err != nil {
		err = fmt.Errorf("hci: failed to marshal cmd: %w", err)
		h.close(err)
		return nil, err
	}

	h.muSent.Lock()
	h.sent[c.OpCode()] = p
	h.muSent.Unlock()
	if n, err := h.skt.Write(b[:4+c.Len()]); err != nil || n != 4+c.Len() {
		if err != nil {
			err = fmt.Errorf("hci: failed to send cmd: %w", err)
		} else {
			err = fmt.Errorf("hci: failed to send whole cmd pkt to hci socket")
		}
		h.close(err)
		// The command never (fully) went out, so nothing will complete
		// it — fail now instead of parking on p.done for cmdTimeout.
		h.muSent.Lock()
		delete(h.sent, c.OpCode())
		h.muSent.Unlock()
		return nil, err
	}

	var ret []byte
	var err error

	// emergency timeout to prevent calls from locking up if the HCI
	// interface doesn't respond.  Responsed here should normally be fast
	// a timeout indicates a major problem with HCI.
	timeout := time.NewTimer(cmdTimeout)
	select {
	case <-timeout.C:
		err = fmt.Errorf("no response to command, hci connection failed: %w", ErrCommandTimeout)
		ret = nil
	case <-h.done:
		err = h.Error()
		ret = nil
	case b := <-p.done:
		err = nil
		ret = b
	}
	timeout.Stop()

	// clear sent table when done, we sometimes get command complete or
	// command status messages with no matching send, which can attempt to
	// access stale packets in sent and fail or lock up.
	h.muSent.Lock()
	delete(h.sent, c.OpCode())
	h.muSent.Unlock()

	return ret, err
}

// evtBufSize is the largest possible HCI event packet: 1 byte packet type,
// 2 bytes event header, up to 255 bytes of parameters.
const evtBufSize = 1 + 2 + 255

// evtPool recycles buffers for the HCI event packets whose handlers provably
// release them before handlePkt returns (see poolableEvtPkt).
var evtPool = sync.Pool{New: func() interface{} {
	b := make([]byte, evtBufSize)
	return &b
}}

// poolableEvtPkt reports whether the raw HCI packet is an event whose handler
// provably releases the buffer before handlePkt returns:
//   - CommandComplete: handleCommandComplete copies ReturnParameters before
//     handing them to the waiting sender.
//   - CommandStatus: only fixed fields are read; the status byte sent onward
//     is a fresh allocation.
//   - NumberOfCompletedPackets: only fixed fields are read. This is the hot
//     one — the controller sends it continuously to return ACL credits
//     during data transfer.
//
// Everything else escapes with a consumer-determined lifetime and must keep
// its own allocation: ACL data aliases into conn.chInPkt/chInPDU and is only
// released when (or after) Conn.Read consumes the PDU; LE advertising
// reports are retained by Advertisement (adHist and the adv dispatcher);
// connection complete/disconnect events are retained in Conn.param and
// handed to user callbacks.
func poolableEvtPkt(b []byte) bool {
	if len(b) < 2 || len(b) > evtBufSize || b[0] != pktTypeEvent {
		return false
	}
	switch int(b[1]) {
	case evt.CommandCompleteCode, evt.CommandStatusCode, evt.NumberOfCompletedPacketsCode:
		return true
	}
	return false
}

func (h *HCI) sktLoop() {
	b := make([]byte, 4096)
	defer close(h.done)
	// LIFO with the defer above: connections are torn down (Disconnected()
	// fires) before done closes, and after the loop below has recorded the
	// fatal error — so Error() is accurate for anyone woken by either.
	defer h.cleanupConns()
	for {
		n, err := h.skt.Read(b)
		if n == 0 || err != nil {
			// If-absent: a close(err) tears the socket down and lands
			// here; the resulting read error must not clobber the
			// recorded root cause.
			if err == io.EOF {
				h.setErrIfAbsent(err) // callers depend on detecting io.EOF, don't wrap it.
			} else {
				h.setErrIfAbsent(fmt.Errorf("skt: %w", err))
			}
			return
		}
		var p []byte
		var pooled *[]byte
		if poolableEvtPkt(b[:n]) {
			pooled = evtPool.Get().(*[]byte)
			p = (*pooled)[:n]
		} else {
			p = make([]byte, n)
		}
		copy(p, b)
		err = h.handlePkt(p)
		if pooled != nil {
			evtPool.Put(pooled)
		}
		if err != nil {
			// Some bluetooth devices may append vendor specific packets at the last,
			// in this case, simply ignore them.
			if strings.HasPrefix(err.Error(), "unsupported vendor packet:") {
				ble.Logger.Error("skt: ignoring vendor packet", "err", err)
			} else {
				ble.Logger.Error("skt: failed to handle packet", "err", err)
				continue
			}
		}
	}
}

func (h *HCI) close(err error) error {
	// Record the error before tearing the socket down: the socket close
	// ends sktLoop, whose exit path (cleanupConns) closes every conn's
	// chDone — Disconnected() observers calling Error() must see the root
	// cause, not nil.
	h.setErr(err)
	if h.skt != nil {
		return h.skt.Close()
	}
	return err
}

// cleanupConns tears down every registered connection after the HCI
// transport is gone (adapter reset, unplug, or an internal close). Without
// it a socket death left each conn's channels open forever: the recombine
// goroutines leaked, chDone never closed, and Disconnected()-driven
// reconnect logic upstairs never fired — a notify-only peripheral simply
// went silent for good.
//
// It runs on the sktLoop goroutine after the read loop has exited, so it
// cannot race handleACL or handleDisconnectionComplete (same goroutine,
// program order). Conns already torn down by a disconnect event were
// removed from h.conns under muConns and are not revisited here; closeChans
// tolerates any residual overlap regardless.
func (h *HCI) cleanupConns() {
	h.muConns.Lock()
	conns := h.conns
	h.conns = make(map[uint16]*Conn)
	h.muConns.Unlock()
	for _, c := range conns {
		c.closeChans()
		// Return the connection's outstanding TX credits, exactly as
		// handleDisconnectionComplete does: with the transport gone no
		// NumberOfCompletedPackets event will ever return them, and a
		// writer mid-writePDU must not leak buffers from the shared
		// pool (hence the per-conn train mutex ReclaimAll takes, with
		// chDone already closed so the writer exits promptly).
		c.txBuffer.ReclaimAll()
	}
}

// resetAdHistory clears the AD/SR pairing history for a fresh scan. Guarded
// because Scan() — notably its Disallowed reconciliation, which runs while
// the controller is still streaming reports — races the report handler on
// sktLoop.
func (h *HCI) resetAdHistory() {
	h.muAdHist.Lock()
	h.adHist = make([]*Advertisement, 128)
	h.adLast = 0
	h.muAdHist.Unlock()
}

// dispose runs c.Close on its own tracked goroutine. The goroutine is
// mandatory for callers on sktLoop (kill, the undeliverable-conn paths in
// handleLEConnectionComplete): Close sends a Disconnect command whose
// completion only sktLoop can process, so an inline call would
// self-deadlock.
func (h *HCI) dispose(c *Conn) {
	h.wgConnDisposal.Add(1)
	go func() {
		defer h.wgConnDisposal.Done()
		c.Close()
	}()
}

// cleanupConn force-removes c as though its DisconnectionComplete had been
// processed: deregister, close channels, reclaim TX credits. Used by
// Conn.Close when the Disconnect command failed and the event never arrived
// — the one abandoned-command aftermath with no completion to reconcile on.
// Safe against a racing event or cleanupConns: whoever removes c from
// h.conns performs the teardown, and closeChans/ReclaimAll tolerate
// repeats. Should the abandoned Disconnect still execute later, its event
// finds no conn and handleDisconnectionComplete reports the stray handle —
// log noise, not state divergence.
func (h *HCI) cleanupConn(c *Conn) {
	handle := c.param.ConnectionHandle()
	h.muConns.Lock()
	cc, found := h.conns[handle]
	if found && cc == c {
		delete(h.conns, handle)
	}
	h.muConns.Unlock()
	if !found || cc != c {
		// Already torn down — or the handle was reused by a newer conn,
		// which must be left alone.
		return
	}
	c.closeChans()
	c.txBuffer.ReclaimAll()
}

func (h *HCI) handlePkt(b []byte) error {
	// Strip the 1-byte HCI header and pass down the rest of the packet.
	t, b := b[0], b[1:]
	switch t {
	case pktTypeCommand:
		return fmt.Errorf("unmanaged cmd: % X", b)
	case pktTypeACLData:
		return h.handleACL(b)
	case pktTypeSCOData:
		return fmt.Errorf("unsupported sco packet: % X", b)
	case pktTypeEvent:
		return h.handleEvt(b)
	case pktTypeVendor:
		return fmt.Errorf("unsupported vendor packet: % X", b)
	default:
		return fmt.Errorf("invalid packet: 0x%02X % X", t, b)
	}
}

func (h *HCI) handleACL(b []byte) error {
	handle := packet(b).handle()
	h.muConns.Lock()
	c, ok := h.conns[handle]
	h.muConns.Unlock()
	if !ok {
		ble.Logger.Warn("invalid connection handle on ACL packet", "handle", handle)
		return nil
	}
	// Non-blocking: this runs on sktLoop, the single dispatcher for every
	// connection on the adapter. A full chInPkt means this connection's
	// recombine/Read pipeline has stopped consuming, and a blocking send
	// here would park sktLoop and freeze ALL connections. Drop the packet
	// and kill just this connection instead. Dropping mid-PDU ACL data
	// corrupts this connection's L2CAP stream — which is fine, because we
	// are tearing the connection down anyway.
	select {
	case c.chInPkt <- b:
	default:
		h.aclDropped.Add(1)
		// kill is once-per-conn: a burst of would-block packets spawns
		// exactly one teardown goroutine and logs one warning.
		if c.kill() {
			ble.Logger.Warn("hci: connection not consuming ACL data; dropping packet and closing connection", "handle", handle)
		}
	}
	return nil
}

// ACLDropped returns the number of inbound ACL data packets dropped because
// the owning connection stopped consuming them (see handleACL). Non-zero
// means at least one connection was torn down for wedging its receive path.
func (h *HCI) ACLDropped() uint64 {
	return h.aclDropped.Load()
}

func (h *HCI) handleEvt(b []byte) error {
	code, plen := int(b[0]), int(b[1])
	if plen != len(b[2:]) {
		return fmt.Errorf("invalid event packet: % X", b)
	}
	if code == evt.CommandCompleteCode || code == evt.CommandStatusCode {
		if f := h.evth[code]; f != nil {
			return f(b[2:])
		}
	}
	if f := h.evth[code]; f != nil {
		// A handler error concerns one event, not the transport: log it
		// instead of storing it in h.err, where it would fail every
		// subsequent Send until overwritten (and race with send's read).
		if err := f(b[2:]); err != nil {
			ble.Logger.Error("hci: event handler failed", "code", fmt.Sprintf("%#02x", code), "err", err)
		}
		return nil
	}
	if code == 0xff { // Ignore vendor events
		return nil
	}
	return fmt.Errorf("unsupported event packet: % X", b)
}

func (h *HCI) handleLEMeta(b []byte) error {
	subcode := int(b[0])
	if f := h.subh[subcode]; f != nil {
		return f(b)
	}
	return fmt.Errorf("unsupported LE event: % X", b)
}

// errOrphanScanRsp reports that an LE Advertising Report event carried at
// least one scan response with no matching Advertising Data packet in the
// scan history. It is a shared sentinel — returned once per event, after
// every report in the event has been processed — so the orphan surfaces in
// sktLoop's log without a per-report fmt.Errorf allocation on the hot scan
// path (see also SROrphaned).
var errOrphanScanRsp = errors.New("hci: received scan response with no associated Advertising Data packet")

func (h *HCI) handleLEAdvertisingReport(b []byte) error {
	if h.advHandler == nil {
		return nil
	}

	var orphanErr error
	e := evt.LEAdvertisingReport(b)
	// The whole event is processed under muAdHist: every arm of the switch
	// below touches adHist/adLast, which Scan() reallocates concurrently.
	h.muAdHist.Lock()
	defer h.muAdHist.Unlock()
	for i := 0; i < int(e.NumReports()); i++ {
		var a *Advertisement
		switch e.EventType(i) {
		case evtTypAdvInd:
			fallthrough
		case evtTypAdvScanInd:
			a = newAdvertisement(e, i)
			h.adHist[h.adLast] = a
			h.adLast++
			if h.adLast == len(h.adHist) {
				h.adLast = 0
			}
		case evtTypScanRsp:
			sr := newAdvertisement(e, i)
			for idx := h.adLast - 1; idx != h.adLast; idx-- {
				if idx == -1 {
					idx = len(h.adHist) - 1
				}
				if h.adHist[idx] == nil {
					break
				}
				if h.adHist[idx].e.Address(h.adHist[idx].i) == sr.e.Address(sr.i) {
					// Pair AD+SR in a fresh Advertisement instead of mutating
					// the stored entry: the stored one may still be in the
					// handler's hands, and setScanResponse would race with its
					// field accessors and packet cache. adHist entries stay
					// immutable after creation. Comparing raw addresses also
					// avoids two string allocations per scanned history entry.
					a = newAdvertisement(h.adHist[idx].e, h.adHist[idx].i)
					a.setScanResponse(sr)
					break
				}
			}
			// Got a SR without having received an associated AD before?
			// Count it and keep going: the remaining reports in this HCI
			// event are independent, and aborting here (the old behavior)
			// dropped them all while allocating a fresh error per orphan.
			if a == nil {
				h.srOrphaned.Add(1)
				orphanErr = errOrphanScanRsp
				continue
			}
		default:
			a = newAdvertisement(e, i)
		}
		// Non-blocking hand-off to the dispatcher. A goroutine per report
		// (the previous model) churns the scheduler at scan rates, delivers
		// reports out of order, and leaks goroutines when a handler blocks.
		select {
		case h.chAdv <- a:
		default:
			h.advDropped.Add(1)
		}
	}

	return orphanErr
}

// advDispatcher delivers advertising reports to the registered handler, one
// at a time and in arrival order. It runs for the lifetime of the HCI
// instance and exits when h.done is closed (by sktLoop, on socket close).
func (h *HCI) advDispatcher() {
	for {
		select {
		case a := <-h.chAdv:
			if f := h.advHandler; f != nil {
				f(a)
			}
		case <-h.done:
			return
		}
	}
}

// AdvDropped returns the number of advertising reports dropped because the
// dispatch queue was full, i.e. the advertisement handler could not keep up
// with the scan rate.
func (h *HCI) AdvDropped() uint64 {
	return h.advDropped.Load()
}

// SROrphaned returns the number of scan-response reports dropped because no
// matching Advertising Data packet was found in the scan history (typically
// the AD was evicted from the bounded history, or scanning started mid
// AD/SR exchange). Orphans are counted per report and never prevent the
// remaining reports in the same HCI event from being processed.
func (h *HCI) SROrphaned() uint64 {
	return h.srOrphaned.Load()
}

func (h *HCI) handleCommandComplete(b []byte) error {
	e := evt.CommandComplete(b)
	h.setAllowedCommands(int(e.NumHCICommandPackets()))

	// NOP command, used for flow control purpose [Vol 2, Part E, 4.4]
	// no handling other than setAllowedCommands needed
	if e.CommandOpcode() == 0x0000 {
		return nil
	}
	h.muSent.Lock()
	p, found := h.sent[int(e.CommandOpcode())]
	h.muSent.Unlock()
	if !found {
		return fmt.Errorf("can't find the cmd for CommandCompleteEP: % X", e)
	}
	// ReturnParameters aliases the event buffer, and the send() caller parked
	// on p.done keeps using it after this handler returns (Send passes it to
	// Unmarshal) — hand over a copy so the event buffer can go back to
	// evtPool. Command completions are cold path; the copy is cheap.
	ret := make([]byte, len(e.ReturnParameters()))
	copy(ret, e.ReturnParameters())
	// Non-blocking: done holds exactly the one expected reply. If send()
	// timed out just before this event arrived (its entry still in h.sent),
	// or a duplicate completion shows up for the same opcode, drop the
	// reply instead of wedging sktLoop on a channel nobody reads.
	select {
	case p.done <- ret:
	default:
	}
	return nil
}

func (h *HCI) handleCommandStatus(b []byte) error {
	e := evt.CommandStatus(b)
	h.setAllowedCommands(int(e.NumHCICommandPackets()))

	h.muSent.Lock()
	p, found := h.sent[int(e.CommandOpcode())]
	h.muSent.Unlock()
	if !found {
		return fmt.Errorf("can't find the cmd for CommandStatusEP: % X", e)
	}
	// Non-blocking for the same reason as in handleCommandComplete.
	select {
	case p.done <- []byte{e.Status()}:
	default:
	}
	return nil
}

func (h *HCI) handleLEConnectionComplete(b []byte) error {
	e := evt.LEConnectionComplete(b)
	if e.Status() != 0x00 {
		if e.Role() == roleMaster && ErrCommand(e.Status()) == ErrConnID {
			// The connection was canceled successfully.
			return nil
		}
		// The connect failed (e.g. 0x3E connection failed to be
		// established). For either role, don't create or register a
		// Conn: the handle is junk, no disconnect event will ever
		// arrive for it, and the Conn (with its recombine goroutine
		// and buffers) would sit in h.conns forever. chMasterConn and
		// chSlaveConn deliberately get nothing: gap.go's Dial keeps
		// waiting and surfaces the failure through its context or
		// dialerTmo timeout, and Accept keeps listening for the next
		// inbound connection. connectedHandler is not invoked either —
		// it used to fire for failed slave connects, handing the
		// application an event for a connection that never existed.
		ble.Logger.Warn("hci: connection failed to establish", "role", e.Role(), "status", ErrCommand(e.Status()).Error())
		return nil
	}
	// Status is 0x00 from here on (failures returned above).
	c := newConn(h, e)
	h.muConns.Lock()
	h.conns[e.ConnectionHandle()] = c
	h.muConns.Unlock()
	if e.Role() == roleMaster {
		select {
		case h.chMasterConn <- c:
		default:
			h.dispose(c)
		}
		return nil
	}
	// Slave (peripheral) role. The hand-off must be non-blocking for the
	// same reason as the master path above: this runs on sktLoop, and with
	// no Accept() pending a blocking send would wedge the whole adapter.
	// With no listener the connection is refused cleanly instead — Close
	// sends a Disconnect command (from its own goroutine; only sktLoop can
	// process its completion), and the disconnect event finishes the
	// teardown of the registered conn.
	select {
	case h.chSlaveConn <- c:
	default:
		h.dispose(c)
	}
	// When a controller accepts a connection, it moves from advertising
	// state to idle/ready state. Host needs to explicitly ask the
	// controller to re-enable advertising. Note that the host was most
	// likely in advertising state. Otherwise it couldn't accept the
	// connection in the first place. The only exception is that user
	// asked the host to stop advertising during this tiny window.
	// The re-enabling might failed or ignored by the controller, if
	// it had reached the maximum number of concurrent connections.
	// So we also re-enable the advertising when a connection disconnected
	h.params.RLock()
	if h.params.advEnable.AdvertisingEnable == 1 {
		go h.Send(&cmd.LESetAdvertiseEnable{AdvertisingEnable: 0}, nil)
	}
	h.params.RUnlock()
	if h.connectedHandler != nil {
		h.connectedHandler(e)
	}
	return nil
}

// handleLEConnectionUpdateComplete dispatches an LE Connection Update Complete
// meta event to the connection that requested the update, if it is still
// registered and still has a waiter (see Conn.UpdateParams). It runs on the
// sktLoop goroutine, so it must never block: the conn lookup mirrors
// handleDisconnectionComplete (guarded by muConns), and deliverConnUpdate does
// a non-blocking send. A completion with no matching conn (torn down) or no
// registered waiter (a slave-forwarded update from signal.go, or the waiter
// already gave up) is a harmless no-op.
func (h *HCI) handleLEConnectionUpdateComplete(b []byte) error {
	e := evt.LEConnectionUpdateComplete(b)
	h.muConns.Lock()
	c, found := h.conns[e.ConnectionHandle()]
	h.muConns.Unlock()
	if !found {
		if logDebugEnabled() {
			ble.Logger.Debug("hci: LE connection update complete for unknown handle",
				"handle", e.ConnectionHandle(), "status", e.Status())
		}
		return nil
	}
	c.deliverConnUpdate(leConnUpdate{
		status:   e.Status(),
		interval: e.ConnInterval(),
		latency:  e.ConnLatency(),
		timeout:  e.SupervisionTimeout(),
	})
	return nil
}

// handleLEDataLengthChange records the negotiated LE data-length maximums from
// an LE Data Length Change meta event on the owning connection, for DataLength()
// to read. It runs on the sktLoop goroutine, so it must never block: the conn
// lookup mirrors handleLEConnectionUpdateComplete (guarded by muConns) and
// setDataLength only takes a brief mutex to store the value — there is no
// waiter to deliver to, because this event is asynchronous, optional, and can
// be peer-initiated (unsolicited DLE), so it is never correlated 1:1 with a
// SetDataLength call. An event for a handle with no registered conn (a change
// racing teardown) is a harmless no-op.
//
// Like the sibling meta-event handlers this trusts the controller's event
// length by convention and does not length-guard the accessors.
func (h *HCI) handleLEDataLengthChange(b []byte) error {
	e := evt.LEDataLengthChange(b)
	h.muConns.Lock()
	c, found := h.conns[e.ConnectionHandle()]
	h.muConns.Unlock()
	if !found {
		if logDebugEnabled() {
			ble.Logger.Debug("hci: LE data length change for unknown handle", "handle", e.ConnectionHandle())
		}
		return nil
	}
	c.setDataLength(dataLength{
		maxTxOctets: e.MaxTxOctets(),
		maxTxTime:   e.MaxTxTime(),
		maxRxOctets: e.MaxRxOctets(),
		maxRxTime:   e.MaxRxTime(),
	})
	if logDebugEnabled() {
		ble.Logger.Debug("hci: LE data length change",
			"handle", e.ConnectionHandle(),
			"maxTxOctets", e.MaxTxOctets(), "maxTxTime", e.MaxTxTime(),
			"maxRxOctets", e.MaxRxOctets(), "maxRxTime", e.MaxRxTime())
	}
	return nil
}

func (h *HCI) handleDisconnectionComplete(b []byte) error {
	e := evt.DisconnectionComplete(b)
	h.muConns.Lock()
	c, found := h.conns[e.ConnectionHandle()]
	delete(h.conns, e.ConnectionHandle())
	h.muConns.Unlock()
	if !found {
		return fmt.Errorf("disconnecting an invalid handle %04X", e.ConnectionHandle())
	}
	// Close the conn's channels for both roles: chInPkt winds down the
	// recombine goroutine, chDone signals Disconnected() and fails writes
	// fast. (Historically chDone was closed for masters only, leaving a
	// slave conn's Disconnected() silent forever and its writes free to
	// sit out full credit timeouts on a dead link.) The conn was removed
	// from h.conns above, so handleACL can no longer deliver to chInPkt;
	// the guarded close makes any overlap with cleanupConns or a repeat
	// teardown panic-free.
	c.closeChans()

	if c.param.Role() == roleSlave {
		// Re-enable advertising, if it was advertising. Refer to the
		// handleLEConnectionComplete() for details.
		// This may failed with ErrCommandDisallowed, if the controller
		// was actually in advertising state. It does no harm though.
		h.params.RLock()
		if h.params.advEnable.AdvertisingEnable == 1 {
			go h.Send(&h.params.advEnable, nil)
		}
		h.params.RUnlock()
	}
	// When a connection disconnects, all the sent packets and weren't acked yet
	// will be recycled. [Vol2, Part E 4.1.1]
	//
	// ReclaimAll serializes against an in-flight writePDU train on this
	// conn via the per-conn train mutex; closeChans above closed chDone
	// first, so such a train exits its credit wait promptly instead of
	// holding the mutex for the full ACLWriteTimeout.
	c.txBuffer.ReclaimAll()
	if h.disconnectedHandler != nil {
		h.disconnectedHandler(e)
	}
	return nil
}

func (h *HCI) handleNumberOfCompletedPackets(b []byte) error {
	e := evt.NumberOfCompletedPackets(b)
	h.muConns.Lock()
	defer h.muConns.Unlock()
	for i := 0; i < int(e.NumberOfHandles()); i++ {
		c, found := h.conns[e.ConnectionHandle(i)]
		if !found {
			continue
		}

		// Put the delivered buffers back to the pool.
		for j := 0; j < int(e.HCNumOfCompletedPackets(i)); j++ {
			c.txBuffer.Put()
		}
	}
	return nil
}

func (h *HCI) handleLELongTermKeyRequest(b []byte) error {
	// No LTK is stored (encryption is unsupported), so refuse the request.
	// The reply must NOT be sent synchronously: h.Send blocks until the
	// command's completion event, which only sktLoop — the goroutine
	// running this handler — can process. A synchronous Send self-stalls
	// for the full cmdTimeout and then surfaces a bogus command-timeout
	// failure. Send from a fresh goroutine instead, like
	// handleDisconnectionComplete's re-advertise path, and log any error.
	handle := evt.LELongTermKeyRequest(b).ConnectionHandle()
	go func() {
		if err := h.Send(&cmd.LELongTermKeyRequestNegativeReply{
			ConnectionHandle: handle,
		}, nil); err != nil {
			ble.Logger.Error("hci: LE long term key negative reply failed", "handle", handle, "err", err)
		}
	}()
	return nil
}

func (h *HCI) setAllowedCommands(n int) {

	//hard-coded limit to command queue depth
	//matches make(chan []byte, 16) in NewHCI
	// TODO make this a constant, decide correct size
	if n > 16 {
		n = 16
	}

	for len(h.chCmdBufs) < n {
		h.chCmdBufs <- make([]byte, 64) // TODO make buffer size a constant
	}
}
