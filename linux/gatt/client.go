package gatt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/att"
)

const (
	cccNotify   = 0x0001
	cccIndicate = 0x0002
)

// ErrNoCCCD is returned by Subscribe and Unsubscribe when the characteristic
// carries no Client Characteristic Configuration descriptor — typically
// because descriptor discovery has not been run for it, or because the peer
// genuinely exposes none. Callers doing targeted discovery can assert on it
// with errors.Is and fall back to a descriptor walk.
var ErrNoCCCD = errors.New("gatt: characteristic has no CCCD")

// NewClient returns a GATT Client.
func NewClient(conn ble.Conn) (*Client, error) {
	p := &Client{
		subs: make(map[uint16]*sub),
		conn: conn,
	}
	p.ac = att.NewClient(conn, p)
	go p.ac.Loop()
	return p, nil
}

// A Client is a GATT Client.
type Client struct {
	sync.RWMutex

	profile *ble.Profile

	// nameMu guards name/nameRead and serializes Name resolution. It is
	// deliberately NOT the client-wide embedded lock: Name reads the GAP
	// Device Name over up to two ATT round trips, and holding the embedded
	// lock across them stalled every other client method — Addr, Profile,
	// reads, writes — for up to the ATT transaction timeout (30s) per trip
	// whenever the peer was slow. With its own mutex, Name blocks only
	// other Name calls (which is the point: concurrent callers dedupe onto
	// the single resolution instead of each issuing the reads), and the ATT
	// layer serializes the reads against other traffic on its own.
	// nameRead caches the resolved outcome — including deterministic
	// negatives (peer exposes none, malformed response) — so at most one
	// call ever pays the round trips.
	nameMu   sync.Mutex
	name     string
	nameRead bool

	// subsMu guards subs and each sub's handler fields for HandleNotification,
	// which must not touch the embedded client-wide mutex: request methods hold
	// that exclusively for their entire ATT round trip, and even an RLock would
	// queue behind them. Mutators (setHandlers, ClearSubscriptions) run under
	// the embedded lock and additionally take subsMu around each mutation.
	// Lock ordering: embedded lock first, then subsMu; subsMu is a leaf and is
	// never held across an ATT round trip or a handler invocation.
	subsMu sync.RWMutex
	subs   map[uint16]*sub

	ac   *att.Client
	conn ble.Conn
}

// Addr returns the address of the client.
func (p *Client) Addr() ble.Addr {
	p.RLock()
	defer p.RUnlock()
	return p.conn.RemoteAddr()
}

// Name returns the device name of the remote peripheral, read lazily from
// the GAP Device Name characteristic (service 0x1800, characteristic 0x2A00)
// on first call and cached thereafter.
//
// Name keeps its error-free signature — it is a convenience accessor. On
// failure it logs at debug level and returns "". Deterministic outcomes —
// success, a peer that exposes no GAP Device Name, a malformed response —
// are cached, so at most one call ever pays the round trips; transient
// failures (ATT error, dead link) are not cached and the next call
// retries. The read locates the characteristic value directly with an ATT
// Read By Type request, so the caller need not have discovered service
// 0x1800 first; each round trip is bounded by the ATT bearer's own
// transaction timeout.
//
// Name holds only nameMu (not the client-wide lock), so a slow peer during
// the read never blocks other operations on this client.
func (p *Client) Name() string {
	p.nameMu.Lock()
	defer p.nameMu.Unlock()
	if p.nameRead {
		return p.name
	}
	ctx := context.Background()
	length, b, err := p.ac.ReadByType(ctx, 0x0001, 0xFFFF, ble.DeviceNameUUID)
	if errors.Is(err, ble.ErrAttrNotFound) {
		// The peer has no GAP Device Name — a stable fact, cache it.
		p.nameRead = true
		return ""
	}
	if err != nil {
		ble.Logger().Debug("gatt: reading GAP device name", "err", err)
		return ""
	}
	if length < 2 || len(b) < length {
		ble.Logger().Debug("gatt: malformed GAP device name response", "length", length, "data", len(b))
		p.nameRead = true // deterministic peer bug; retrying won't unmangle it
		return ""
	}
	// First entry: value handle (2 bytes) + as much of the value as fit in
	// the response. Re-read by handle so a name longer than one Read By Type
	// response is not silently truncated.
	h := binary.LittleEndian.Uint16(b[:2])
	v, err := p.ac.Read(ctx, h)
	if err != nil {
		ble.Logger().Debug("gatt: reading GAP device name value", "handle", h, "err", err)
		return ""
	}
	p.name = string(v)
	p.nameRead = true
	return p.name
}

// Profile returns the discovered profile.
func (p *Client) Profile() *ble.Profile {
	p.RLock()
	defer p.RUnlock()
	return p.profile
}

// DiscoverProfile discovers the whole hierarchy of a server.
func (p *Client) DiscoverProfile(ctx context.Context, force bool) (*ble.Profile, error) {
	if p.profile != nil && !force {
		return p.profile, nil
	}
	ss, err := p.DiscoverServices(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("can't discover services: %w", err)
	}
	for _, s := range ss {
		cs, err := p.DiscoverCharacteristics(ctx, nil, s)
		if err != nil {
			return nil, fmt.Errorf("can't discover characteristics: %w", err)
		}
		for _, c := range cs {
			_, err := p.DiscoverDescriptors(ctx, nil, c)
			if err != nil {
				return nil, fmt.Errorf("can't discover descriptors: %w", err)
			}
		}
	}
	p.profile = &ble.Profile{Services: ss}
	return p.profile, nil
}

// DiscoverServices finds all the primary services on a server. [Vol 3, Part G, 4.4.1]
// If filter is specified, only filtered services are returned.
func (p *Client) DiscoverServices(ctx context.Context, filter []ble.UUID) ([]*ble.Service, error) {
	p.Lock()
	defer p.Unlock()
	if p.profile == nil {
		p.profile = &ble.Profile{}
	}
	start := uint16(0x0001)
	for {
		length, b, err := p.ac.ReadByGroupType(ctx, start, 0xFFFF, ble.PrimaryServiceUUID)
		if errors.Is(err, ble.ErrAttrNotFound) {
			return p.profile.Services, nil
		}
		if err != nil {
			return nil, err
		}
		// Service entry [Vol 3, Part G, 4.4.1]: attribute handle (2) +
		// end group handle (2) + service UUID (2 or 16). length is
		// peer-controlled; any other value would walk b out of bounds.
		if length != 6 && length != 20 {
			return nil, fmt.Errorf("gatt: service entry has invalid length %d", length)
		}
		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			endh := binary.LittleEndian.Uint16(b[2:4])
			u := ble.UUID(b[4:length])
			if filter == nil || ble.Contains(filter, u) {
				s := &ble.Service{
					UUID:      u,
					Handle:    h,
					EndHandle: endh,
				}
				p.profile.Services = append(p.profile.Services, s)
			}
			if endh == 0xFFFF {
				return p.profile.Services, nil
			}
			start = endh + 1
			b = b[length:]
		}
	}
}

// DiscoverIncludedServices finds the included services of a service.
// [Vol 3, Part G, 4.5.1] If filter is specified, only filtered services are
// returned. Discovered includes are also recorded on s.Includes.
//
// A service with no includes yields an empty non-nil slice and a nil error:
// the walk actually ran and found nothing — distinct from the old stub's
// (nil, nil), which faked success without ever asking the peer.
func (p *Client) DiscoverIncludedServices(ctx context.Context, filter []ble.UUID, s *ble.Service) ([]*ble.Service, error) {
	p.Lock()
	defer p.Unlock()
	iss := []*ble.Service{}
	start := s.Handle
	for start <= s.EndHandle {
		length, b, err := p.ac.ReadByType(ctx, start, s.EndHandle, ble.IncludeUUID)
		if errors.Is(err, ble.ErrAttrNotFound) {
			break
		} else if err != nil {
			return nil, err
		}
		// Include declaration value [Vol 3, Part G, 3.2]: included service
		// attribute handle (2) + end group handle (2) + the service UUID
		// when it is 16-bit (entry length 8, incl. the 2-byte attribute
		// handle prefix). A 128-bit UUID is never carried in the
		// declaration (entry length 6) and requires a follow-up read of
		// the included service's declaration attribute.
		if length != 6 && length != 8 {
			return nil, fmt.Errorf("gatt: include declaration entry has invalid length %d", length)
		}
		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			ish := binary.LittleEndian.Uint16(b[2:4])
			iseh := binary.LittleEndian.Uint16(b[4:6])
			var u ble.UUID
			if length == 8 {
				u = ble.UUID(b[6:8])
			} else {
				v, err := p.ac.Read(ctx, ish)
				if err != nil {
					return nil, fmt.Errorf("gatt: can't read included service declaration at 0x%04X: %w", ish, err)
				}
				u = ble.UUID(v)
			}
			if filter == nil || ble.Contains(filter, u) {
				is := &ble.Service{
					UUID:      u,
					Handle:    ish,
					EndHandle: iseh,
				}
				iss = append(iss, is)
				s.Includes = append(s.Includes, is)
			}
			if h == 0xFFFF {
				return iss, nil
			}
			start = h + 1
			b = b[length:]
		}
	}
	return iss, nil
}

// DiscoverCharacteristics finds all the characteristics within a service. [Vol 3, Part G, 4.6.1]
// If filter is specified, only filtered characteristics are returned.
func (p *Client) DiscoverCharacteristics(ctx context.Context, filter []ble.UUID, s *ble.Service) ([]*ble.Characteristic, error) {
	p.Lock()
	defer p.Unlock()
	start := s.Handle
	var lastChar *ble.Characteristic
	for start <= s.EndHandle {
		length, b, err := p.ac.ReadByType(ctx, start, s.EndHandle, ble.CharacteristicUUID)
		if errors.Is(err, ble.ErrAttrNotFound) {
			break
		} else if err != nil {
			return nil, err
		}
		// Characteristic entry [Vol 3, Part G, 4.6.1]: attribute handle (2)
		// + properties (1) + value handle (2) + UUID (2 or 16). length is
		// peer-controlled; any other value would walk b out of bounds.
		if length != 7 && length != 21 {
			return nil, fmt.Errorf("gatt: characteristic entry has invalid length %d", length)
		}
		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			p := ble.Property(b[2])
			vh := binary.LittleEndian.Uint16(b[3:5])
			u := ble.UUID(b[5:length])
			c := &ble.Characteristic{
				UUID:        u,
				Property:    p,
				Handle:      h,
				ValueHandle: vh,
				EndHandle:   s.EndHandle,
			}
			if filter == nil || ble.Contains(filter, u) {
				s.Characteristics = append(s.Characteristics, c)
			}
			if lastChar != nil {
				lastChar.EndHandle = c.Handle - 1
			}
			lastChar = c
			start = vh + 1
			b = b[length:]
		}
	}
	return s.Characteristics, nil
}

// DiscoverDescriptors finds all the descriptors within a characteristic. [Vol 3, Part G, 4.7.1]
// If filter is specified, only filtered descriptors are returned.
func (p *Client) DiscoverDescriptors(ctx context.Context, filter []ble.UUID, c *ble.Characteristic) ([]*ble.Descriptor, error) {
	p.Lock()
	defer p.Unlock()
	start := c.ValueHandle + 1
	for start <= c.EndHandle {
		fmt, b, err := p.ac.FindInformation(ctx, start, c.EndHandle)
		if errors.Is(err, ble.ErrAttrNotFound) {
			break
		} else if err != nil {
			return nil, err
		}
		length := 2 + 2
		if fmt == 0x02 {
			length = 2 + 16
		}
		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			u := ble.UUID(b[2:length])
			d := &ble.Descriptor{UUID: u, Handle: h}
			if filter == nil || ble.Contains(filter, u) {
				c.Descriptors = append(c.Descriptors, d)
			}
			if u.Equal(ble.ClientCharacteristicConfigUUID) {
				c.CCCD = d
			}
			start = h + 1
			b = b[length:]
		}
	}
	return c.Descriptors, nil
}

// ReadCharacteristic reads a characteristic value from a server. [Vol 3, Part G, 4.8.1]
func (p *Client) ReadCharacteristic(ctx context.Context, c *ble.Characteristic) ([]byte, error) {
	p.Lock()
	defer p.Unlock()
	val, err := p.ac.Read(ctx, c.ValueHandle)
	if err != nil {
		return nil, err
	}

	c.Value = val
	return val, nil
}

// ReadLongCharacteristic reads a characteristic value which is longer than the MTU. [Vol 3, Part G, 4.8.3]
func (p *Client) ReadLongCharacteristic(ctx context.Context, c *ble.Characteristic) ([]byte, error) {
	p.Lock()
	defer p.Unlock()

	// The maximum length of an attribute value shall be 512 octects [Vol 3, 3.2.9]
	buffer := make([]byte, 0, 512)

	read, err := p.ac.Read(ctx, c.ValueHandle)
	if err != nil {
		return nil, err
	}
	buffer = append(buffer, read...)

	for len(read) >= p.conn.TxMTU()-1 {
		if read, err = p.ac.ReadBlob(ctx, c.ValueHandle, uint16(len(buffer))); err != nil {
			return nil, err
		}
		buffer = append(buffer, read...)
	}

	c.Value = buffer
	return buffer, nil
}

// WriteCharacteristic writes a characteristic value to a server. [Vol 3, Part G, 4.9.3]
func (p *Client) WriteCharacteristic(ctx context.Context, c *ble.Characteristic, v []byte, noRsp bool) error {
	p.Lock()
	defer p.Unlock()
	if noRsp {
		return p.ac.WriteCommand(ctx, c.ValueHandle, v)
	}
	return p.ac.Write(ctx, c.ValueHandle, v)
}

// ReadDescriptor reads a characteristic descriptor from a server. [Vol 3, Part G, 4.12.1]
func (p *Client) ReadDescriptor(ctx context.Context, d *ble.Descriptor) ([]byte, error) {
	p.Lock()
	defer p.Unlock()
	val, err := p.ac.Read(ctx, d.Handle)
	if err != nil {
		return nil, err
	}

	d.Value = val
	return val, nil
}

// WriteDescriptor writes a characteristic descriptor to a server. [Vol 3, Part G, 4.12.3]
func (p *Client) WriteDescriptor(ctx context.Context, d *ble.Descriptor, v []byte) error {
	p.Lock()
	defer p.Unlock()
	return p.ac.Write(ctx, d.Handle, v)
}

// ReadRSSI retrieves the current RSSI value of the remote peripheral, in
// dBm. [Vol 2, Part E, 7.5.4]
//
// The underlying HCI command exchange is bounded by the HCI layer's own
// command timeout and cannot be interrupted mid-flight; per the ble.Client
// contract, ctx bounds only this caller's wait — on expiry ctx.Err() is
// returned and the exchange runs to completion in the background with its
// result discarded.
//
// ReadRSSI deliberately does not hold the client-wide lock: it exchanges an
// HCI command, not an ATT request, so it neither queues behind nor blocks an
// in-flight ATT round trip. (p.conn is assigned once at construction.)
func (p *Client) ReadRSSI(ctx context.Context) (int, error) {
	type result struct {
		rssi int
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		rssi, err := p.conn.ReadRSSI()
		ch <- result{rssi, err}
	}()
	select {
	case r := <-ch:
		return r.rssi, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// ExchangeMTU informs the server of the client’s maximum receive MTU size and
// request the server to respond with its maximum receive MTU size. [Vol 3, Part F, 3.4.2.1]
func (p *Client) ExchangeMTU(ctx context.Context, mtu int) (int, error) {
	p.Lock()
	defer p.Unlock()
	return p.ac.ExchangeMTU(ctx, mtu)
}

// Subscribe subscribes to indication (if ind is set true), or notification of a
// characteristic value. [Vol 3, Part G, 4.10 & 4.11]
func (p *Client) Subscribe(ctx context.Context, c *ble.Characteristic, ind bool, h ble.NotificationHandler) error {
	p.Lock()
	defer p.Unlock()
	if c.CCCD == nil {
		return ErrNoCCCD
	}
	if ind {
		return p.setHandlers(ctx, c.CCCD.Handle, c.ValueHandle, cccIndicate, h)
	}
	return p.setHandlers(ctx, c.CCCD.Handle, c.ValueHandle, cccNotify, h)
}

// Unsubscribe unsubscribes to indication (if ind is set true), or notification
// of a specified characteristic value. [Vol 3, Part G, 4.10 & 4.11]
func (p *Client) Unsubscribe(ctx context.Context, c *ble.Characteristic, ind bool) error {
	p.Lock()
	defer p.Unlock()
	if c.CCCD == nil {
		return ErrNoCCCD
	}
	if ind {
		return p.setHandlers(ctx, c.CCCD.Handle, c.ValueHandle, cccIndicate, nil)
	}
	return p.setHandlers(ctx, c.CCCD.Handle, c.ValueHandle, cccNotify, nil)
}

// setHandlers reconciles the local subscription state and the peer's CCCD
// for one flag. Local state must only ever record what the peer has
// acknowledged: the old mutate-then-write order meant a failed CCCD write
// left s.ccc set, so the caller's retry hit the already-subscribed early
// return and reported success for a subscription the peer never enabled.
//
// Ordering per direction: subscribing installs the handler BEFORE the
// write (the first notification can arrive the moment the write is acked)
// and rolls back on failure; unsubscribing writes first and forgets the
// handler only on success (a late notification still finds it). A
// re-Subscribe on an enabled flag installs the new handler without a wire
// write — it no longer silently keeps the old one.
func (p *Client) setHandlers(ctx context.Context, cccdh, vh, flag uint16, h ble.NotificationHandler) error {
	s, ok := p.subs[vh]
	if !ok {
		s = &sub{cccdh, 0x0000, nil, nil}
		p.subsMu.Lock()
		p.subs[vh] = s
		p.subsMu.Unlock()
	}
	if h == nil && (s.ccc&flag) == 0 {
		return nil
	}

	prevCCC := s.ccc
	p.subsMu.Lock()
	prevH := s.nHandler
	if flag == cccIndicate {
		prevH = s.iHandler
	}
	p.subsMu.Unlock()

	setState := func(ccc uint16, handler ble.NotificationHandler) {
		p.subsMu.Lock()
		s.ccc = ccc
		if flag == cccIndicate {
			s.iHandler = handler
		} else {
			s.nHandler = handler
		}
		p.subsMu.Unlock()
	}
	writeCCC := func(ccc uint16) error {
		v := make([]byte, 2)
		binary.LittleEndian.PutUint16(v, ccc)
		return p.ac.Write(ctx, s.cccdh, v)
	}

	if h != nil {
		newCCC := prevCCC | flag
		setState(newCCC, h)
		if newCCC != prevCCC {
			if err := writeCCC(newCCC); err != nil {
				setState(prevCCC, prevH)
				return err
			}
		}
		return nil
	}

	newCCC := prevCCC &^ flag
	if err := writeCCC(newCCC); err != nil {
		return err
	}
	setState(newCCC, nil)
	return nil
}

// ClearSubscriptions clears all subscriptions to notifications and indications.
func (p *Client) ClearSubscriptions(ctx context.Context) error {
	p.Lock()
	defer p.Unlock()
	zero := make([]byte, 2)
	for vh, s := range p.subs {
		if err := p.ac.Write(ctx, s.cccdh, zero); err != nil {
			return err
		}
		p.subsMu.Lock()
		delete(p.subs, vh)
		p.subsMu.Unlock()
	}
	return nil
}

// CancelConnection disconnects the connection.
func (p *Client) CancelConnection() error {
	p.Lock()
	defer p.Unlock()
	return p.conn.Close()
}

// Disconnected returns a receiving channel, which is closed when the client disconnects.
func (p *Client) Disconnected() <-chan struct{} {
	p.Lock()
	defer p.Unlock()
	return p.conn.Disconnected()
}

// Conn returns the client's current connection.
func (p *Client) Conn() ble.Conn {
	return p.conn
}

// HandleNotification dispatches an incoming notification or indication PDU
// to the matching subscriber's handler.
//
// The subscription's handler is snapshotted under subsMu (never the
// client-wide mutex, which request methods — ReadCharacteristic,
// WriteCharacteristic, Subscribe, ... — hold exclusively for their entire
// ATT round trip) and invoked with no lock held: a notification is delivered
// without waiting for an in-flight request, and a handler may safely call
// back into the Client without deadlocking. The one consequence of the
// snapshot: a handler already being dispatched may be invoked once more
// after Unsubscribe (or ClearSubscriptions) returns.
func (p *Client) HandleNotification(req []byte) {
	if len(req) < 3 {
		// Opcode + 2-byte attribute handle is the spec minimum. att.Client
		// drops runts before dispatch; this guards the exported entry point
		// against other callers.
		ble.Logger().Warn("gatt: dropping runt notification/indication", "len", len(req))
		return
	}
	vh := att.HandleValueIndication(req).AttributeHandle()
	p.subsMu.RLock()
	sub, ok := p.subs[vh]
	var fn ble.NotificationHandler
	if ok {
		fn = sub.nHandler
		if req[0] == att.HandleValueIndicationCode {
			fn = sub.iHandler
		}
	}
	p.subsMu.RUnlock()
	if !ok {
		// FIXME: disconnects and propagate an error to the user.
		ble.Logger().Warn("gatt: got an unregistered notification")
		return
	}
	if fn != nil {
		fn(req[3:])
	}
}

type sub struct {
	cccdh    uint16
	ccc      uint16
	nHandler ble.NotificationHandler
	iHandler ble.NotificationHandler
}
