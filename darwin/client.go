package darwin

import (
	"context"
	"fmt"

	"github.com/JuulLabs-OSS/cbgo"
	"github.com/bdstark/ble"
)

// A Client is a GATT client.
type Client struct {
	cbgo.PeripheralDelegateBase

	profile *ble.Profile
	pc      profCache
	name    string
	cm      cbgo.CentralManager

	id   ble.UUID
	conn *conn
}

// NewClient ...
func NewClient(cm cbgo.CentralManager, c ble.Conn) (*Client, error) {
	as := c.RemoteAddr().String()
	id, err := ble.Parse(as)
	if err != nil {
		return nil, fmt.Errorf("connection has invalid address: addr=%s", as)
	}

	cln := &Client{
		conn: c.(*conn),
		pc:   newProfCache(),
		cm:   cm,
		id:   id,
	}

	cln.conn.prph.SetDelegate(cln)

	return cln, nil
}

// Addr returns UUID of the remote peripheral.
func (cln *Client) Addr() ble.Addr {
	return cln.conn.RemoteAddr()
}

// awaitSlot waits for a single delegate event on ch (from eventSlot.Listen).
// It returns the event's error (nil on success), ctx.Err() if the caller's
// wait expired, or a disconnected error if the link dropped or the slot was
// closed under the waiter by connection teardown — a closed slot must never
// read as a successful event.
func (cln *Client) awaitSlot(ctx context.Context, ch chan any) error {
	select {
	case itf, ok := <-ch:
		if !ok {
			return fmt.Errorf("disconnected")
		}
		if itf != nil {
			return itf.(error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-cln.Disconnected():
		return fmt.Errorf("disconnected")
	}
}

// Name returns the name of the remote peripheral.
// This can be the advertised name, if exists, or the GAP device name, which takes priority.
func (cln *Client) Name() string {
	return cln.name
}

// Profile returns the discovered profile.
func (cln *Client) Profile() *ble.Profile {
	return cln.profile
}

// DiscoverProfile discovers the whole hierarchy of a server.
func (cln *Client) DiscoverProfile(ctx context.Context, force bool) (*ble.Profile, error) {
	if cln.profile != nil && !force {
		return cln.profile, nil
	}
	ss, err := cln.DiscoverServices(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("can't discover services: %s", err)
	}
	for _, s := range ss {
		cs, err := cln.DiscoverCharacteristics(ctx, nil, s)
		if err != nil {
			return nil, fmt.Errorf("can't discover characteristics: %s", err)
		}
		for _, c := range cs {
			_, err := cln.DiscoverDescriptors(ctx, nil, c)
			if err != nil {
				return nil, fmt.Errorf("can't discover descriptors: %s", err)
			}
		}
	}
	cln.profile = &ble.Profile{Services: ss}
	return cln.profile, nil
}

// DiscoverServices finds all the primary services on a server. [Vol 3, Part G, 4.4.1]
// If filter is specified, only filtered services are returned.
func (cln *Client) DiscoverServices(ctx context.Context, ss []ble.UUID) ([]*ble.Service, error) {
	ch := cln.conn.evl.svcsDiscovered.Listen()
	defer cln.conn.evl.svcsDiscovered.Close()

	cbuuids := uuidsToCbgoUUIDs(ss)
	cln.conn.prph.DiscoverServices(cbuuids)

	if err := cln.awaitSlot(ctx, ch); err != nil {
		return nil, err
	}

	svcs := []*ble.Service{}
	for _, dsvc := range cln.conn.prph.Services() {
		svc := &ble.Service{
			UUID: ble.UUID(dsvc.UUID()),
		}
		cln.pc.addSvc(svc, dsvc)
		svcs = append(svcs, svc)
	}
	if cln.profile == nil {
		cln.profile = &ble.Profile{Services: svcs}
	}
	return svcs, nil
}

// DiscoverIncludedServices finds the included services of a service. [Vol 3, Part G, 4.5.1]
// If filter is specified, only filtered services are returned.
func (cln *Client) DiscoverIncludedServices(ctx context.Context, ss []ble.UUID, s *ble.Service) ([]*ble.Service, error) {
	return nil, ble.ErrNotImplemented
}

// DiscoverCharacteristics finds all the characteristics within a service. [Vol 3, Part G, 4.6.1]
// If filter is specified, only filtered characteristics are returned.
func (cln *Client) DiscoverCharacteristics(ctx context.Context, cs []ble.UUID, s *ble.Service) ([]*ble.Characteristic, error) {
	cbsvc, err := cln.pc.findCbSvc(s)
	if err != nil {
		return nil, err
	}

	ch := cln.conn.evl.chrsDiscovered.Listen()
	defer cln.conn.evl.chrsDiscovered.Close()

	cbuuids := uuidsToCbgoUUIDs(cs)
	cln.conn.prph.DiscoverCharacteristics(cbuuids, cbsvc)

	if err := cln.awaitSlot(ctx, ch); err != nil {
		return nil, err
	}

	for _, dchr := range cbsvc.Characteristics() {
		chr := &ble.Characteristic{
			UUID:     ble.UUID(dchr.UUID()),
			Property: ble.Property(dchr.Properties()),
		}
		cln.pc.addChr(chr, dchr)
		s.Characteristics = append(s.Characteristics, chr)
	}
	return s.Characteristics, nil
}

// DiscoverDescriptors finds all the descriptors within a characteristic. [Vol 3, Part G, 4.7.1]
// If filter is specified, only filtered descriptors are returned.
func (cln *Client) DiscoverDescriptors(ctx context.Context, ds []ble.UUID, c *ble.Characteristic) ([]*ble.Descriptor, error) {
	cbchr, err := cln.pc.findCbChr(c)
	if err != nil {
		return nil, err
	}

	ch := cln.conn.evl.dscsDiscovered.Listen()
	defer cln.conn.evl.dscsDiscovered.Close()

	cln.conn.prph.DiscoverDescriptors(cbchr)

	if err := cln.awaitSlot(ctx, ch); err != nil {
		return nil, err
	}

	for _, ddsc := range cbchr.Descriptors() {
		dsc := &ble.Descriptor{
			UUID: ble.UUID(ddsc.UUID()),
		}
		c.Descriptors = append(c.Descriptors, dsc)
		cln.pc.addDsc(dsc, ddsc)
	}
	return c.Descriptors, nil
}

// ReadCharacteristic reads a characteristic value from a server. [Vol 3, Part G, 4.8.1]
func (cln *Client) ReadCharacteristic(ctx context.Context, c *ble.Characteristic) ([]byte, error) {
	cbchr, err := cln.pc.findCbChr(c)
	if err != nil {
		return nil, err
	}

	ch, err := cln.conn.addChrReader(c)
	if err != nil {
		return nil, fmt.Errorf("failed to read characteristic: %v", err)
	}
	defer cln.conn.delChrReader(c)

	cln.conn.prph.ReadCharacteristic(cbchr)

	select {
	case err := <-ch:
		if err != nil {
			return nil, err
		}

	case <-ctx.Done():
		// Abandon the wait; the deferred delChrReader deregisters the
		// channel, and a late response's buffered send is simply GC'd.
		return nil, ctx.Err()

	case <-cln.Disconnected():
		return nil, fmt.Errorf("disconnected")
	}

	c.Value = cbchr.Value()

	return c.Value, nil
}

// ReadLongCharacteristic reads a characteristic value which is longer than the MTU. [Vol 3, Part G, 4.8.3]
func (cln *Client) ReadLongCharacteristic(ctx context.Context, c *ble.Characteristic) ([]byte, error) {
	return cln.ReadCharacteristic(ctx, c)
}

// WriteCharacteristic writes a characteristic value to a server. [Vol 3, Part G, 4.9.3]
func (cln *Client) WriteCharacteristic(ctx context.Context, c *ble.Characteristic, b []byte, noRsp bool) error {
	cbchr, err := cln.pc.findCbChr(c)
	if err != nil {
		return err
	}

	if noRsp {
		cln.conn.prph.WriteCharacteristic(b, cbchr, false)
		return nil
	}

	ch := cln.conn.evl.chrWritten.Listen()
	defer cln.conn.evl.chrWritten.Close()

	cln.conn.prph.WriteCharacteristic(b, cbchr, true)

	return cln.awaitSlot(ctx, ch)
}

// ReadDescriptor reads a characteristic descriptor from a server. [Vol 3, Part G, 4.12.1]
func (cln *Client) ReadDescriptor(ctx context.Context, d *ble.Descriptor) ([]byte, error) {
	cbdsc, err := cln.pc.findCbDsc(d)
	if err != nil {
		return nil, err
	}

	ch := cln.conn.evl.dscRead.Listen()
	defer cln.conn.evl.dscRead.Close()

	cln.conn.prph.ReadDescriptor(cbdsc)

	if err := cln.awaitSlot(ctx, ch); err != nil {
		return nil, err
	}

	d.Value = cbdsc.Value()

	return d.Value, nil
}

// WriteDescriptor writes a characteristic descriptor to a server. [Vol 3, Part G, 4.12.3]
func (cln *Client) WriteDescriptor(ctx context.Context, d *ble.Descriptor, b []byte) error {
	cbdsc, err := cln.pc.findCbDsc(d)
	if err != nil {
		return err
	}

	ch := cln.conn.evl.dscWritten.Listen()
	defer cln.conn.evl.dscWritten.Close()

	cln.conn.prph.WriteDescriptor(b, cbdsc)

	return cln.awaitSlot(ctx, ch)
}

// ReadRSSI retrieves the current RSSI value of the remote peripheral, in
// dBm. [Vol 2, Part E, 7.5.4] ctx bounds this caller's wait; on expiry the
// slot is abandoned and the callback's late result is discarded. (The old
// wrapper goroutine that outlived ctx held its Listen open, and its
// deferred Close then tore down the NEXT ReadRSSI's freshly created slot —
// abandoning in place has no cross-call aftermath.)
func (cln *Client) ReadRSSI(ctx context.Context) (int, error) {
	return cln.conn.readRSSI(ctx)
}

// ExchangeMTU set the ATT_MTU to the maximum possible value that can be
// supported by both devices [Vol 3, Part G, 4.3.1]
func (cln *Client) ExchangeMTU(ctx context.Context, mtu int) (int, error) {
	// TODO: find the xpc command to tell OS X the rxMTU we can handle.
	return cln.conn.TxMTU(), nil
}

// Subscribe subscribes to indication (if ind is set true), or notification of a
// characteristic value. [Vol 3, Part G, 4.10 & 4.11]
func (cln *Client) Subscribe(ctx context.Context, c *ble.Characteristic, ind bool, fn ble.NotificationHandler) error {
	cbchr, err := cln.pc.findCbChr(c)
	if err != nil {
		return err
	}

	cln.conn.addSub(c, fn)

	ch := cln.conn.evl.notifyChanged.Listen()
	defer cln.conn.evl.notifyChanged.Close()

	cln.conn.prph.SetNotify(true, cbchr)

	if err := cln.awaitSlot(ctx, ch); err != nil {
		cln.conn.delSub(c)
		return err
	}

	return nil
}

// Unsubscribe unsubscribes to indication (if ind is set true), or notification
// of a specified characteristic value. [Vol 3, Part G, 4.10 & 4.11]
func (cln *Client) Unsubscribe(ctx context.Context, c *ble.Characteristic, ind bool) error {
	cbchr, err := cln.pc.findCbChr(c)
	if err != nil {
		return err
	}

	ch := cln.conn.evl.notifyChanged.Listen()
	defer cln.conn.evl.notifyChanged.Close()

	cln.conn.prph.SetNotify(false, cbchr)

	if err := cln.awaitSlot(ctx, ch); err != nil {
		return err
	}

	cln.conn.delSub(c)

	return nil
}

// ClearSubscriptions clears all subscriptions to notifications and indications.
func (cln *Client) ClearSubscriptions(ctx context.Context) error {
	// Snapshot under the conn lock: addSub/delSub mutate the map, and
	// Unsubscribe below calls delSub itself.
	cln.conn.RLock()
	subs := make([]*sub, 0, len(cln.conn.subs))
	for _, s := range cln.conn.subs {
		subs = append(subs, s)
	}
	cln.conn.RUnlock()
	for _, s := range subs {
		if err := cln.Unsubscribe(ctx, s.char, false); err != nil {
			return err
		}
	}
	return nil
}

// CancelConnection disconnects the connection.
func (cln *Client) CancelConnection() error {
	cln.cm.CancelConnect(cln.conn.prph)
	return nil
}

// Disconnected returns a receiving channel, which is closed when the client disconnects.
func (cln *Client) Disconnected() <-chan struct{} {
	return cln.conn.Disconnected()
}

// Conn returns the client's current connection.
func (cln *Client) Conn() ble.Conn {
	return cln.conn
}

type sub struct {
	fn   ble.NotificationHandler
	char *ble.Characteristic
}

func (cln *Client) DidDiscoverServices(prph cbgo.Peripheral, err error) {
	cln.conn.evl.svcsDiscovered.RxSignal(err)
}
func (cln *Client) DidDiscoverCharacteristics(prph cbgo.Peripheral, svc cbgo.Service, err error) {
	cln.conn.evl.chrsDiscovered.RxSignal(err)
}
func (cln *Client) DidDiscoverDescriptors(prph cbgo.Peripheral, chr cbgo.Characteristic, err error) {
	cln.conn.evl.dscsDiscovered.RxSignal(err)
}
func (cln *Client) DidUpdateValueForCharacteristic(prph cbgo.Peripheral, chr cbgo.Characteristic, err error) {
	cln.conn.processChrRead(err, chr)
}

func (cln *Client) DidUpdateValueForDescriptor(prph cbgo.Peripheral, dsc cbgo.Descriptor, err error) {
	cln.conn.evl.dscRead.RxSignal(err)
}
func (cln *Client) DidWriteValueForCharacteristic(prph cbgo.Peripheral, chr cbgo.Characteristic, err error) {
	cln.conn.evl.chrWritten.RxSignal(err)
}
func (cln *Client) DidWriteValueForDescriptor(prph cbgo.Peripheral, dsc cbgo.Descriptor, err error) {
	cln.conn.evl.dscWritten.RxSignal(err)
}
func (cln *Client) DidUpdateNotificationState(prph cbgo.Peripheral, chr cbgo.Characteristic, err error) {
	cln.conn.evl.notifyChanged.RxSignal(err)
}
func (cln *Client) DidReadRSSI(prph cbgo.Peripheral, rssi int, err error) {
	cln.conn.evl.rssiRead.RxSignal(&eventRSSIRead{
		err:  err,
		rssi: int(rssi),
	})
}
