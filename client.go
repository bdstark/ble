package ble

import "context"

// A Client is a GATT client.
//
// Methods that take a context.Context use it to bound their waits: the
// request is abandoned and ctx.Err() is returned (unwrapped or wrapped, so
// errors.Is(err, context.Canceled) / context.DeadlineExceeded hold) when the
// context is canceled or its deadline passes. Cancellation is best-effort at
// the transport layer: an in-flight ACL write cannot be interrupted mid-write;
// on the Linux stack it is independently bounded by hci.ACLWriteTimeout.
//
// A request abandoned after it reached the wire still owns the ATT bearer
// (ATT is a sequential protocol). On the Linux stack the next request first
// resolves that transaction — waiting for its late response, or for its
// 30-second spec deadline and closing the bearer on expiry — so cancellation
// never causes a later request to consume a stale response.
//
// Teardown paths (CancelConnection) deliberately take no context so that a
// client can always be torn down, even when no request context is available.
type Client interface {
	// Addr returns platform specific unique ID of the remote peripheral, e.g. MAC on Linux, Client UUID on OS X.
	Addr() Addr

	// Name returns the name of the remote peripheral.
	// This can be the advertised name, if exists, or the GAP device name, which takes priority.
	Name() string

	// Profile returns discovered profile.
	Profile() *Profile

	// DiscoverProfile discovers the whole hierarchy of a server.
	DiscoverProfile(ctx context.Context, force bool) (*Profile, error)

	// DiscoverServices finds all the primary services on a server. [Vol 3, Part G, 4.4.1]
	// If filter is specified, only filtered services are returned.
	DiscoverServices(ctx context.Context, filter []UUID) ([]*Service, error)

	// DiscoverIncludedServices finds the included services of a service. [Vol 3, Part G, 4.5.1]
	// If filter is specified, only filtered services are returned.
	DiscoverIncludedServices(ctx context.Context, filter []UUID, s *Service) ([]*Service, error)

	// DiscoverCharacteristics finds all the characteristics within a service. [Vol 3, Part G, 4.6.1]
	// If filter is specified, only filtered characteristics are returned.
	DiscoverCharacteristics(ctx context.Context, filter []UUID, s *Service) ([]*Characteristic, error)

	// DiscoverDescriptors finds all the descriptors within a characteristic. [Vol 3, Part G, 4.7.1]
	// If filter is specified, only filtered descriptors are returned.
	DiscoverDescriptors(ctx context.Context, filter []UUID, c *Characteristic) ([]*Descriptor, error)

	// ReadCharacteristic reads a characteristic value from a server. [Vol 3, Part G, 4.8.1]
	ReadCharacteristic(ctx context.Context, c *Characteristic) ([]byte, error)

	// ReadLongCharacteristic reads a characteristic value which is longer than the MTU. [Vol 3, Part G, 4.8.3]
	ReadLongCharacteristic(ctx context.Context, c *Characteristic) ([]byte, error)

	// WriteCharacteristic writes a characteristic value to a server. [Vol 3, Part G, 4.9.3]
	WriteCharacteristic(ctx context.Context, c *Characteristic, value []byte, noRsp bool) error

	// ReadDescriptor reads a characteristic descriptor from a server. [Vol 3, Part G, 4.12.1]
	ReadDescriptor(ctx context.Context, d *Descriptor) ([]byte, error)

	// WriteDescriptor writes a characteristic descriptor to a server. [Vol 3, Part G, 4.12.3]
	WriteDescriptor(ctx context.Context, d *Descriptor, v []byte) error

	// ReadRSSI retrieves the current RSSI value of the remote peripheral, in
	// dBm. [Vol 2, Part E, 7.5.4] The underlying command exchange is bounded
	// by the backend's own internal timeout and cannot be interrupted
	// mid-flight; per the contract above, ctx bounds only this caller's wait
	// — on expiry ctx.Err() is returned and the exchange's eventual result
	// is discarded.
	ReadRSSI(ctx context.Context) (int, error)

	// ExchangeMTU set the ATT_MTU to the maximum possible value that can be supported by both devices [Vol 3, Part G, 4.3.1]
	ExchangeMTU(ctx context.Context, rxMTU int) (txMTU int, err error)

	// Subscribe subscribes to indication (if ind is set true), or notification of a characteristic value. [Vol 3, Part G, 4.10 & 4.11]
	Subscribe(ctx context.Context, c *Characteristic, ind bool, h NotificationHandler) error

	// Unsubscribe unsubscribes to indication (if ind is set true), or notification of a specified characteristic value. [Vol 3, Part G, 4.10 & 4.11]
	Unsubscribe(ctx context.Context, c *Characteristic, ind bool) error

	// ClearSubscriptions clears all subscriptions to notifications and indications.
	ClearSubscriptions(ctx context.Context) error

	// CancelConnection disconnects the connection.
	CancelConnection() error

	// Disconnected returns a receiving channel, which is closed when the client disconnects.
	Disconnected() <-chan struct{}

	// Conn returns the client's current connection.
	Conn() Conn
}
