package ble

import (
	"context"
	"errors"
	"testing"
	"time"
)

// stubAdv is a minimal Advertisement for exercising Connect's matching path.
type stubAdv struct{ addr Addr }

func (a *stubAdv) LocalName() string          { return "stub" }
func (a *stubAdv) ManufacturerData() []byte   { return nil }
func (a *stubAdv) ServiceData() []ServiceData { return nil }
func (a *stubAdv) Services() []UUID           { return nil }
func (a *stubAdv) OverflowService() []UUID    { return nil }
func (a *stubAdv) TxPowerLevel() int          { return 0 }
func (a *stubAdv) Connectable() bool          { return true }
func (a *stubAdv) SolicitedService() []UUID   { return nil }
func (a *stubAdv) RSSI() int                  { return -40 }
func (a *stubAdv) Addr() Addr                 { return a.addr }

// stubDevice implements Device the way Connect uses it: Scan feeds the
// handler (or just waits) until its context ends, Dial records the address.
type stubDevice struct {
	advs   []Advertisement // delivered repeatedly to the scan handler
	dialed chan Addr
	dialer func(ctx context.Context, a Addr) (Client, error)
}

func (d *stubDevice) AddService(svc *Service) error     { return ErrNotImplemented }
func (d *stubDevice) RemoveAllServices() error          { return ErrNotImplemented }
func (d *stubDevice) SetServices(svcs []*Service) error { return ErrNotImplemented }
func (d *stubDevice) Stop() error                       { return nil }
func (d *stubDevice) Advertise(ctx context.Context, adv Advertisement) error {
	return ErrNotImplemented
}
func (d *stubDevice) AdvertiseNameAndServices(ctx context.Context, name string, uuids ...UUID) error {
	return ErrNotImplemented
}
func (d *stubDevice) AdvertiseMfgData(ctx context.Context, id uint16, b []byte) error {
	return ErrNotImplemented
}
func (d *stubDevice) AdvertiseServiceData16(ctx context.Context, id uint16, b []byte) error {
	return ErrNotImplemented
}
func (d *stubDevice) AdvertiseIBeaconData(ctx context.Context, b []byte) error {
	return ErrNotImplemented
}
func (d *stubDevice) AdvertiseIBeacon(ctx context.Context, u UUID, major, minor uint16, pwr int8) error {
	return ErrNotImplemented
}

func (d *stubDevice) Scan(ctx context.Context, allowDup bool, h AdvHandler) error {
	for {
		for _, a := range d.advs {
			h(a)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (d *stubDevice) Dial(ctx context.Context, a Addr) (Client, error) {
	if d.dialed != nil {
		d.dialed <- a
	}
	if d.dialer != nil {
		return d.dialer(ctx, a)
	}
	return nil, nil
}

// TestConnectNoMatchReturnsPromptly pins the fix for a live-hit wedge: when
// the parent context expired before any advertisement matched, the old
// Connect interpreted the resulting context.Canceled from Scan as a match
// and blocked forever receiving from a channel nobody writes to.
func TestConnectNoMatchReturnsPromptly(t *testing.T) {
	SetDefaultDevice(&stubDevice{}) // scans forever, never delivers a matching adv
	defer SetDefaultDevice(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	type result struct {
		cln Client
		err error
	}
	done := make(chan result, 1)
	go func() {
		cln, err := Connect(ctx, func(a Advertisement) bool { return false })
		done <- result{cln, err}
	}()

	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("Connect returned nil error with no matching advertisement")
		}
		if !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatalf("Connect error = %v, want errors.Is(..., context.DeadlineExceeded)", r.err)
		}
		if r.cln != nil {
			t.Fatalf("Connect returned a client (%v) with no match", r.cln)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return after the parent context expired (pre-fix wedge)")
	}
}

// TestConnectMatchDials verifies the happy path: a matching advertisement
// stops the scan and its address is dialed, even when the scanner keeps
// producing further matches (the handler must not block or leak on them).
func TestConnectMatchDials(t *testing.T) {
	want := NewAddr("11:22:33:44:55:66")
	dev := &stubDevice{
		advs:   []Advertisement{&stubAdv{addr: want}, &stubAdv{addr: NewAddr("aa:bb:cc:dd:ee:ff")}},
		dialed: make(chan Addr, 1),
	}
	SetDefaultDevice(dev)
	defer SetDefaultDevice(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := Connect(ctx, func(a Advertisement) bool { return true }); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case got := <-dev.dialed:
		if got.String() != want.String() {
			t.Fatalf("dialed %v, want the first matching advertisement %v", got, want)
		}
	default:
		t.Fatal("Connect reported success without dialing")
	}
}
