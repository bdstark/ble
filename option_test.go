package ble

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
)

// fakeOptions is a minimal DeviceOption implementation that records which
// setter was called with what value and returns a configurable per-setter
// error, so the Opt* closures can be pinned to propagate — not discard —
// setter errors.
type fakeOptions struct {
	errs  map[string]error // setter name -> error to return
	calls []string

	deviceID            int
	dialerTmo           time.Duration
	listenerTmo         time.Duration
	connParams          cmd.LECreateConnection
	scanParams          cmd.LESetScanParameters
	advParams           cmd.LESetAdvertisingParameters
	connectedHandler    func(evt.LEConnectionComplete)
	disconnectedHandler func(evt.DisconnectionComplete)
}

func (f *fakeOptions) done(name string) error {
	f.calls = append(f.calls, name)
	return f.errs[name]
}

func (f *fakeOptions) SetDeviceID(id int) error {
	f.deviceID = id
	return f.done("SetDeviceID")
}

func (f *fakeOptions) SetDialerTimeout(d time.Duration) error {
	f.dialerTmo = d
	return f.done("SetDialerTimeout")
}

func (f *fakeOptions) SetListenerTimeout(d time.Duration) error {
	f.listenerTmo = d
	return f.done("SetListenerTimeout")
}

func (f *fakeOptions) SetConnParams(p cmd.LECreateConnection) error {
	f.connParams = p
	return f.done("SetConnParams")
}

func (f *fakeOptions) SetScanParams(p cmd.LESetScanParameters) error {
	f.scanParams = p
	return f.done("SetScanParams")
}

func (f *fakeOptions) SetAdvParams(p cmd.LESetAdvertisingParameters) error {
	f.advParams = p
	return f.done("SetAdvParams")
}

func (f *fakeOptions) SetConnectedHandler(h func(evt.LEConnectionComplete)) error {
	f.connectedHandler = h
	return f.done("SetConnectedHandler")
}

func (f *fakeOptions) SetDisconnectedHandler(h func(evt.DisconnectionComplete)) error {
	f.disconnectedHandler = h
	return f.done("SetDisconnectedHandler")
}

func (f *fakeOptions) SetPeripheralRole() error { return f.done("SetPeripheralRole") }
func (f *fakeOptions) SetCentralRole() error    { return f.done("SetCentralRole") }

// optCases covers every Opt* constructor with the setter it must invoke.
func optCases() []struct {
	setter string
	opt    Option
} {
	return []struct {
		setter string
		opt    Option
	}{
		{"SetDeviceID", OptDeviceID(3)},
		{"SetDialerTimeout", OptDialerTimeout(4 * time.Second)},
		{"SetListenerTimeout", OptListenerTimeout(5 * time.Second)},
		{"SetConnParams", OptConnParams(cmd.LECreateConnection{ConnIntervalMin: 0x18})},
		{"SetScanParams", OptScanParams(cmd.LESetScanParameters{LEScanInterval: 0x60})},
		{"SetAdvParams", OptAdvParams(cmd.LESetAdvertisingParameters{AdvertisingIntervalMin: 0xA0})},
		{"SetConnectHandler", OptConnectHandler(func(evt.LEConnectionComplete) {})},
		{"SetDisconnectHandler", OptDisconnectHandler(func(evt.DisconnectionComplete) {})},
		{"SetPeripheralRole", OptPeripheralRole()},
		{"SetCentralRole", OptCentralRole()},
	}
}

// setterFor maps the case label to the fake's recorded setter name (the two
// handler options drop the "ed" suffix in their constructor names).
func setterFor(label string) string {
	switch label {
	case "SetConnectHandler":
		return "SetConnectedHandler"
	case "SetDisconnectHandler":
		return "SetDisconnectedHandler"
	}
	return label
}

// TestOptPropagatesSetterError pins the core fix: every Opt* closure returns
// its setter's error instead of discarding it.
func TestOptPropagatesSetterError(t *testing.T) {
	for _, tc := range optCases() {
		t.Run(tc.setter, func(t *testing.T) {
			boom := errors.New("boom")
			f := &fakeOptions{errs: map[string]error{setterFor(tc.setter): boom}}
			if err := tc.opt(f); !errors.Is(err, boom) {
				t.Fatalf("%s option: err = %v, want %v", tc.setter, err, boom)
			}
		})
	}
}

// TestOptCallsSetterWithValue verifies each Opt* still invokes the right
// setter with the value it was constructed with, and returns nil on success.
func TestOptCallsSetterWithValue(t *testing.T) {
	f := &fakeOptions{}
	for _, tc := range optCases() {
		if err := tc.opt(f); err != nil {
			t.Fatalf("%s option: err = %v, want nil", tc.setter, err)
		}
	}
	if len(f.calls) != 10 {
		t.Fatalf("setter calls = %d (%v), want 10", len(f.calls), f.calls)
	}
	if f.deviceID != 3 {
		t.Errorf("deviceID = %d, want 3", f.deviceID)
	}
	if f.dialerTmo != 4*time.Second {
		t.Errorf("dialerTmo = %v, want 4s", f.dialerTmo)
	}
	if f.listenerTmo != 5*time.Second {
		t.Errorf("listenerTmo = %v, want 5s", f.listenerTmo)
	}
	if f.connParams.ConnIntervalMin != 0x18 {
		t.Errorf("connParams.ConnIntervalMin = %#x, want 0x18", f.connParams.ConnIntervalMin)
	}
	if f.scanParams.LEScanInterval != 0x60 {
		t.Errorf("scanParams.LEScanInterval = %#x, want 0x60", f.scanParams.LEScanInterval)
	}
	if f.advParams.AdvertisingIntervalMin != 0xA0 {
		t.Errorf("advParams.AdvertisingIntervalMin = %#x, want 0xA0", f.advParams.AdvertisingIntervalMin)
	}
	if f.connectedHandler == nil {
		t.Errorf("connectedHandler not stored")
	}
	if f.disconnectedHandler == nil {
		t.Errorf("disconnectedHandler not stored")
	}
}

// TestApplyOptionsJoinsAllErrors pins the apply-loop fix: every failing
// option's error survives (not just the last), each is errors.Is-matchable
// through the join, and options after a failure are still applied.
func TestApplyOptionsJoinsAllErrors(t *testing.T) {
	errID := errors.New("id rejected")
	errTmo := errors.New("timeout rejected")
	f := &fakeOptions{errs: map[string]error{
		"SetDeviceID":      errID,
		"SetDialerTimeout": errTmo,
	}}

	err := ApplyOptions(f,
		OptDeviceID(1),
		OptListenerTimeout(time.Second), // valid, between the two failures
		OptDialerTimeout(2*time.Second),
	)
	if err == nil {
		t.Fatal("ApplyOptions: err = nil, want joined errors")
	}
	if !errors.Is(err, errID) {
		t.Errorf("joined error lost first failure: %v", err)
	}
	if !errors.Is(err, errTmo) {
		t.Errorf("joined error lost second failure: %v", err)
	}
	if f.listenerTmo != time.Second {
		t.Errorf("valid option after a failure not applied: listenerTmo = %v", f.listenerTmo)
	}
	if len(f.calls) != 3 {
		t.Errorf("setter calls = %v, want all 3 options attempted", f.calls)
	}
}

// TestApplyOptionsNilOnSuccess: all-valid options yield a nil error.
func TestApplyOptionsNilOnSuccess(t *testing.T) {
	f := &fakeOptions{}
	if err := ApplyOptions(f, OptDeviceID(2), OptDialerTimeout(time.Second)); err != nil {
		t.Fatalf("ApplyOptions: err = %v, want nil", err)
	}
}

// TestErrUnsupportedOptionMatchable pins the sentinel contract: a setter
// wrapping ErrUnsupportedOption with the option name stays matchable through
// both the wrap and an ApplyOptions join.
func TestErrUnsupportedOptionMatchable(t *testing.T) {
	wrapped := fmt.Errorf("darwin: SetDialerTimeout: %w", ErrUnsupportedOption)
	if !errors.Is(wrapped, ErrUnsupportedOption) {
		t.Fatalf("wrapped sentinel not matchable: %v", wrapped)
	}
	f := &fakeOptions{errs: map[string]error{"SetDialerTimeout": wrapped}}
	err := ApplyOptions(f, OptDeviceID(1), OptDialerTimeout(time.Second))
	if !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("sentinel not matchable through join: %v", err)
	}
}
