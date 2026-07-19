package hci

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/linux/hci/cmd"
	"github.com/go-ble/ble/linux/hci/evt"
)

// ---------------------------------------------------------------------------
// Option application against a real HCI
// ---------------------------------------------------------------------------

// TestOptionSettersApply drives every supported ble.Opt* through HCI.Option
// and verifies the value actually lands on the HCI — pinning that the Opt*
// closures call through to the setters and report success only when the
// setter succeeded.
func TestOptionSettersApply(t *testing.T) {
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}

	connected := func(evt.LEConnectionComplete) {}
	disconnected := func(evt.DisconnectionComplete) {}
	err = h.Option(
		ble.OptDeviceID(2),
		ble.OptDialerTimeout(4*time.Second),
		ble.OptListenerTimeout(5*time.Second),
		ble.OptConnParams(cmd.LECreateConnection{ConnIntervalMin: 0x18}),
		ble.OptScanParams(cmd.LESetScanParameters{LEScanInterval: 0x60}),
		ble.OptAdvParams(cmd.LESetAdvertisingParameters{AdvertisingIntervalMin: 0xA0}),
		ble.OptConnectHandler(connected),
		ble.OptDisconnectHandler(disconnected),
	)
	if err != nil {
		t.Fatalf("Option with supported options: err = %v, want nil", err)
	}
	if h.id != 2 {
		t.Errorf("id = %d, want 2", h.id)
	}
	if h.dialerTmo != 4*time.Second {
		t.Errorf("dialerTmo = %v, want 4s", h.dialerTmo)
	}
	if h.listenerTmo != 5*time.Second {
		t.Errorf("listenerTmo = %v, want 5s", h.listenerTmo)
	}
	if h.params.connParams.ConnIntervalMin != 0x18 {
		t.Errorf("connParams.ConnIntervalMin = %#x, want 0x18", h.params.connParams.ConnIntervalMin)
	}
	if h.params.scanParams.LEScanInterval != 0x60 {
		t.Errorf("scanParams.LEScanInterval = %#x, want 0x60", h.params.scanParams.LEScanInterval)
	}
	if h.params.advParams.AdvertisingIntervalMin != 0xA0 {
		t.Errorf("advParams.AdvertisingIntervalMin = %#x, want 0xA0", h.params.advParams.AdvertisingIntervalMin)
	}
	if h.connectedHandler == nil {
		t.Errorf("connectedHandler not set")
	}
	if h.disconnectedHandler == nil {
		t.Errorf("disconnectedHandler not set")
	}
}

// TestOptionRoleNoOp pins that the role options are honored no-ops on the
// HCI backend: it advertises and accepts connections as well as scanning and
// dialing, so both roles are always available. Requesting one must not fail
// device construction (it did briefly when the setters returned the
// unsupported-option sentinel), matching the darwin backend.
func TestOptionRoleNoOp(t *testing.T) {
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		opt  ble.Option
	}{
		{"SetPeripheralRole", ble.OptPeripheralRole()},
		{"SetCentralRole", ble.OptCentralRole()},
	} {
		if err := h.Option(tc.opt); err != nil {
			t.Errorf("%s: err = %v, want nil", tc.name, err)
		}
	}
}

// TestApplyOptionsMixedOnHCI applies a mix of valid and failing options to a
// real HCI via ble.ApplyOptions: every failure survives in the joined error
// (each errors.Is-matchable) and the valid option still lands. Synthetic
// failing options stand in because no real HCI setter is unsupported.
func TestApplyOptionsMixedOnHCI(t *testing.T) {
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}
	errA := fmt.Errorf("option A: %w", ble.ErrUnsupportedOption)
	errB := fmt.Errorf("option B: %w", ble.ErrUnsupportedOption)
	err = ble.ApplyOptions(h,
		func(ble.DeviceOption) error { return errA },
		ble.OptDeviceID(7), // valid
		func(ble.DeviceOption) error { return errB },
	)
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Errorf("joined error lost a failure: %v", err)
	}
	if !errors.Is(err, ble.ErrUnsupportedOption) {
		t.Errorf("joined error not matchable as ErrUnsupportedOption: %v", err)
	}
	if h.id != 7 {
		t.Errorf("valid option not applied alongside failures: id = %d, want 7", h.id)
	}
}

// TestNewHCIRoleOptionSucceeds pins that a role option passed at construction
// does not fail NewHCI.
func TestNewHCIRoleOptionSucceeds(t *testing.T) {
	h, err := NewHCI(ble.OptPeripheralRole())
	if err != nil {
		t.Fatalf("NewHCI(OptPeripheralRole): err = %v, want nil", err)
	}
	_ = h
}
