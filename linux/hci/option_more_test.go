package hci

import (
	"errors"
	"strings"
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

// TestOptionRoleUnsupported pins that the role options — silent successes
// before the fix — now surface hci's "not supported" as the typed sentinel
// through HCI.Option.
func TestOptionRoleUnsupported(t *testing.T) {
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
		err := h.Option(tc.opt)
		if !errors.Is(err, ble.ErrUnsupportedOption) {
			t.Errorf("%s: err = %v, want ErrUnsupportedOption", tc.name, err)
		}
		if err != nil && !strings.Contains(err.Error(), tc.name) {
			t.Errorf("%s: error does not name the option: %v", tc.name, err)
		}
	}
}

// TestApplyOptionsMixedOnHCI applies a mix of valid and invalid options to a
// real HCI via ble.ApplyOptions: both failures survive in the joined error
// (each named, each errors.Is-matchable) and the valid option still lands.
func TestApplyOptionsMixedOnHCI(t *testing.T) {
	h, err := NewHCI()
	if err != nil {
		t.Fatal(err)
	}
	err = ble.ApplyOptions(h,
		ble.OptPeripheralRole(), // invalid on hci
		ble.OptDeviceID(7),      // valid
		ble.OptCentralRole(),    // invalid on hci
	)
	if err == nil {
		t.Fatal("ApplyOptions: err = nil, want joined unsupported-option errors")
	}
	if !errors.Is(err, ble.ErrUnsupportedOption) {
		t.Errorf("joined error not matchable as ErrUnsupportedOption: %v", err)
	}
	for _, want := range []string{"SetPeripheralRole", "SetCentralRole"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error missing %s: %v", want, err)
		}
	}
	if h.id != 7 {
		t.Errorf("valid option not applied alongside failures: id = %d, want 7", h.id)
	}
}

// TestNewHCIUnsupportedRoleOption pins that an unsupported option passed at
// construction fails NewHCI with the sentinel intact through the wrap.
func TestNewHCIUnsupportedRoleOption(t *testing.T) {
	_, err := NewHCI(ble.OptPeripheralRole())
	if !errors.Is(err, ble.ErrUnsupportedOption) {
		t.Fatalf("NewHCI(OptPeripheralRole): err = %v, want ErrUnsupportedOption", err)
	}
}
