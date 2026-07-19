package darwin

import (
	"errors"
	"testing"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/hci/cmd"
)

// The unsupported setters never dereference their receiver, so a nil Device
// exercises them without CoreBluetooth. Each must surface
// ble.ErrUnsupportedOption; the role setters are honored no-ops (NewDevice
// always runs both a central and a peripheral manager).
func TestUnsupportedSettersReturnTypedError(t *testing.T) {
	var d *Device
	for name, err := range map[string]error{
		"SetDeviceID":            d.SetDeviceID(1),
		"SetDialerTimeout":       d.SetDialerTimeout(time.Second),
		"SetListenerTimeout":     d.SetListenerTimeout(time.Second),
		"SetConnParams":          d.SetConnParams(cmd.LECreateConnection{}),
		"SetScanParams":          d.SetScanParams(cmd.LESetScanParameters{}),
		"SetAdvParams":           d.SetAdvParams(cmd.LESetAdvertisingParameters{}),
		"SetConnectedHandler":    d.SetConnectedHandler(nil),
		"SetDisconnectedHandler": d.SetDisconnectedHandler(nil),
	} {
		if !errors.Is(err, ble.ErrUnsupportedOption) {
			t.Errorf("%s = %v, want ErrUnsupportedOption", name, err)
		}
	}
	if err := d.SetPeripheralRole(); err != nil {
		t.Errorf("SetPeripheralRole = %v, want nil", err)
	}
	if err := d.SetCentralRole(); err != nil {
		t.Errorf("SetCentralRole = %v, want nil", err)
	}
}
