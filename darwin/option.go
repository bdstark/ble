//go:build darwin

package darwin

import (
	"fmt"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
)

// The darwin backend drives CoreBluetooth through cbgo; the HCI-level knobs
// below have no CoreBluetooth equivalent (the OS owns the controller), so
// their setters report ble.ErrUnsupportedOption wrapped with the option name
// instead of silently no-opping. Now that Opt* closures propagate setter
// errors, a consumer setting a linux-only option on darwin finds out.

// SetConnectedHandler sets handler to be called when new connection is established.
func (d *Device) SetConnectedHandler(f func(evt.LEConnectionComplete)) error {
	return fmt.Errorf("darwin: SetConnectedHandler: %w", ble.ErrUnsupportedOption)
}

// SetDisconnectedHandler sets handler to be called on disconnect.
func (d *Device) SetDisconnectedHandler(f func(evt.DisconnectionComplete)) error {
	return fmt.Errorf("darwin: SetDisconnectedHandler: %w", ble.ErrUnsupportedOption)
}

// SetPeripheralRole configures the device to perform Peripheral tasks.
// The darwin Device always runs both a central and a peripheral manager
// (see NewDevice), so the request is already satisfied.
func (d *Device) SetPeripheralRole() error {
	return nil
}

// SetCentralRole configures the device to perform Central tasks.
// Honored: the central manager is always active on darwin.
func (d *Device) SetCentralRole() error {
	return nil
}

// SetDeviceID sets HCI device ID.
func (d *Device) SetDeviceID(id int) error {
	return fmt.Errorf("darwin: SetDeviceID: %w", ble.ErrUnsupportedOption)
}

// SetDialerTimeout sets dialing timeout for Dialer.
func (d *Device) SetDialerTimeout(dur time.Duration) error {
	return fmt.Errorf("darwin: SetDialerTimeout: %w", ble.ErrUnsupportedOption)
}

// SetListenerTimeout sets dialing timeout for Listener.
func (d *Device) SetListenerTimeout(dur time.Duration) error {
	return fmt.Errorf("darwin: SetListenerTimeout: %w", ble.ErrUnsupportedOption)
}

// SetConnParams overrides default connection parameters.
func (d *Device) SetConnParams(param cmd.LECreateConnection) error {
	return fmt.Errorf("darwin: SetConnParams: %w", ble.ErrUnsupportedOption)
}

// SetScanParams overrides default scanning parameters.
func (d *Device) SetScanParams(param cmd.LESetScanParameters) error {
	return fmt.Errorf("darwin: SetScanParams: %w", ble.ErrUnsupportedOption)
}

// SetAdvParams overrides default advertising parameters.
func (d *Device) SetAdvParams(param cmd.LESetAdvertisingParameters) error {
	return fmt.Errorf("darwin: SetAdvParams: %w", ble.ErrUnsupportedOption)
}
