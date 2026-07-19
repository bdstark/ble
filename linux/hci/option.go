package hci

import (
	"fmt"
	"time"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/linux/hci/cmd"
	"github.com/go-ble/ble/linux/hci/evt"
)

// SetDeviceID sets HCI device ID.
func (h *HCI) SetDeviceID(id int) error {
	h.id = id
	return nil
}

// SetDialerTimeout sets dialing timeout for Dialer.
func (h *HCI) SetDialerTimeout(d time.Duration) error {
	h.dialerTmo = d
	return nil
}

// SetListenerTimeout sets dialing timeout for Listener.
func (h *HCI) SetListenerTimeout(d time.Duration) error {
	h.listenerTmo = d
	return nil
}

// SetConnParams overrides default connection parameters.
func (h *HCI) SetConnParams(param cmd.LECreateConnection) error {
	h.params.connParams = param
	return nil
}

// SetScanParams overrides default scanning parameters.
func (h *HCI) SetScanParams(param cmd.LESetScanParameters) error {
	h.params.scanParams = param
	return nil
}

// SetConnectedHandler sets handler to be called when new connection is established.
func (h *HCI) SetConnectedHandler(f func(complete evt.LEConnectionComplete)) error {
	h.connectedHandler = f
	return nil
}

// SetDisconnectedHandler sets handler to be called on disconnect.
func (h *HCI) SetDisconnectedHandler(f func(evt.DisconnectionComplete)) error {
	h.disconnectedHandler = f
	return nil
}

// SetAdvParams overrides default advertising parameters.
func (h *HCI) SetAdvParams(param cmd.LESetAdvertisingParameters) error {
	h.params.advParams = param
	return nil
}

// SetPeripheralRole is not supported on the HCI backend; the role is fixed
// by how the device is used. The error now propagates through Opt* closures
// so callers find out instead of silently succeeding.
func (h *HCI) SetPeripheralRole() error {
	return fmt.Errorf("hci: SetPeripheralRole: %w", ble.ErrUnsupportedOption)
}

// SetCentralRole is not supported on the HCI backend; see SetPeripheralRole.
func (h *HCI) SetCentralRole() error {
	return fmt.Errorf("hci: SetCentralRole: %w", ble.ErrUnsupportedOption)
}
