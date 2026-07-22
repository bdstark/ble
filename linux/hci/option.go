package hci

import (
	"time"

	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
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
	h.params.Lock()
	h.params.connParams = param
	h.params.Unlock()
	return nil
}

// SetScanParams overrides default scanning parameters.
func (h *HCI) SetScanParams(param cmd.LESetScanParameters) error {
	h.params.Lock()
	h.params.scanParams = param
	h.params.Unlock()
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
	h.params.Lock()
	h.params.advParams = param
	h.params.Unlock()
	return nil
}

// SetPeripheralRole is a no-op: the HCI backend advertises and accepts
// connections as well as scanning and dialing, so both roles are always
// available and requesting one needs no configuration. Returns nil to
// match the darwin backend (which likewise runs both managers).
func (h *HCI) SetPeripheralRole() error {
	return nil
}

// SetCentralRole is a no-op for the same reason as SetPeripheralRole.
func (h *HCI) SetCentralRole() error {
	return nil
}
