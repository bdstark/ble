package ble

import (
	"errors"
	"time"

	"github.com/bdstark/ble/linux/hci/cmd"
	"github.com/bdstark/ble/linux/hci/evt"
)

// ErrUnsupportedOption is returned (wrapped with the option name) by a
// backend's setter when it cannot honor the option — e.g. setting a
// linux-only HCI option on the darwin backend. Match with
// errors.Is(err, ble.ErrUnsupportedOption).
var ErrUnsupportedOption = errors.New("unsupported option")

// DeviceOption is an interface which the device should implement to allow using configuration options
type DeviceOption interface {
	SetDeviceID(int) error
	SetDialerTimeout(time.Duration) error
	SetListenerTimeout(time.Duration) error
	SetConnParams(cmd.LECreateConnection) error
	SetScanParams(cmd.LESetScanParameters) error
	SetAdvParams(cmd.LESetAdvertisingParameters) error
	SetConnectedHandler(f func(evt.LEConnectionComplete)) error
	SetDisconnectedHandler(f func(evt.DisconnectionComplete)) error
	SetPeripheralRole() error
	SetCentralRole() error
}

// An Option is a configuration function, which configures the device.
type Option func(DeviceOption) error

// ApplyOptions applies every option to dev and returns the collected
// errors joined with errors.Join — it does not stop at the first failure,
// so a caller passing several options learns about every one that was
// rejected, and errors.Is still matches each individual cause.
// Backends' Option methods and device constructors should apply user
// options through this helper instead of hand-rolled loops that keep only
// the last error.
func ApplyOptions(dev DeviceOption, opts ...Option) error {
	var errs []error
	for _, opt := range opts {
		if err := opt(dev); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// OptDeviceID sets HCI device ID.
func OptDeviceID(id int) Option {
	return func(opt DeviceOption) error {
		return opt.SetDeviceID(id)
	}
}

// OptDialerTimeout sets dialing timeout for Dialer.
func OptDialerTimeout(d time.Duration) Option {
	return func(opt DeviceOption) error {
		return opt.SetDialerTimeout(d)
	}
}

// OptListenerTimeout sets dialing timeout for Listener.
func OptListenerTimeout(d time.Duration) Option {
	return func(opt DeviceOption) error {
		return opt.SetListenerTimeout(d)
	}
}

// OptConnParams overrides default connection parameters.
func OptConnParams(param cmd.LECreateConnection) Option {
	return func(opt DeviceOption) error {
		return opt.SetConnParams(param)
	}
}

// OptScanParams overrides default scanning parameters.
func OptScanParams(param cmd.LESetScanParameters) Option {
	return func(opt DeviceOption) error {
		return opt.SetScanParams(param)
	}
}

// OptAdvParams overrides default advertising parameters.
func OptAdvParams(param cmd.LESetAdvertisingParameters) Option {
	return func(opt DeviceOption) error {
		return opt.SetAdvParams(param)
	}
}

func OptConnectHandler(f func(evt.LEConnectionComplete)) Option {
	return func(opt DeviceOption) error {
		return opt.SetConnectedHandler(f)
	}
}

func OptDisconnectHandler(f func(evt.DisconnectionComplete)) Option {
	return func(opt DeviceOption) error {
		return opt.SetDisconnectedHandler(f)
	}
}

// OptPeripheralRole configures the device to perform Peripheral tasks.
func OptPeripheralRole() Option {
	return func(opt DeviceOption) error {
		return opt.SetPeripheralRole()
	}
}

// OptCentralRole configures the device to perform Central tasks.
func OptCentralRole() Option {
	return func(opt DeviceOption) error {
		return opt.SetCentralRole()
	}
}
