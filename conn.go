package ble

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrInvalidConnParams is returned by ConnParams.Encode (and, wrapped, by
// Conn.UpdateParams) when a requested connection parameter falls outside the
// range the Bluetooth spec permits, or the fields are mutually inconsistent.
var ErrInvalidConnParams = errors.New("invalid connection parameters")

// ErrInvalidDataLength is returned by ValidateDataLength (and, wrapped, by
// Conn.SetDataLength) when a requested LE data-length parameter falls outside
// the range the Bluetooth spec permits.
var ErrInvalidDataLength = errors.New("invalid data length parameters")

// LE Data Length Extension permitted ranges for the host's preferred maximum
// transmission [Vol 6, Part B, 4.5.10]. TxOctets is a link-layer payload size
// in octets; TxTime is the corresponding air time in microseconds. The
// controller clamps the request to what it and the peer support, then reports
// the negotiated maximums in an LE Data Length Change event.
const (
	DataLengthMinTxOctets = 27    // minimum supported PDU payload
	DataLengthMaxTxOctets = 251   // maximum supported PDU payload
	DataLengthMinTxTime   = 328   // air time for a 27-octet PDU (µs)
	DataLengthMaxTxTime   = 17040 // air time for a 251-octet coded-PHY PDU (µs)
)

// ValidateDataLength reports whether txOctets and txTime are within the ranges
// [Vol 6, Part B, 4.5.10] accepts for LE Set Data Length: TxOctets in
// [27, 251], TxTime in [328, 17040] µs. An out-of-range field returns
// ErrInvalidDataLength (wrapped, with detail); nil means the pair is valid.
// Pass DataLengthMaxTxOctets / DataLengthMaxTxTime (251 / 17040) to request the
// controller's ceiling.
func ValidateDataLength(txOctets, txTime uint16) error {
	switch {
	case txOctets < DataLengthMinTxOctets || txOctets > DataLengthMaxTxOctets:
		return fmt.Errorf("tx octets %d out of range [%d, %d]: %w", txOctets, DataLengthMinTxOctets, DataLengthMaxTxOctets, ErrInvalidDataLength)
	case txTime < DataLengthMinTxTime || txTime > DataLengthMaxTxTime:
		return fmt.Errorf("tx time %d out of range [%d, %d]: %w", txTime, DataLengthMinTxTime, DataLengthMaxTxTime, ErrInvalidDataLength)
	}
	return nil
}

// Bluetooth LE connection-parameter units and permitted ranges
// [Vol 6, Part B, 4.5]. Connection interval is expressed in 1.25 ms steps,
// supervision timeout in 10 ms steps.
const (
	connIntervalUnit       = 1250 * time.Microsecond // 1.25 ms
	supervisionTimeoutUnit = 10 * time.Millisecond   // 10 ms

	connIntervalMinUnits = 6    // 7.5 ms
	connIntervalMaxUnits = 3200 // 4 s
	connLatencyMax       = 499
	supervisionMinUnits  = 10   // 100 ms
	supervisionMaxUnits  = 3200 // 32 s
)

// ConnParams describes a requested LE connection-parameter update in human
// units. It is converted to the controller's integer units by Encode.
//
// Valid ranges [Vol 6, Part B, 4.5]:
//   - IntervalMin / IntervalMax: 7.5 ms to 4 s (encoded in 1.25 ms steps),
//     with IntervalMin <= IntervalMax.
//   - Latency: 0 to 499 connection events the peripheral may skip.
//   - Timeout: 100 ms to 32 s (encoded in 10 ms steps), and large enough that
//     Timeout > (1 + Latency) * IntervalMax * 2.
//
// Durations are rounded to the nearest encoding step.
type ConnParams struct {
	IntervalMin time.Duration
	IntervalMax time.Duration
	Latency     int
	Timeout     time.Duration
}

// durUnits rounds d to the nearest whole multiple of unit. A negative d (a
// caller error) yields -1 so the range checks in Encode reject it rather than
// wrapping to a large uint16.
func durUnits(d, unit time.Duration) int {
	if d < 0 {
		return -1
	}
	return int((d + unit/2) / unit)
}

// Encode validates p against the permitted ranges and converts it to the
// controller's integer units: connection intervals in 1.25 ms steps, timeout
// in 10 ms steps, latency as a raw connection-event count. Any violation
// returns ErrInvalidConnParams (wrapped, with detail) and zero values.
func (p ConnParams) Encode() (intervalMin, intervalMax, latency, timeout uint16, err error) {
	imin := durUnits(p.IntervalMin, connIntervalUnit)
	imax := durUnits(p.IntervalMax, connIntervalUnit)
	tmo := durUnits(p.Timeout, supervisionTimeoutUnit)

	switch {
	case imin < connIntervalMinUnits || imin > connIntervalMaxUnits:
		err = fmt.Errorf("connection interval min %v out of range [7.5ms, 4s]: %w", p.IntervalMin, ErrInvalidConnParams)
	case imax < connIntervalMinUnits || imax > connIntervalMaxUnits:
		err = fmt.Errorf("connection interval max %v out of range [7.5ms, 4s]: %w", p.IntervalMax, ErrInvalidConnParams)
	case imin > imax:
		err = fmt.Errorf("connection interval min %v exceeds max %v: %w", p.IntervalMin, p.IntervalMax, ErrInvalidConnParams)
	case p.Latency < 0 || p.Latency > connLatencyMax:
		err = fmt.Errorf("slave latency %d out of range [0, %d]: %w", p.Latency, connLatencyMax, ErrInvalidConnParams)
	case tmo < supervisionMinUnits || tmo > supervisionMaxUnits:
		err = fmt.Errorf("supervision timeout %v out of range [100ms, 32s]: %w", p.Timeout, ErrInvalidConnParams)
	case tmo*4 <= (1+p.Latency)*imax:
		// Spec constraint timeout_ms > (1+latency)*intervalMax_ms*2, reduced
		// to integer units: timeout_u*10 > (1+latency)*imax_u*1.25*2, i.e.
		// timeout_u*4 > (1+latency)*imax_u.
		err = fmt.Errorf("supervision timeout %v too small for interval/latency (need > (1+latency)*intervalMax*2): %w", p.Timeout, ErrInvalidConnParams)
	}
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return uint16(imin), uint16(imax), uint16(p.Latency), uint16(tmo), nil
}

// Conn implements a L2CAP connection.
type Conn interface {
	io.ReadWriteCloser

	// Context returns the context that is used by this Conn.
	Context() context.Context

	// SetContext sets the context that is used by this Conn.
	SetContext(ctx context.Context)

	// LocalAddr returns local device's address.
	LocalAddr() Addr

	// RemoteAddr returns remote device's address.
	RemoteAddr() Addr

	// RxMTU returns the ATT_MTU which the local device is capable of accepting.
	RxMTU() int

	// SetRxMTU sets the ATT_MTU which the local device is capable of accepting.
	SetRxMTU(mtu int)

	// TxMTU returns the ATT_MTU which the remote device is capable of accepting.
	TxMTU() int

	// SetTxMTU sets the ATT_MTU which the remote device is capable of accepting.
	SetTxMTU(mtu int)

	// ReadRSSI retrieves the current RSSI value of the remote peripheral, in
	// dBm. [Vol 2, Part E, 7.5.4] Any transport or command failure is
	// reported as an error rather than a fabricated zero reading. The
	// exchange is bounded by the backend's own command timeout.
	ReadRSSI() (int, error)

	// UpdateParams issues an LE Connection Update on a live central link and
	// blocks until the controller reports it complete. p is validated and
	// converted with ConnParams.Encode; an out-of-range field returns
	// ErrInvalidConnParams (wrapped) without touching the controller. The
	// wait is bounded by ctx, by the backend's own update timeout, and by
	// connection teardown. Backends with no central-side update API (e.g.
	// CoreBluetooth, which manages parameters itself) return a wrapped
	// ErrNotImplemented.
	UpdateParams(ctx context.Context, p ConnParams) error

	// SetDataLength requests LE Data Length Extension on a live central link:
	// it asks the controller to use up to txOctets-octet link-layer payloads
	// (and txTime µs of air time) for this connection, cutting the packet
	// count of large GATT operations and thus radio airtime. txOctets and
	// txTime are validated by ValidateDataLength; an out-of-range value returns
	// ErrInvalidDataLength (wrapped) without touching the controller. Pass
	// DataLengthMaxTxOctets / DataLengthMaxTxTime (251 / 17040) for "the
	// controller's maximum".
	//
	// Unlike UpdateParams, this returns as soon as the controller accepts or
	// rejects the command (a Command Complete with a status): the actual
	// negotiated length arrives asynchronously — if at all — as an LE Data
	// Length Change event and may also be driven by the peer, so it is not
	// correlated 1:1 with this call. A non-zero command status is returned as
	// an error. Backends that manage data length themselves (e.g.
	// CoreBluetooth) return a wrapped ErrNotImplemented.
	SetDataLength(ctx context.Context, txOctets, txTime uint16) error

	// Disconnected returns a receiving channel, which is closed when the connection disconnects.
	Disconnected() <-chan struct{}
}
