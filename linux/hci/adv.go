package hci

import (
	"net"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/linux/adv"
	"github.com/go-ble/ble/linux/hci/evt"
)

// RandomAddress is a Random Device Address.
type RandomAddress struct {
	ble.Addr
}

// [Vol 6, Part B, 4.4.2] [Vol 3, Part C, 11]
const (
	evtTypAdvInd        = 0x00 // Connectable undirected advertising (ADV_IND).
	evtTypAdvDirectInd  = 0x01 // Connectable directed advertising (ADV_DIRECT_IND).
	evtTypAdvScanInd    = 0x02 // Scannable undirected advertising (ADV_SCAN_IND).
	evtTypAdvNonconnInd = 0x03 // Non connectable undirected advertising (ADV_NONCONN_IND).
	evtTypScanRsp       = 0x04 // Scan Response (SCAN_RSP).
)

func newAdvertisement(e evt.LEAdvertisingReport, i int) *Advertisement {
	return &Advertisement{e: e, i: i}
}

// Advertisement implements ble.Advertisement and other functions that are only
// available on Linux.
type Advertisement struct {
	e  evt.LEAdvertisingReport
	i  int
	sr *Advertisement

	// cached packets.
	p *adv.Packet

	// cached Addr() result (see Addr).
	addr ble.Addr
}

// setScanResponse ssociate sca response to the existing advertisement.
func (a *Advertisement) setScanResponse(sr *Advertisement) {
	a.sr = sr
	a.p = nil // clear the cached.
}

// packets returns the combined advertising packet and scan response (if presents)
// The parse is cached: field accessors (LocalName, Services, ...) are typically
// called several times per advertisement, and re-parsing allocates a fresh
// adv.Packet each time. Handlers run on the single adv dispatcher goroutine and
// paired AD+SR advertisements are built fresh rather than mutated in place
// (see handleLEAdvertisingReport), so the lazy write to a.p is race-free.
func (a *Advertisement) packets() *adv.Packet {
	if a.p == nil {
		a.p = adv.NewRawPacket(a.Data(), a.ScanResponse())
	}
	return a.p
}

// LocalName returns the LocalName of the remote peripheral.
func (a *Advertisement) LocalName() string {
	// One walk per source: each adv.Packet.LocalName() call re-walks the
	// TLV fields and allocates a fresh string.
	if name := a.packets().LocalName(); name != "" {
		return name
	}
	if a.sr != nil {
		return a.sr.LocalName()
	}
	return ""
}

// ManufacturerData returns the ManufacturerData of the advertisement.
func (a *Advertisement) ManufacturerData() []byte {
	return a.packets().ManufacturerData()
}

// ServiceData returns the service data of the advertisement.
func (a *Advertisement) ServiceData() []ble.ServiceData {
	return a.packets().ServiceData()
}

// Services returns the service UUIDs of the advertisement.
func (a *Advertisement) Services() []ble.UUID {
	return a.packets().UUIDs()
}

// OverflowService returns the UUIDs of overflowed service.
func (a *Advertisement) OverflowService() []ble.UUID {
	return a.packets().UUIDs()
}

// TxPowerLevel returns the tx power level of the remote peripheral.
func (a *Advertisement) TxPowerLevel() int {
	pwr, _ := a.packets().TxPower()
	return pwr
}

// SolicitedService returns UUIDs of solicited services.
func (a *Advertisement) SolicitedService() []ble.UUID {
	return a.packets().ServiceSol()
}

// Connectable indicates weather the remote peripheral is connectable.
func (a *Advertisement) Connectable() bool {
	return a.EventType() == evtTypAdvDirectInd || a.EventType() == evtTypAdvInd
}

// RSSI returns RSSI signal strength.
func (a *Advertisement) RSSI() int {
	return int(a.e.RSSI(a.i))
}

// Addr returns the address of the remote peripheral.
//
// The result is computed once and cached: the conversion from the
// little-endian wire address allocates a 6-byte slice plus an interface box,
// and app-side scan filters call Addr on every report. The unguarded lazy
// write is race-free for the same reason packets' cache is: handlers run on
// the single adv dispatcher goroutine, and paired AD+SR advertisements are
// built fresh rather than mutated in place (see handleLEAdvertisingReport).
// The address comes from the immutable event buffer (a scan response is
// paired by equal address), so the cache never needs invalidation.
func (a *Advertisement) Addr() ble.Addr {
	if a.addr != nil {
		return a.addr
	}
	b := a.e.Address(a.i)
	addr := net.HardwareAddr([]byte{b[5], b[4], b[3], b[2], b[1], b[0]})
	if a.e.AddressType(a.i) == 1 {
		a.addr = RandomAddress{addr}
	} else {
		a.addr = addr
	}
	return a.addr
}

// EventType returns the event type of Advertisement.
// This is linux sepcific.
func (a *Advertisement) EventType() uint8 {
	return a.e.EventType(a.i)
}

// AddressType returns the address type of the Advertisement.
// This is linux sepcific.
func (a *Advertisement) AddressType() uint8 {
	return a.e.AddressType(a.i)
}

// Data returns the advertising data of the packet.
// This is linux sepcific.
func (a *Advertisement) Data() []byte {
	return a.e.Data(a.i)
}

// ScanResponse returns the scan response of the packet, if it presents.
// This is linux sepcific.
func (a *Advertisement) ScanResponse() []byte {
	if a.sr == nil {
		return nil
	}
	return a.sr.Data()
}
