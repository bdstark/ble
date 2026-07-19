package hci

import (
	"testing"

	"github.com/go-ble/ble/linux/hci/evt"
)

// advReportFixture is a raw LE Advertising Report event payload with one
// ADV_IND report carrying flags, a complete local name ("hello"), and one
// 16-bit service UUID.
var advReportFixture = evt.LEAdvertisingReport([]byte{
	0x02, // subevent: LE Advertising Report
	0x01, // one report
	evtTypAdvInd,
	0x00,                               // public address
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // address
	14,               // data length
	0x02, 0x01, 0x06, // flags
	0x06, 0x09, 'h', 'e', 'l', 'l', 'o', // complete local name
	0x03, 0x03, 0xAA, 0xBB, // 16-bit service UUIDs
	0xC8, // RSSI
})

func TestAdvertisementFieldAccess(t *testing.T) {
	a := newAdvertisement(advReportFixture, 0)
	if got := a.LocalName(); got != "hello" {
		t.Fatalf("LocalName = %q, want %q", got, "hello")
	}
	if got := a.Services(); len(got) != 1 || got[0].String() != "bbaa" {
		t.Fatalf("Services = %v, want [bbaa]", got)
	}
	if !a.Connectable() {
		t.Fatal("Connectable = false, want true")
	}
	if got := a.RSSI(); got != -56 {
		t.Fatalf("RSSI = %d, want -56", got)
	}
	if a.p == nil {
		t.Fatal("packet parse was not cached")
	}
}

// BenchmarkAdvertisementFieldAccess models a scan handler inspecting one
// received report: one parse, several field accesses.
func BenchmarkAdvertisementFieldAccess(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a := newAdvertisement(advReportFixture, 0)
		if a.LocalName() != "hello" {
			b.Fatal("bad local name")
		}
		_ = a.Services()
		_ = a.ManufacturerData()
		_ = a.Connectable()
		_ = a.RSSI()
	}
}

// BenchmarkAdvertisementFieldAccessNoCache approximates the pre-caching
// behavior, where every accessor re-parsed the AD packet, by clearing the
// cache between accesses.
func BenchmarkAdvertisementFieldAccessNoCache(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a := newAdvertisement(advReportFixture, 0)
		if a.LocalName() != "hello" {
			b.Fatal("bad local name")
		}
		a.p = nil
		_ = a.Services()
		a.p = nil
		_ = a.ManufacturerData()
		a.p = nil
		_ = a.Connectable()
		_ = a.RSSI()
	}
}
