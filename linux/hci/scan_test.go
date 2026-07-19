package hci

import (
	"errors"
	"net"
	"testing"

	"github.com/bdstark/ble/linux/hci/evt"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// advTestReport describes one report in a multi-report advertising event.
// The full address is {addrLo, 02, 03, 04, 05, 06}.
type advTestReport struct {
	evtTyp  byte
	addrTyp byte
	addrLo  byte
	data    []byte
}

// multiAdvReportPkt builds an LE Advertising Report payload holding several
// reports, in the columnar wire layout the evt accessors expect: all event
// types, then all address types, all addresses, all data lengths, all data,
// all RSSIs.
func multiAdvReportPkt(reports ...advTestReport) []byte {
	b := []byte{
		0x02, // subevent: LE Advertising Report
		byte(len(reports)),
	}
	for _, r := range reports {
		b = append(b, r.evtTyp)
	}
	for _, r := range reports {
		b = append(b, r.addrTyp)
	}
	for _, r := range reports {
		b = append(b, r.addrLo, 0x02, 0x03, 0x04, 0x05, 0x06)
	}
	for _, r := range reports {
		b = append(b, byte(len(r.data)))
	}
	for _, r := range reports {
		b = append(b, r.data...)
	}
	for range reports {
		b = append(b, 0xC8) // RSSI
	}
	return b
}

// recvAdv fails the test unless an advertisement is waiting on chAdv.
func recvAdv(t *testing.T, h *HCI) *Advertisement {
	t.Helper()
	select {
	case a := <-h.chAdv:
		return a
	default:
		t.Fatal("no advertisement was delivered")
		return nil
	}
}

// ---------------------------------------------------------------------------
// Orphan scan responses: counted, never abort the event
// ---------------------------------------------------------------------------

// TestAdvReportOrphanSRCountedAndRestProcessed: a scan response with no
// matching AD in the history is counted in SROrphaned, and — unlike the old
// early return — the remaining reports in the same HCI event are still
// processed and delivered.
func TestAdvReportOrphanSRCountedAndRestProcessed(t *testing.T) {
	h := newAdvHCI(8)
	adData := []byte{0x02, 0x01, 0x06}
	pkt := multiAdvReportPkt(
		advTestReport{evtTyp: evtTypScanRsp, addrLo: 0x99},              // orphan: empty history
		advTestReport{evtTyp: evtTypAdvInd, addrLo: 0x01, data: adData}, // must still get through
	)
	if err := h.handleLEAdvertisingReport(pkt); !errors.Is(err, errOrphanScanRsp) {
		t.Fatalf("handleLEAdvertisingReport = %v, want errOrphanScanRsp", err)
	}
	if got := h.SROrphaned(); got != 1 {
		t.Fatalf("SROrphaned = %d, want 1", got)
	}
	a := recvAdv(t, h)
	if got := a.Addr().String(); got != "06:05:04:03:02:01" {
		t.Fatalf("delivered report has Addr %s, want the AD after the orphan (06:05:04:03:02:01)", got)
	}
	if h.adHist[0] == nil {
		t.Fatal("the AD report after the orphan SR was not recorded in history")
	}

	// A follow-up orphan keeps counting; the counter is monotonic.
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypScanRsp, 0x99, nil)); !errors.Is(err, errOrphanScanRsp) {
		t.Fatalf("second orphan: err = %v, want errOrphanScanRsp", err)
	}
	if got := h.SROrphaned(); got != 2 {
		t.Fatalf("SROrphaned = %d, want 2", got)
	}
	// A matched SR (the AD for 0x01 is in the history) is not an orphan.
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypScanRsp, 0x01, nil)); err != nil {
		t.Fatalf("matched SR: err = %v, want nil", err)
	}
	if got := h.SROrphaned(); got != 2 {
		t.Fatalf("SROrphaned = %d after a matched SR, want 2 (unchanged)", got)
	}
}

// ---------------------------------------------------------------------------
// LocalName: correct for AD-carried, SR-carried, and absent names
// ---------------------------------------------------------------------------

func TestLocalNameFromAD(t *testing.T) {
	h := newAdvHCI(8)
	name := []byte{0x04, 0x09, 'a', 'b', 'c'} // complete local name "abc"
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvInd, 0x01, name)); err != nil {
		t.Fatalf("AD report: %v", err)
	}
	a := recvAdv(t, h)
	if got := a.LocalName(); got != "abc" {
		t.Fatalf("LocalName = %q, want %q", got, "abc")
	}
	// Repeated calls stay correct (single packets() walk per call).
	if got := a.LocalName(); got != "abc" {
		t.Fatalf("second LocalName = %q, want %q", got, "abc")
	}
}

func TestLocalNameFromScanResponse(t *testing.T) {
	h := newAdvHCI(8)
	flags := []byte{0x02, 0x01, 0x06}
	srName := []byte{0x04, 0x09, 'x', 'y', 'z'}
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvScanInd, 0x01, flags)); err != nil {
		t.Fatalf("AD report: %v", err)
	}
	if got := recvAdv(t, h).LocalName(); got != "" {
		t.Fatalf("AD-only LocalName = %q, want empty (name is in the SR)", got)
	}
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypScanRsp, 0x01, srName)); err != nil {
		t.Fatalf("SR report: %v", err)
	}
	paired := recvAdv(t, h)
	if got := paired.LocalName(); got != "xyz" {
		t.Fatalf("paired LocalName = %q, want %q", got, "xyz")
	}
	if got := paired.LocalName(); got != "xyz" {
		t.Fatalf("second paired LocalName = %q, want %q", got, "xyz")
	}
}

func TestLocalNameAbsent(t *testing.T) {
	h := newAdvHCI(8)
	flags := []byte{0x02, 0x01, 0x06}
	// No name anywhere, no scan response.
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvScanInd, 0x02, flags)); err != nil {
		t.Fatalf("AD report: %v", err)
	}
	if got := recvAdv(t, h).LocalName(); got != "" {
		t.Fatalf("LocalName = %q, want empty", got)
	}
	// No name anywhere, with a (nameless) scan response attached: the SR
	// fallback path must also report empty.
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypScanRsp, 0x02, nil)); err != nil {
		t.Fatalf("SR report: %v", err)
	}
	paired := recvAdv(t, h)
	if paired.sr == nil {
		t.Fatal("paired advertisement lost its scan response")
	}
	if got := paired.LocalName(); got != "" {
		t.Fatalf("paired nameless LocalName = %q, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// Addr: cached, equal across calls, stable across SR attach
// ---------------------------------------------------------------------------

// TestAddrCachedPublic: Addr() must return equal values across calls
// without re-allocating (the cached slice is returned), and must not change
// when a scan response is attached (the SR was paired by this very address).
func TestAddrCachedPublic(t *testing.T) {
	h := newAdvHCI(8)
	if err := h.handleLEAdvertisingReport(advReportPkt(evtTypAdvInd, 0x01, nil)); err != nil {
		t.Fatalf("AD report: %v", err)
	}
	a := recvAdv(t, h)

	hw1, ok := a.Addr().(net.HardwareAddr)
	if !ok {
		t.Fatalf("Addr = %T, want net.HardwareAddr for a public address", a.Addr())
	}
	if got := hw1.String(); got != "06:05:04:03:02:01" {
		t.Fatalf("Addr = %s, want 06:05:04:03:02:01", got)
	}
	hw2 := a.Addr().(net.HardwareAddr)
	if hw2.String() != hw1.String() {
		t.Fatalf("second Addr = %s, want %s", hw2, hw1)
	}
	if &hw2[0] != &hw1[0] {
		t.Fatal("second Addr call allocated a fresh value (not cached)")
	}

	// Attaching a scan response (same device by definition of the pairing)
	// must not change the cached address.
	sr := newAdvertisement(evt.LEAdvertisingReport(advReportPkt(evtTypScanRsp, 0x01, nil)), 0)
	a.setScanResponse(sr)
	hw3 := a.Addr().(net.HardwareAddr)
	if &hw3[0] != &hw1[0] {
		t.Fatal("Addr changed after scan-response attach")
	}
}

// TestAddrCachedRandom covers the random-address branch of the cache.
func TestAddrCachedRandom(t *testing.T) {
	h := newAdvHCI(8)
	pkt := multiAdvReportPkt(advTestReport{evtTyp: evtTypAdvInd, addrTyp: 1, addrLo: 0x0A})
	if err := h.handleLEAdvertisingReport(pkt); err != nil {
		t.Fatalf("AD report: %v", err)
	}
	a := recvAdv(t, h)

	r1, ok := a.Addr().(RandomAddress)
	if !ok {
		t.Fatalf("Addr = %T, want RandomAddress for address type 1", a.Addr())
	}
	if got := r1.String(); got != "06:05:04:03:02:0a" {
		t.Fatalf("Addr = %s, want 06:05:04:03:02:0a", got)
	}
	r2 := a.Addr().(RandomAddress)
	hw1 := r1.Addr.(net.HardwareAddr)
	hw2 := r2.Addr.(net.HardwareAddr)
	if &hw2[0] != &hw1[0] {
		t.Fatal("second Addr call allocated a fresh value (not cached)")
	}
}
