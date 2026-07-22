package adv

import "testing"

// TestFieldLengthByteOverflow: an AD structure whose length byte is 0xff
// must not wedge the field walk. 1+l computed as byte arithmetic overflows
// to 0, so b = b[1+l:] left the slice unadvanced and Field/fieldPos spun
// forever — a remote DoS, since advertising data is peer-controlled.
func TestFieldLengthByteOverflow(t *testing.T) {
	// Both walks must terminate; before the fix these spun forever and the
	// test's -timeout would fire with a Field/fieldPos stack.
	p := NewRawPacket([]byte{0xff, 0x06}) // length 0xff, some type, no data
	_ = p.LocalName()                     // Field walk
	_ = p.UUIDs()                         // fieldPos walk
	_ = p.ManufacturerData()
	_ = p.ServiceSol()
}

// TestFieldWellFormed: a valid packet with two structures parses correctly
// after the int-arithmetic fix (no off-by-one from the reworked slicing).
func TestFieldWellFormed(t *testing.T) {
	// [len=4][type=completeName]"abc"  +  [len=2][type=flags][0x06]
	p := NewRawPacket([]byte{0x04, 0x09, 'a', 'b', 'c', 0x02, 0x01, 0x06})
	if got := p.LocalName(); got != "abc" {
		t.Fatalf("LocalName = %q, want abc", got)
	}
	if f, ok := p.Flags(); !ok || f != 0x06 {
		t.Fatalf("Flags = %#x, %v, want 0x06, true", f, ok)
	}
}

// TestTxPowerAndFlags: both fields were unreadable before the header-strip
// fix (each guarded a b[2] read a one-byte payload never reached).
func TestTxPowerAndFlags(t *testing.T) {
	// [len=2 flags 0x06][len=2 txPower -12(0xF4)]
	p := NewRawPacket([]byte{0x02, 0x01, 0x06, 0x02, 0x0A, 0xF4})
	if f, ok := p.Flags(); !ok || f != 0x06 {
		t.Fatalf("Flags = %#x, %v, want 0x06, true", f, ok)
	}
	if pw, ok := p.TxPower(); !ok || pw != -12 {
		t.Fatalf("TxPower = %d, %v, want -12, true", pw, ok)
	}
}

// TestFieldMaxLegalLength: length 0xfe (254) is legal and, absent enough
// bytes, must terminate as not-found rather than over-read.
func TestFieldMaxLegalLength(t *testing.T) {
	p := NewRawPacket([]byte{0xfe, 0x09, 'a'})
	if got := p.LocalName(); got != "" {
		t.Fatalf("LocalName on a truncated max-length field = %q, want empty", got)
	}
}

// TestServiceSolWidths: solicited-service UUIDs decode at their declared
// widths — 32-bit ones were parsed as 128-bit garbage before the fix.
func TestServiceSol(t *testing.T) {
	// serviceSol16 (0x14) 0x1234 ; serviceSol32 (0x1F) 0x12345678
	p := NewRawPacket([]byte{
		0x03, 0x14, 0x34, 0x12,
		0x05, 0x1F, 0x78, 0x56, 0x34, 0x12,
	})
	u := p.ServiceSol()
	if len(u) != 2 {
		t.Fatalf("ServiceSol returned %d UUIDs, want 2: %v", len(u), u)
	}
	if u[0].String() != "1234" {
		t.Fatalf("16-bit solicited UUID = %s, want 1234", u[0])
	}
	if u[1].String() != "12345678" {
		t.Fatalf("32-bit solicited UUID = %s, want 12345678", u[1])
	}
}

// TestServiceData: service data decodes UUID then payload, at the declared
// UUID width; the 32-bit form (0x21) exercises the width-aware copy that
// replaced the old always-from-byte-2 copy.
func TestServiceData(t *testing.T) {
	// serviceData32 (0x20): UUID 0x12345678 + data {0xAA,0xBB}
	p := NewRawPacket([]byte{0x07, 0x20, 0x78, 0x56, 0x34, 0x12, 0xAA, 0xBB})
	sd := p.ServiceData()
	if len(sd) != 1 {
		t.Fatalf("ServiceData returned %d entries, want 1", len(sd))
	}
	if sd[0].UUID.String() != "12345678" {
		t.Fatalf("service data UUID = %s, want 12345678", sd[0].UUID)
	}
	if string(sd[0].Data) != string([]byte{0xAA, 0xBB}) {
		t.Fatalf("service data payload = % X, want AA BB", sd[0].Data)
	}
}

// TestServiceDataTruncatedUUID: a service-data structure shorter than its
// UUID width is dropped, not sliced past its end (the old make(-n) panic).
func TestServiceDataTruncatedUUID(t *testing.T) {
	p := NewRawPacket([]byte{0x02, 0x20, 0x78}) // 32-bit type, one UUID byte
	if sd := p.ServiceData(); len(sd) != 0 {
		t.Fatalf("truncated service data yielded %d entries, want 0", len(sd))
	}
}
