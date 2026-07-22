package ble

import "testing"

// TestProfileFind covers the dispatch of Profile.Find across the three
// target types (and the no-match nil).
func TestProfileFind(t *testing.T) {
	dsc := &Descriptor{UUID: UUID16(0x2902)}
	chr := &Characteristic{UUID: UUID16(0x2A00), Descriptors: []*Descriptor{dsc}}
	svc := &Service{UUID: UUID16(0x1800), Characteristics: []*Characteristic{chr}}
	p := &Profile{Services: []*Service{svc}}

	if got := p.Find(&Service{UUID: UUID16(0x1800)}); got != svc {
		t.Fatalf("Find(service) = %v, want the discovered service", got)
	}
	if got := p.Find(&Characteristic{UUID: UUID16(0x2A00)}); got != chr {
		t.Fatalf("Find(characteristic) = %v, want the discovered characteristic", got)
	}
	if got := p.Find(&Descriptor{UUID: UUID16(0x2902)}); got != dsc {
		t.Fatalf("Find(descriptor) = %v, want the discovered descriptor", got)
	}
	// No match returns a genuine nil interface, so a plain == nil check
	// fires (the upstream typed-nil wart boxed a nil *Service into a
	// non-nil any, and this comparison failed).
	if got := p.Find(&Service{UUID: UUID16(0xFFFF)}); got != nil {
		t.Fatalf("Find(unknown service) = %v (%T), want a nil interface", got, got)
	}
	if got := p.Find(&Characteristic{UUID: UUID16(0xFFFF)}); got != nil {
		t.Fatalf("Find(unknown characteristic) = %v (%T), want a nil interface", got, got)
	}
	if got := p.Find(&Descriptor{UUID: UUID16(0xFFFF)}); got != nil {
		t.Fatalf("Find(unknown descriptor) = %v (%T), want a nil interface", got, got)
	}
	// An unsupported target type returns nil, not a panic.
	if got := p.Find("not a profile node"); got != nil {
		t.Fatalf("Find(unsupported type) = %v, want nil", got)
	}
}
