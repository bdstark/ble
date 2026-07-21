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
	// No match yields a nil *Service inside a non-nil any — a typed-nil
	// wart inherited from upstream (a bare `== nil` check on Find's result
	// never fires); pinned here so a fix is a deliberate contract change.
	if got := p.Find(&Service{UUID: UUID16(0xFFFF)}).(*Service); got != nil {
		t.Fatalf("Find(unknown service) = %v, want nil *Service", got)
	}
}
