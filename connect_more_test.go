package ble

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// endScanDevice embeds stubDevice but ends its scan immediately with a fixed
// result instead of scanning until the context is done. It never delivers an
// advertisement, so Connect's post-scan drain finds no match.
type endScanDevice struct {
	stubDevice
	scanErr error
}

func (d *endScanDevice) Scan(ctx context.Context, allowDup bool, h AdvHandler) error {
	return d.scanErr
}

func TestConnectNoDefaultDevice(t *testing.T) {
	SetDefaultDevice(nil)
	cln, err := Connect(context.Background(), func(a Advertisement) bool { return true })
	if !errors.Is(err, ErrDefaultDevice) {
		t.Fatalf("Connect error = %v, want ErrDefaultDevice", err)
	}
	if cln != nil {
		t.Fatalf("Connect returned a client (%v) without a default device", cln)
	}
}

func TestConnectDialErrorIsWrapped(t *testing.T) {
	boom := errors.New("dial exploded")
	dev := &stubDevice{
		advs:   []Advertisement{&stubAdv{addr: NewAddr("11:22:33:44:55:66")}},
		dialer: func(ctx context.Context, a Addr) (Client, error) { return nil, boom },
	}
	SetDefaultDevice(dev)
	defer SetDefaultDevice(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cln, err := Connect(ctx, func(a Advertisement) bool { return true })
	if !errors.Is(err, boom) {
		t.Fatalf("Connect error = %v, want it to wrap the dial error %v", err, boom)
	}
	if !strings.Contains(err.Error(), "can't dial") {
		t.Fatalf("Connect error = %q, want a %q wrap", err, "can't dial")
	}
	if cln != nil {
		t.Fatalf("Connect returned a client (%v) alongside a dial error", cln)
	}
}

func TestConnectScanErrorIsWrapped(t *testing.T) {
	boom := errors.New("radio fell over")
	SetDefaultDevice(&endScanDevice{scanErr: boom})
	defer SetDefaultDevice(nil)

	_, err := Connect(context.Background(), func(a Advertisement) bool { return false })
	if !errors.Is(err, boom) {
		t.Fatalf("Connect error = %v, want it to wrap the scan error %v", err, boom)
	}
	if !strings.Contains(err.Error(), "can't scan") {
		t.Fatalf("Connect error = %q, want a %q wrap", err, "can't scan")
	}
}

func TestConnectScanEndsCleanlyWithoutMatch(t *testing.T) {
	// Scan returns nil: it "completed" without the context expiring and
	// without delivering a matching advertisement.
	SetDefaultDevice(&endScanDevice{})
	defer SetDefaultDevice(nil)

	_, err := Connect(context.Background(), func(a Advertisement) bool { return false })
	if err == nil {
		t.Fatal("Connect returned nil error after a matchless scan")
	}
	if !strings.Contains(err.Error(), "scan ended without a matching advertisement") {
		t.Fatalf("Connect error = %q, want the no-match sentinel message", err)
	}
}
