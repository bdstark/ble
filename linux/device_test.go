package linux

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/linux/gatt"
	"github.com/go-ble/ble/linux/hci"
)

func TestNewDeviceOptionErrorWrapped(t *testing.T) {
	boom := errors.New("bad option")
	_, err := NewDevice(func(ble.DeviceOption) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("NewDevice error = %v, want it to wrap the option error %v", err, boom)
	}
	if !strings.Contains(err.Error(), "can't create hci") {
		t.Fatalf("NewDevice error = %q, want a %q wrap", err, "can't create hci")
	}
}

func TestNewDeviceInitErrorWrapped(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("a real HCI socket may exist on linux")
	}
	// On non-linux hosts the socket stub always fails, so Init fails.
	_, err := NewDevice()
	if err == nil {
		t.Fatal("NewDevice returned nil error without an HCI socket")
	}
	if !strings.Contains(err.Error(), "can't init hci") {
		t.Fatalf("NewDevice error = %q, want a %q wrap", err, "can't init hci")
	}
}

// loop must exit (after logging) when Accept fails with a non-EOF error.
func TestLoopExitsOnAcceptError(t *testing.T) {
	h, err := hci.NewHCI(ble.OptListenerTimeout(10 * time.Millisecond))
	if err != nil {
		t.Fatalf("NewHCI: %v", err)
	}
	srv, err := gatt.NewServer()
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		loop(h, srv, ble.DefaultMTU)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not return after Accept timed out")
	}
}

func TestDeviceDialInvalidAddrWrapped(t *testing.T) {
	h, err := hci.NewHCI()
	if err != nil {
		t.Fatalf("NewHCI: %v", err)
	}
	d := &Device{HCI: h}

	_, err = d.Dial(context.Background(), ble.NewAddr("not-a-mac"))
	if !errors.Is(err, hci.ErrInvalidAddr) {
		t.Fatalf("Dial error = %v, want it to wrap hci.ErrInvalidAddr", err)
	}
	if !strings.Contains(err.Error(), "can't dial") {
		t.Fatalf("Dial error = %q, want a %q wrap", err, "can't dial")
	}
}
