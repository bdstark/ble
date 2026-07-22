//go:build linux
// +build linux

package socket

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// newPairSocket wraps one end of a socketpair in a Socket so Close can be
// exercised without a Bluetooth adapter.
func newPairSocket(t *testing.T) (*Socket, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	old := readPollMs
	readPollMs = 20
	t.Cleanup(func() { readPollMs = old })
	return &Socket{fd: fds[0], closed: make(chan struct{})}, fds[1]
}

// A second Close must be a no-op returning the first call's result, not a
// panic on re-closing s.closed: HCI.close reaches Socket.Close from internal
// error paths (command-buffer timeout, write failure) and the daemon's
// Device.Stop can follow with its own Close during recovery.
func TestCloseIdempotent(t *testing.T) {
	s, peer := newPairSocket(t)
	defer unix.Close(peer)

	first := s.Close()
	second := s.Close()

	if first != nil {
		t.Errorf("first Close: %v", first)
	}
	if second != first {
		t.Errorf("second Close = %v, want first call's result (%v)", second, first)
	}
}

// TestCloseUnblocksRead: Close must get a parked Read out with EOF. The
// peer stays completely SILENT — the dead-controller case. The old
// implementation woke the reader by writing a no-op HCI command and
// waiting for the controller's reply, so a fully dead firmware wedged
// Close (and, through it, HCI.close holding the HCI mutex) forever; the
// poll-based reader needs no cooperation from the peer.
func TestCloseUnblocksRead(t *testing.T) {
	s, peer := newPairSocket(t)
	defer unix.Close(peer)

	readErr := make(chan error, 1)
	go func() {
		_, err := s.Read(make([]byte, 32))
		readErr <- err
	}()

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked on a silent peer (controller-dependent wakeup?)")
	}
	select {
	case err := <-readErr:
		if err == nil {
			t.Error("Read returned nil after Close, want EOF")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read still parked after Close")
	}
}

// TestReadDeliversTraffic: the poll-based reader still delivers a packet
// the peer writes, and a packet racing Close is suppressed (EOF), never
// delivered after teardown began.
func TestReadDeliversTraffic(t *testing.T) {
	s, peer := newPairSocket(t)
	defer unix.Close(peer)

	want := []byte{0x04, 0x0E, 0x03, 0x01, 0x03, 0x0C}
	go unix.Write(peer, want)

	b := make([]byte, 32)
	n, err := s.Read(b)
	if err != nil || n != len(want) {
		t.Fatalf("Read = (%d, %v), want (%d, nil)", n, err, len(want))
	}
	for i := range want {
		if b[i] != want[i] {
			t.Fatalf("Read delivered % X, want % X", b[:n], want)
		}
	}
}
