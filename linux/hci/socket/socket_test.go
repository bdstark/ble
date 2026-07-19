//go:build linux
// +build linux

package socket

import (
	"testing"

	"golang.org/x/sys/unix"
)

// newPairSocket wraps one end of a socketpair in a Socket so Close can be
// exercised without a Bluetooth adapter. The other end is left open so the
// wake-up write inside Close has somewhere to go.
func newPairSocket(t *testing.T) (*Socket, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
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

// Close's wake-up write relies on the controller echoing a completion back
// to the blocked reader; the peer goroutine plays the controller. The
// unblocked Read must report EOF, not the echoed bytes.
func TestCloseUnblocksRead(t *testing.T) {
	s, peer := newPairSocket(t)
	defer unix.Close(peer)

	go func() {
		b := make([]byte, 32)
		n, err := unix.Read(peer, b)
		if err != nil {
			return
		}
		unix.Write(peer, b[:n])
	}()

	readErr := make(chan error, 1)
	go func() {
		_, err := s.Read(make([]byte, 32))
		readErr <- err
	}()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-readErr; err == nil {
		t.Error("Read returned nil after Close, want EOF")
	}
}
