//go:build linux
// +build linux

package socket

import (
	"fmt"
	"io"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

func ioR(t, nr, size uintptr) uintptr {
	return (directionRead << directionShift) | (t << typeShift) | nr | (size << sizeShift)
}

func ioW(t, nr, size uintptr) uintptr {
	return (directionWrite << directionShift) | (t << typeShift) | nr | (size << sizeShift)
}

func ioctl(fd, op, arg uintptr) error {
	if _, _, ep := unix.Syscall(unix.SYS_IOCTL, fd, op, arg); ep != 0 {
		return ep
	}
	return nil
}

const (
	ioctlSize     = 4
	hciMaxDevices = 16
	typHCI        = 72 // 'H'
)

var (
	hciUpDevice      = ioW(typHCI, 201, ioctlSize) // HCIDEVUP
	hciDownDevice    = ioW(typHCI, 202, ioctlSize) // HCIDEVDOWN
	hciResetDevice   = ioW(typHCI, 203, ioctlSize) // HCIDEVRESET
	hciGetDeviceList = ioR(typHCI, 210, ioctlSize) // HCIGETDEVLIST
	hciGetDeviceInfo = ioR(typHCI, 211, ioctlSize) // HCIGETDEVINFO
)

type devListRequest struct {
	devNum     uint16
	devRequest [hciMaxDevices]struct {
		id  uint16
		opt uint32
	}
}

// Socket implements a HCI User Channel as ReadWriteCloser.
type Socket struct {
	fd        int
	closed    chan struct{}
	rmu       sync.Mutex
	wmu       sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// NewSocket returns a HCI User Channel of specified device id.
// If id is -1, the first available HCI device is returned.
func NewSocket(id int) (*Socket, error) {
	var err error
	// Create RAW HCI Socket.
	fd, err := unix.Socket(unix.AF_BLUETOOTH, unix.SOCK_RAW, unix.BTPROTO_HCI)
	if err != nil {
		return nil, fmt.Errorf("can't create socket: %w", err)
	}

	if id != -1 {
		return open(fd, id)
	}

	req := devListRequest{devNum: hciMaxDevices}
	if err = ioctl(uintptr(fd), hciGetDeviceList, uintptr(unsafe.Pointer(&req))); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("can't get device list: %w", err)
	}
	var msg string
	for id := 0; id < int(req.devNum); id++ {
		s, err := open(fd, int(req.devRequest[id].id))
		if err == nil {
			return s, nil
		}
		msg = msg + fmt.Sprintf("(hci%d: %s)", id, err)
	}
	unix.Close(fd)
	return nil, fmt.Errorf("no devices available: %s", msg)
}

func open(fd, id int) (*Socket, error) {
	// Reset the device in case previous session didn't cleanup properly.
	if err := ioctl(uintptr(fd), hciDownDevice, uintptr(id)); err != nil {
		return nil, fmt.Errorf("can't down device: %w", err)
	}
	if err := ioctl(uintptr(fd), hciUpDevice, uintptr(id)); err != nil {
		return nil, fmt.Errorf("can't up device: %w", err)
	}

	// HCI User Channel requires exclusive access to the device.
	// The device has to be down at the time of binding.
	if err := ioctl(uintptr(fd), hciDownDevice, uintptr(id)); err != nil {
		return nil, fmt.Errorf("can't down device: %w", err)
	}

	// Bind the RAW socket to HCI User Channel
	sa := unix.SockaddrHCI{Dev: uint16(id), Channel: unix.HCI_CHANNEL_USER}
	if err := unix.Bind(fd, &sa); err != nil {
		return nil, fmt.Errorf("can't bind socket to hci user channel: %w", err)
	}

	// poll for 20ms to see if any data becomes available, then clear it
	pfds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	unix.Poll(pfds, 20)
	if pfds[0].Revents&unix.POLLIN > 0 {
		b := make([]byte, 100)
		unix.Read(fd, b)
	}

	return &Socket{fd: fd, closed: make(chan struct{})}, nil
}

func (s *Socket) Read(p []byte) (int, error) {
	s.rmu.Lock()
	n, err := unix.Read(s.fd, p)
	s.rmu.Unlock()
	// Close always sends a dummy command to wake up Read
	// bad things happen to the HCI state machines if they receive
	// a reply from that command, so make sure no data is returned
	// on a closed socket.
	//
	// note that if Write and Close are called concurrently it's
	// indeterminate which replies get through.
	select {
	case <-s.closed:
		return 0, io.EOF
	default:
	}
	if err != nil {
		return n, fmt.Errorf("can't read hci socket: %w", err)
	}
	return n, nil
}

func (s *Socket) Write(p []byte) (int, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	n, err := unix.Write(s.fd, p)
	if err != nil {
		return n, fmt.Errorf("can't write hci socket: %w", err)
	}
	return n, nil
}

// Close is idempotent: HCI teardown is reachable from several paths
// (I/O errors, command timeouts, and an explicit Device.Stop), which
// can each end up here. Only the first call closes the socket; later
// calls return the first call's error so every caller sees the same
// outcome.
func (s *Socket) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.Write([]byte{0x01, 0x09, 0x10, 0x00}) // no-op command to wake up the Read call if it's blocked
		s.rmu.Lock()
		defer s.rmu.Unlock()
		if err := unix.Close(s.fd); err != nil {
			s.closeErr = fmt.Errorf("can't close hci socket: %w", err)
		}
	})
	return s.closeErr
}
