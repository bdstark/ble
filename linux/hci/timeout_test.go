package hci

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bdstark/ble/linux/hci/cmd"
)

// setCmdTimeout lowers cmdTimeout for one test and restores it afterwards.
// Tests using it must not run in parallel with anything that sends commands.
func setCmdTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := cmdTimeout
	cmdTimeout = d
	t.Cleanup(func() { cmdTimeout = old })
}

// TestSendCmdBufTimeout: with every command buffer outstanding and a
// controller that never returns them, send must give up with
// ErrCommandTimeout and close the HCI instead of parking forever.
func TestSendCmdBufTimeout(t *testing.T) {
	setCmdTimeout(t, 50*time.Millisecond)
	skt := newFakeSkt()
	h := newLoopedHCI(t, skt)
	<-h.chCmdBufs // starve the command buffer

	_, err := h.send(&cmd.LECreateConnectionCancel{})
	if !errors.Is(err, ErrCommandTimeout) || !strings.Contains(err.Error(), "no command buffer available") {
		t.Fatalf("send = %v, want ErrCommandTimeout (no command buffer available)", err)
	}
	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
		t.Fatal("send did not close the HCI after the command-buffer timeout")
	}
}

// TestSendResponseTimeout: the command is written but the controller never
// sends a completion event; send must fail with ErrCommandTimeout.
func TestSendResponseTimeout(t *testing.T) {
	setCmdTimeout(t, 50*time.Millisecond)
	skt := newFakeSkt() // records writes, never replies
	h := newLoopedHCI(t, skt)

	_, err := h.send(&cmd.LECreateConnectionCancel{})
	if !errors.Is(err, ErrCommandTimeout) || !strings.Contains(err.Error(), "no response to command") {
		t.Fatalf("send = %v, want ErrCommandTimeout (no response to command)", err)
	}
	if len(skt.written()) == 0 {
		t.Fatal("send timed out without ever writing the command")
	}
}

// TestCancelDialTimeout: cancel is disallowed (connection already up) but
// the connection-complete event was lost; the bounded wait must expire
// instead of parking forever.
func TestCancelDialTimeout(t *testing.T) {
	old := connCancelTimeout
	connCancelTimeout = 50 * time.Millisecond
	t.Cleanup(func() { connCancelTimeout = old })

	skt := newFakeSkt()
	respondWithStatus(skt, 0x0C) // ErrDisallowed
	h := newLoopedHCI(t, skt)
	h.chMasterConn = make(chan *Conn) // nothing will ever arrive

	cln, err := h.cancelDial()
	if cln != nil || err == nil || !strings.Contains(err.Error(), "connection never arrived") {
		t.Fatalf("cancelDial = (%v, %v), want the lost-event timeout error", cln, err)
	}
}
