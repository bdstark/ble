package hci

import (
	"testing"

	"github.com/go-ble/ble/linux/hci/evt"
)

func TestPoolableEvtPkt(t *testing.T) {
	tests := []struct {
		name string
		pkt  []byte
		want bool
	}{
		{"command complete", []byte{pktTypeEvent, evt.CommandCompleteCode, 3, 1, 0x00, 0x10}, true},
		{"command status", []byte{pktTypeEvent, evt.CommandStatusCode, 4, 0, 1, 0x00, 0x10}, true},
		{"num completed pkts", []byte{pktTypeEvent, evt.NumberOfCompletedPacketsCode, 5, 1, 0x40, 0x00, 1, 0}, true},
		// Retained by Advertisement / Conn / user callbacks — never pooled.
		{"le meta (adv report)", []byte{pktTypeEvent, 0x3E, 3, 0x02, 0, 0}, false},
		{"disconnection complete", []byte{pktTypeEvent, evt.DisconnectionCompleteCode, 4, 0, 0x40, 0x00, 0x13}, false},
		{"acl data", []byte{pktTypeACLData, 0x40, 0x00, 0x04, 0x00}, false},
		{"too short", []byte{pktTypeEvent}, false},
	}
	for _, tt := range tests {
		if got := poolableEvtPkt(tt.pkt); got != tt.want {
			t.Errorf("%s: poolableEvtPkt = %v, want %v", tt.name, got, tt.want)
		}
	}
}
