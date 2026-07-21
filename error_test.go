package ble

import (
	"strings"
	"testing"
)

// TestATTErrorNames pins the boundaries of ATTError.Error's range switch —
// 0x11 (the last named code) used to fall through every case to "unknown
// error", leaving the errName entry unreachable.
func TestATTErrorNames(t *testing.T) {
	for _, tc := range []struct {
		e    ATTError
		want string
	}{
		{ErrSuccess, "success"},
		{ErrInvalidHandle, "invalid handle"},
		{ErrUnsuppGrpType, "unsupported group type"},
		{ErrInsuffResources, "insufficient resources"},
	} {
		if got := tc.e.Error(); got != tc.want {
			t.Errorf("ATTError(0x%02X).Error() = %q, want %q", int(tc.e), got, tc.want)
		}
	}
	for _, tc := range []struct {
		e    ATTError
		want string
	}{
		{ATTError(0x12), "reserved"},
		{ATTError(0x80), "application"},
		{ATTError(0xA0), "reserved"},
		{ATTError(0xE0), "profile or service"},
	} {
		if got := tc.e.Error(); !strings.Contains(got, tc.want) {
			t.Errorf("ATTError(0x%02X).Error() = %q, want it to mention %q", int(tc.e), got, tc.want)
		}
	}
}
