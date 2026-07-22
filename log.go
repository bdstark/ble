package ble

import (
	"log/slog"
	"sync/atomic"
)

// logger holds the logger installed by SetLogger. Nil means "not set" —
// Logger falls back to slog.Default() so a later slog.SetDefault is honored.
// atomic.Pointer makes the swap race-free: the HCI/ATT stacks log from
// several goroutines (sktLoop, the adv dispatcher, per-connection readers),
// and a consumer may reconfigure logging while a device is running.
var logger atomic.Pointer[slog.Logger]

// SetLogger routes this package's logs — and its subpackages' — to l, or to
// slog.Default() when l is nil. Safe to call at any time, including
// concurrently with active logging: the previous plain package variable
// raced every log site the moment it was reassigned after a device opened.
func SetLogger(l *slog.Logger) {
	logger.Store(l)
}

// Logger returns the logger in use: the one set by SetLogger, or
// slog.Default() if none was. Hot-path debug sites call Logger().Enabled
// first, so no formatting cost is paid while debug logging is off.
func Logger() *slog.Logger {
	if l := logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}
