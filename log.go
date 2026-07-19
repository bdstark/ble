package ble

import "log/slog"

// Logger is the logger used by this package and its subpackages.
// It defaults to slog.Default(); consumers that want library logs routed
// elsewhere — or debug-level packet dumps enabled — should assign their own
// *slog.Logger before opening a device. Hot-path debug sites check
// Logger.Enabled first so no formatting cost is paid while debug is off.
var Logger = slog.Default()
