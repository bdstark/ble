package hci

import (
	"context"
	"log/slog"

	"github.com/bdstark/ble"
)

// logDebugEnabled gates per-packet debug logging. The hex dumps behind it
// cost a format pass and an allocation per packet, which must not be paid
// while debug logging is off.
func logDebugEnabled() bool {
	return ble.Logger.Enabled(context.Background(), slog.LevelDebug)
}
