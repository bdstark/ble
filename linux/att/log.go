package att

import (
	"context"
	"log/slog"

	"github.com/go-ble/ble"
)

// logDebugEnabled gates per-PDU debug logging. The hex dumps behind it
// cost a format pass and an allocation per ATT operation, which must not
// be paid while debug logging is off.
func logDebugEnabled() bool {
	return ble.Logger.Enabled(context.Background(), slog.LevelDebug)
}
