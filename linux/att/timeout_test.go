package att

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSendReqSeqProtoTimeout: a server that swallows the request without
// responding must trip the ATT sequential-protocol timeout rather than
// parking the request forever.
func TestSendReqSeqProtoTimeout(t *testing.T) {
	old := seqProtoTimeout
	seqProtoTimeout = 50 * time.Millisecond
	t.Cleanup(func() { seqProtoTimeout = old })

	f := newOnceConn()
	c := startClient(t, f)
	go func() { <-f.writes }() // swallow the request, never respond

	_, err := c.Read(context.Background(), 3)
	if !errors.Is(err, ErrSeqProtoTimeout) {
		t.Fatalf("Read = %v, want ErrSeqProtoTimeout", err)
	}
}
