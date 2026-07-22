package att

import (
	"testing"

	"github.com/bdstark/ble"
)

// FuzzServerHandleRequest throws arbitrary PDUs at the ATT server dispatch —
// the surface a hostile or buggy central controls completely. The server is
// shared across inputs, so state carried between PDUs (the negotiated MTU,
// the prepare-write queue) is fuzzed too, which is the live condition. Three
// of the server's historical bugs were remotely triggerable panics on
// exactly this surface.
func FuzzServerHandleRequest(f *testing.F) {
	for _, seed := range [][]byte{
		{ExchangeMTURequestCode, 0xB7, 0x00},
		{FindInformationRequestCode, 0x01, 0x00, 0xFF, 0xFF},
		{FindByTypeValueRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28, 0x15, 0x18},
		{ReadByTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x03, 0x28},
		{ReadRequestCode, 0x03, 0x00},
		{ReadBlobRequestCode, 0x03, 0x00, 0x01, 0x00},
		{ReadByGroupTypeRequestCode, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x28},
		{WriteRequestCode, 0x03, 0x00, 0xAA},
		{WriteCommandCode, 0x03, 0x00, 0xAA},
		{PrepareWriteRequestCode, 0x03, 0x00, 0x00, 0x00, 'h', 'i'},
		{ExecuteWriteRequestCode, 0x01},
		{ExecuteWriteRequestCode, 0x00},
		{HandleValueConfirmationCode},
		{ReadMultipleRequestCode, 0x01, 0x00, 0x02, 0x00},
		{SignedWriteCommandCode, 0x03, 0x00},
		{0xEE},
	} {
		f.Add(seed)
	}

	var sink []byte
	svc, _ := testService(&sink)
	s, err := NewServer(NewDB([]*ble.Service{svc}, 1), newOnceConn())
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) == 0 {
			return // Loop never delivers an empty PDU
		}
		_ = s.handleRequest(b)
	})
}
