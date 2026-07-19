package hci

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// binaryWriteACLData is the pre-refactor header construction, kept here to
// pin the wire format and as a benchmark baseline.
func binaryWriteACLData(pkt *bytes.Buffer, handleFlags uint16, payload []byte) error {
	if err := binary.Write(pkt, binary.LittleEndian, pktTypeACLData); err != nil {
		return err
	}
	if err := binary.Write(pkt, binary.LittleEndian, handleFlags); err != nil {
		return err
	}
	if err := binary.Write(pkt, binary.LittleEndian, uint16(len(payload))); err != nil {
		return err
	}
	return binary.Write(pkt, binary.LittleEndian, payload)
}

func TestWriteACLDataWireFormat(t *testing.T) {
	payload := []byte{0x04, 0x00, 0x04, 0x00, 0x1B, 0x2A, 0x01}
	handleFlags := uint16(0x0040) | (uint16(pbfHostToControllerStart<<4) << 8)

	var got, want bytes.Buffer
	writeACLData(&got, handleFlags, payload)
	if err := binaryWriteACLData(&want, handleFlags, payload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatalf("wire format changed:\n got % X\nwant % X", got.Bytes(), want.Bytes())
	}
}

func BenchmarkWriteACLData(b *testing.B) {
	pkt := bytes.NewBuffer(make([]byte, 0, 64))
	payload := make([]byte, 27)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pkt.Reset()
		writeACLData(pkt, 0x2040, payload)
	}
}

func BenchmarkWriteACLDataBinaryWrite(b *testing.B) {
	pkt := bytes.NewBuffer(make([]byte, 0, 64))
	payload := make([]byte, 27)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pkt.Reset()
		if err := binaryWriteACLData(pkt, 0x2040, payload); err != nil {
			b.Fatal(err)
		}
	}
}
