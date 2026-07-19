package hci

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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

// TestReadClosedMidReassembly pins the fix for a field crash: a disconnect
// closes chInPDU while Read is collecting the remaining fragments of a
// segmented SDU. The receive without an ok-check yielded a nil pdu whose
// payload() panicked, taking down the whole process.
func TestReadClosedMidReassembly(t *testing.T) {
	c := &Conn{chInPDU: make(chan pdu, 1)}

	// First fragment of an SDU that claims 10 payload bytes but carries 4:
	// Read must loop for more fragments.
	first := make(pdu, 4+4)
	binary.LittleEndian.PutUint16(first[0:2], 10)       // dlen
	binary.LittleEndian.PutUint16(first[2:4], cidLEAtt) // cid
	c.chInPDU <- first
	close(c.chInPDU) // disconnect before the continuation arrives

	n, err := c.Read(make([]byte, 64))
	if n != 0 || err == nil {
		t.Fatalf("Read = (%d, %v), want (0, error)", n, err)
	}
	if !errors.Is(err, ErrClosed) || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Read error %v must match ErrClosed and io.ErrClosedPipe", err)
	}
}

// TestReadClosedBeforeFirstPDU covers the pre-existing guarded path for
// completeness: closing before any PDU arrives must also error, not panic.
func TestReadClosedBeforeFirstPDU(t *testing.T) {
	c := &Conn{chInPDU: make(chan pdu)}
	close(c.chInPDU)
	if _, err := c.Read(make([]byte, 64)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Read error %v must match io.ErrClosedPipe", err)
	}
}
