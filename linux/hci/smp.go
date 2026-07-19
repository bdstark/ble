package hci

import (
	"encoding/binary"
	"fmt"

	"github.com/go-ble/ble"
)

const (
	pairingRequest           = 0x01 // Pairing Request LE-U, ACL-U
	pairingResponse          = 0x02 // Pairing Response LE-U, ACL-U
	pairingConfirm           = 0x03 // Pairing Confirm LE-U
	pairingRandom            = 0x04 // Pairing Random LE-U
	pairingFailed            = 0x05 // Pairing Failed LE-U, ACL-U
	encryptionInformation    = 0x06 // Encryption Information LE-U
	masterIdentification     = 0x07 // Master Identification LE-U
	identiInformation        = 0x08 // Identity Information LE-U, ACL-U
	identityAddreInformation = 0x09 // Identity Address Information LE-U, ACL-U
	signingInformation       = 0x0A // Signing Information LE-U, ACL-U
	securityRequest          = 0x0B // Security Request LE-U
	pairingPublicKey         = 0x0C // Pairing Public Key LE-U
	pairingDHKeyCheck        = 0x0D // Pairing DHKey Check LE-U
	pairingKeypress          = 0x0E // Pairing Keypress Notification LE-U
)

// smpPairingNotSupported is the Pairing Failed reason code for
// "Pairing Not Supported". [Vol 3, Part H, 3.5.5]
const smpPairingNotSupported = 0x05

// sendSMP writes one SMP command as a B-frame on the SMP fixed channel
// [Vol 3, Part A, 3.1]: length (2 bytes LE, the SMP command length only),
// CID (2 bytes LE), then the command. Assembled with direct byte writes
// (see writeACLData): binary.Write pays a reflection pass per call.
func (c *Conn) sendSMP(p pdu) error {
	frame := make([]byte, 4+len(p))
	binary.LittleEndian.PutUint16(frame[0:2], uint16(len(p)))
	binary.LittleEndian.PutUint16(frame[2:4], cidSMP)
	copy(frame[4:], p)
	if logDebugEnabled() {
		ble.Logger.Debug("smp send", "pdu", fmt.Sprintf("[%X]", frame))
	}
	_, err := c.writePDU(frame)
	return err
}

// handleSMP responds to an incoming SMP command. p is the full L2CAP frame
// as recombined off the wire (4-byte basic header + payload); the SMP opcode
// is the first payload byte. Pairing is not implemented, so every recognized
// command is answered with Pairing Failed / Pairing Not Supported.
func (c *Conn) handleSMP(p pdu) error {
	if logDebugEnabled() {
		ble.Logger.Debug("smp recv", "pdu", fmt.Sprintf("[%X]", p))
	}
	// A frame too short to carry an opcode is malformed; drop it rather
	// than trusting the peer's framing.
	if len(p) < 4+1 {
		ble.Logger.Error("smp: dropping truncated PDU", "pdu", fmt.Sprintf("[%X]", p))
		return nil
	}
	code := p.payload()[0]
	switch code {
	case pairingRequest:
	case pairingResponse:
	case pairingConfirm:
	case pairingRandom:
	case pairingFailed:
		// Pairing Failed terminates the procedure and must not be answered
		// [Vol 3, Part H, 3.5.5] — replying would make two rejecting stacks
		// ping-pong Pairing Failed at each other.
		return nil
	case encryptionInformation:
	case masterIdentification:
	case identiInformation:
	case identityAddreInformation:
	case signingInformation:
	case securityRequest:
	case pairingPublicKey:
	case pairingDHKeyCheck:
	case pairingKeypress:
		// Keypress notifications are informational; nothing to reject.
		return nil
	default:
		// If a packet is received with a reserved Code it shall be ignored. [Vol 3, Part H, 3.3]
		return nil
	}
	// FIXME: work aound to the lack of SMP implementation - always return non-supported.
	// C.5.1 Pairing Not Supported by Slave
	return c.sendSMP([]byte{pairingFailed, smpPairingNotSupported})
}
