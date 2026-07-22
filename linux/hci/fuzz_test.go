package hci

import (
	"io"
	"testing"

	"github.com/bdstark/ble/linux/hci/evt"
)

// FuzzHandleLEAdvertisingReport throws arbitrary LE Advertising Report
// payloads at the scan path — the parse runs on sktLoop, where a panic takes
// down the whole adapter — and then walks every Advertisement accessor on
// whatever was delivered, since those run in the user's handler over
// peer-controlled AD structures.
func FuzzHandleLEAdvertisingReport(f *testing.F) {
	f.Add(advReportPkt(evtTypAdvInd, 0x01, []byte{0x02, 0x01, 0x06}))
	f.Add(advReportPkt(evtTypScanRsp, 0x01, []byte{0x06, 0x09, 'w', 'o', 'r', 'l', 'd'}))
	f.Add(advReportPkt(evtTypAdvNonconnInd, 0x02, nil))
	f.Add(multiAdvReportPkt(
		advTestReport{evtTyp: evtTypAdvInd, addrLo: 0x01, data: []byte{0x02, 0x01, 0x06}},
		advTestReport{evtTyp: evtTypScanRsp, addrLo: 0x01, data: []byte{0x03, 0x19, 0x00, 0x01}},
	))
	f.Add([]byte{0x02, 0x01})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		// A fresh HCI per input: handleLEAdvertisingReport retains its
		// argument (adHist and the queued Advertisement alias it), so a
		// shared instance would carry one input's history into the next,
		// and the fuzz engine reuses the input backing array between execs.
		// The real stack gives each LE Meta packet a private buffer (see
		// poolableEvtPkt); the copy mirrors that.
		h := newAdvHCI(64)
		_ = h.handleLEAdvertisingReport(append([]byte(nil), b...))
		for {
			select {
			case d := <-h.chAdv:
				a := d.a
				_ = a.LocalName()
				_ = a.ManufacturerData()
				_ = a.ServiceData()
				_ = a.Services()
				_ = a.OverflowService()
				_ = a.TxPowerLevel()
				_ = a.SolicitedService()
				_ = a.Connectable()
				_ = a.RSSI()
				_ = a.Addr()
				_ = a.EventType()
				_ = a.AddressType()
				_ = a.Data()
				_ = a.ScanResponse()
			default:
				return
			}
		}
	})
}

// sinkSkt swallows writes and immediately returns the TX credit, so a fuzz
// run can emit millions of SMP replies without accumulating write records
// (fakeSkt keeps every write) or exhausting the credit pool.
type sinkSkt struct{ c *Conn }

func (s *sinkSkt) Read(p []byte) (int, error)  { return 0, io.EOF }
func (s *sinkSkt) Write(p []byte) (int, error) { s.c.txBuffer.Put(); return len(p), nil }
func (s *sinkSkt) Close() error                { return nil }

// FuzzHandleSMP throws arbitrary L2CAP SMP frames at the security-manager
// stub. The peer controls every byte; the handler must refuse, drop, or
// reply — never panic or wedge.
func FuzzHandleSMP(f *testing.F) {
	f.Add([]byte{0x07, 0x00, 0x06, 0x00, 0x01, 0x03, 0x00, 0x01, 0x10, 0x07, 0x07}) // Pairing Request
	f.Add([]byte{0x02, 0x00, 0x06, 0x00, 0x05, 0x05})                               // Pairing Failed
	f.Add([]byte{0x01, 0x00, 0x06, 0x00, 0x0E})                                     // Keypress
	f.Add([]byte{0x01, 0x00, 0x06, 0x00, 0xFF})                                     // reserved opcode
	f.Add([]byte{})

	c := &Conn{
		param:    make(evt.LEConnectionComplete, 19),
		chDone:   make(chan struct{}),
		txBuffer: newTxCredits(NewPool(64, 4)),
		txMTU:    23,
	}
	c.hci = &HCI{skt: &sinkSkt{c: c}}
	f.Fuzz(func(t *testing.T, b []byte) {
		_ = c.handleSMP(b)
	})
}
