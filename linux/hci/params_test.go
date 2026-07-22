package hci

import (
	"sync"
	"testing"

	"github.com/bdstark/ble/linux/hci/cmd"
)

// TestParamsConcurrentCommands: Scan/StopScanning, Advertise/StopAdvertising,
// SetAdvertisement, and the option setters all mutate the shared h.params
// command structs from caller goroutines while sktLoop's event handlers read
// them under RLock — by design (a scan can be toggled while an advertisement
// is being reconfigured). Every mutator must therefore mutate under the
// params lock and Send a private copy; running them together lets the race
// detector prove it. (The pre-fix code mutated and marshaled the shared
// fields with no lock at all — this test fails under -race there.)
func TestParamsConcurrentCommands(t *testing.T) {
	quietLogger(t)
	skt := newFakeSkt()
	respondWithStatus(skt, 0x00)
	h := newLoopedHCI(t, skt)

	const iters = 100
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				fn()
			}
		}()
	}
	// Each mutator in its OWN goroutine: the race pairs under test are
	// cross-goroutine accesses to one params field (e.g. Advertise vs
	// StopAdvertising on advEnable), which a shared goroutine would
	// serialize away.
	run(func() { _ = h.Scan(false) })
	run(func() { _ = h.StopScanning() })
	run(func() { _ = h.Advertise() })
	run(func() { _ = h.StopAdvertising() })
	run(func() { _ = h.SetAdvertisement([]byte{0x02, 0x01, 0x06}, []byte{0x02, 0x0A, 0x00}) })
	run(func() {
		_ = h.SetAdvParams(cmd.LESetAdvertisingParameters{AdvertisingIntervalMin: 0x20, AdvertisingIntervalMax: 0x20})
		_ = h.SetScanParams(cmd.LESetScanParameters{LEScanInterval: 0x04, LEScanWindow: 0x04})
		_ = h.SetConnParams(cmd.LECreateConnection{ConnIntervalMin: 0x06, ConnIntervalMax: 0x06})
	})
	// A disconnect event reads advEnable under RLock on sktLoop's side and
	// re-sends a copy — mix it in via the slave re-advertise arm's sibling,
	// the connection-complete handler's advertising check.
	run(func() {
		if err := h.handleLEConnectionComplete(leConnCompletePkt(0x3E, 0x0040, roleSlave)); err != nil {
			t.Errorf("handleLEConnectionComplete: %v", err)
		}
	})
	wg.Wait()
}
