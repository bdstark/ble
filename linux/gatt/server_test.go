package gatt

import (
	"testing"
	"time"

	"github.com/go-ble/ble"
)

// defaultHanderFunc must return once the notifier is unsubscribed (its
// context cancelled) instead of spinning forever.
func TestDefaultHandlerFuncReturnsWhenUnsubscribed(t *testing.T) {
	n := ble.NewNotifier(func(b []byte) (int, error) { return len(b), nil })
	n.Close() // cancel the subscription before the handler even starts

	done := make(chan struct{})
	go func() {
		defaultHanderFunc(nil, n)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("defaultHanderFunc did not return after the notifier was closed")
	}
}
