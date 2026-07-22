package ble

import (
	"io"
	"log/slog"
	"sync"
	"testing"
)

// TestLoggerDefault: with nothing set, Logger falls back to slog.Default and
// never returns nil (log sites dereference it directly).
func TestLoggerDefault(t *testing.T) {
	logger.Store(nil)
	if Logger() == nil {
		t.Fatal("Logger() returned nil with no logger set")
	}
	if Logger() != slog.Default() {
		t.Fatal("Logger() did not fall back to slog.Default()")
	}
}

// TestSetLoggerRoundTrip: SetLogger installs a logger; SetLogger(nil) reverts
// to the default.
func TestSetLoggerRoundTrip(t *testing.T) {
	t.Cleanup(func() { logger.Store(nil) })
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	SetLogger(l)
	if Logger() != l {
		t.Fatal("Logger() did not return the logger set by SetLogger")
	}
	SetLogger(nil)
	if Logger() != slog.Default() {
		t.Fatal("SetLogger(nil) did not revert to slog.Default()")
	}
}

// TestLoggerSwapRace: logging and SetLogger race by design — a consumer may
// reconfigure logging while the stack logs from several goroutines. Access
// is atomic, so this is safe; under -race it proves the plain-variable data
// race is gone (a non-atomic Log()/SetLogger over a shared var fails here).
//
// Readers and writers both run as concurrent goroutines: a writer loop with
// no scheduling point can finish all its stores before any reader is
// scheduled, hiding the race, so both sides must be live at once.
func TestLoggerSwapRace(t *testing.T) {
	t.Cleanup(func() { logger.Store(nil) })
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	const iters = 2000
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				Logger().Info("concurrent log line")
			}
		}()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				SetLogger(discard)
				SetLogger(nil)
			}
		}()
	}
	wg.Wait()
}
