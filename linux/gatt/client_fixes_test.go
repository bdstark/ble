package gatt

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/bdstark/ble"
	"github.com/bdstark/ble/linux/att"
)

// serveSlow answers Write Requests immediately but delays each Read Response
// by delay, sending on readStarted as soon as the Read Request arrives — at
// that point the client-side request method is parked mid-round-trip holding
// the client-wide lock.
func serveSlow(conn *fakeConn, delay time.Duration, readStarted chan<- struct{}) {
	for {
		var req []byte
		select {
		case req = <-conn.writes:
		case <-conn.closed:
			return
		}
		var rsp []byte
		switch req[0] {
		case att.WriteRequestCode:
			rsp = []byte{att.WriteResponseCode}
		case att.ReadRequestCode:
			readStarted <- struct{}{}
			select {
			case <-time.After(delay):
			case <-conn.closed:
				return
			}
			rsp = []byte{att.ReadResponseCode, 0x01}
		default:
			continue
		}
		select {
		case conn.in <- rsp:
		case <-conn.closed:
			return
		}
	}
}

// A notification must be delivered while another goroutine's request holds
// the client lock for its full ATT round trip, not queued behind it.
func TestNotificationDeliveredDuringInFlightRequest(t *testing.T) {
	const hold = 500 * time.Millisecond
	conn := newFakeConn()
	readStarted := make(chan struct{}, 1)
	go serveSlow(conn, hold, readStarted)
	cln, err := NewClient(conn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx := testCtx(t)

	c := &ble.Characteristic{ValueHandle: 3, CCCD: &ble.Descriptor{Handle: 4}}
	notified := make(chan []byte, 1)
	if err := cln.Subscribe(ctx, c, false, func(b []byte) {
		notified <- append([]byte(nil), b...)
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := cln.ReadCharacteristic(ctx, &ble.Characteristic{ValueHandle: 3})
		readDone <- err
	}()
	<-readStarted // ReadCharacteristic now holds the client lock at the server

	injected := time.Now()
	conn.in <- []byte{att.HandleValueNotificationCode, 0x03, 0x00, 0xDE, 0xAD}
	select {
	case b := <-notified:
		if took := time.Since(injected); took > 100*time.Millisecond {
			t.Fatalf("notification took %v; it appears to have waited for the in-flight request (%v)", took, hold)
		}
		if !bytes.Equal(b, []byte{0xDE, 0xAD}) {
			t.Fatalf("notification payload = % X, want DE AD", b)
		}
	case <-time.After(2 * hold):
		t.Fatal("notification never delivered while a request was in flight")
	}
	select {
	case <-readDone:
		t.Fatal("slow read finished before the notification was delivered")
	default:
	}
	if err := <-readDone; err != nil {
		t.Fatalf("ReadCharacteristic: %v", err)
	}
}

// A handler that calls back into the same Client must not deadlock (the old
// code invoked handlers under the client's non-reentrant write lock).
func TestNotificationHandlerMayCallBackIntoClient(t *testing.T) {
	cln, _ := newTestClient(t)
	ctx := testCtx(t)

	c := &ble.Characteristic{ValueHandle: 3, CCCD: &ble.Descriptor{Handle: 4}}
	result := make(chan error, 1)
	if err := cln.Subscribe(ctx, c, false, func([]byte) {
		_, err := cln.ReadCharacteristic(ctx, &ble.Characteristic{ValueHandle: 3, EndHandle: 5})
		result <- err
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cln.Conn().(*fakeConn).in <- []byte{att.HandleValueNotificationCode, 0x03, 0x00, 0x01}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("ReadCharacteristic from inside a notification handler: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler calling back into the Client deadlocked")
	}
}

// Pin drop semantics: a notification for an unregistered handle is dropped
// (today it also logs a warning through the global ble.Logger, which we leave
// unasserted to avoid racing on the global), an indication for a sub with no
// indication handler is dropped, and registered handlers get req[3:].
func TestUnmatchedNotificationsAreDropped(t *testing.T) {
	p := &Client{subs: make(map[uint16]*sub)}

	// Unknown handle: logged and dropped, no panic.
	p.HandleNotification([]byte{att.HandleValueNotificationCode, 0x99, 0x00, 0x01})

	var gotN, gotI []byte
	p.subs[3] = &sub{cccdh: 4, ccc: cccNotify, nHandler: func(b []byte) {
		gotN = append([]byte(nil), b...)
	}}

	// Indication for a sub whose indication handler is nil: dropped.
	p.HandleNotification([]byte{att.HandleValueIndicationCode, 0x03, 0x00, 0x01})
	if gotN != nil || gotI != nil {
		t.Fatalf("handler invoked for an unsubscribed indication (n: % X, i: % X)", gotN, gotI)
	}

	p.subs[3].iHandler = func(b []byte) { gotI = append([]byte(nil), b...) }
	p.HandleNotification([]byte{att.HandleValueNotificationCode, 0x03, 0x00, 0xAA, 0xBB})
	p.HandleNotification([]byte{att.HandleValueIndicationCode, 0x03, 0x00, 0xCC})
	if !bytes.Equal(gotN, []byte{0xAA, 0xBB}) {
		t.Fatalf("notification payload = % X, want AA BB", gotN)
	}
	if !bytes.Equal(gotI, []byte{0xCC}) {
		t.Fatalf("indication payload = % X, want CC", gotI)
	}
}

// Subscribe/Unsubscribe (write lock) racing HandleNotification (read-lock
// snapshot) — run under -race; the assertion is no race and no deadlock.
func TestConcurrentSubscriptionChurn(t *testing.T) {
	cln, _ := newTestClient(t)
	ctx := testCtx(t)

	c := &ble.Characteristic{ValueHandle: 3, CCCD: &ble.Descriptor{Handle: 4}}
	// Seed the sub entry so the churn below doesn't spam unregistered-handle
	// warnings before the first Subscribe lands (entries persist with nil
	// handlers after Unsubscribe).
	if err := cln.Subscribe(ctx, c, false, func([]byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := cln.Unsubscribe(ctx, c, false); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	const iters = 100
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if err := cln.Subscribe(ctx, c, false, func([]byte) {}); err != nil {
				t.Errorf("Subscribe(notify) #%d: %v", i, err)
				return
			}
			if err := cln.Unsubscribe(ctx, c, false); err != nil {
				t.Errorf("Unsubscribe(notify) #%d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if err := cln.Subscribe(ctx, c, true, func([]byte) {}); err != nil {
				t.Errorf("Subscribe(indicate) #%d: %v", i, err)
				return
			}
			if err := cln.Unsubscribe(ctx, c, true); err != nil {
				t.Errorf("Unsubscribe(indicate) #%d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			cln.HandleNotification([]byte{att.HandleValueNotificationCode, 0x03, 0x00, byte(i)})
			cln.HandleNotification([]byte{att.HandleValueIndicationCode, 0x03, 0x00, byte(i)})
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent subscription churn deadlocked")
	}
}

// ---------------------------------------------------------------------------
// setHandlers: local state records only peer-acknowledged CCCD writes
// ---------------------------------------------------------------------------

// TestSubscribeFailedWriteAllowsRetry: a rejected CCCD write must leave the
// subscription un-recorded, so the retry actually writes again. Previously
// s.ccc was set before the write; the retry hit the already-subscribed
// early return and reported success for a CCCD the peer never enabled.
func TestSubscribeFailedWriteAllowsRetry(t *testing.T) {
	cln, srv := newTestClient(t)
	ctx := testCtx(t)
	c := &ble.Characteristic{
		ValueHandle: 3,
		CCCD:        &ble.Descriptor{UUID: ble.ClientCharacteristicConfigUUID, Handle: 4},
	}

	srv.setFailWrites(true)
	if err := cln.Subscribe(ctx, c, false, func([]byte) {}); err == nil {
		t.Fatal("Subscribe with a rejected CCCD write returned nil")
	}

	notified := make(chan []byte, 1)
	srv.setFailWrites(false)
	if err := cln.Subscribe(ctx, c, false, func(b []byte) {
		notified <- append([]byte(nil), b...)
	}); err != nil {
		t.Fatalf("Subscribe retry: %v", err)
	}
	// The retry must have reached the wire (the failed attempt recorded no
	// write), and notifications must flow.
	checkCCCDWrite(t, srv, 0, 4, cccNotify)
	srv.conn.in <- []byte{att.HandleValueNotificationCode, 0x03, 0x00, 0x01}
	select {
	case <-notified:
	case <-time.After(2 * time.Second):
		t.Fatal("no notification after successful retry")
	}
}

// TestResubscribeReplacesHandler: Subscribe on an already-enabled flag must
// install the new handler (with no extra CCCD write), not silently keep the
// old one.
func TestResubscribeReplacesHandler(t *testing.T) {
	cln, srv := newTestClient(t)
	ctx := testCtx(t)
	c := &ble.Characteristic{
		ValueHandle: 3,
		CCCD:        &ble.Descriptor{UUID: ble.ClientCharacteristicConfigUUID, Handle: 4},
	}

	first := make(chan []byte, 1)
	if err := cln.Subscribe(ctx, c, false, func(b []byte) { first <- b }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	second := make(chan []byte, 1)
	if err := cln.Subscribe(ctx, c, false, func(b []byte) { second <- b }); err != nil {
		t.Fatalf("re-Subscribe: %v", err)
	}
	if n := len(srv.writes()); n != 1 {
		t.Fatalf("re-Subscribe wrote the CCCD again (%d writes, want 1)", n)
	}

	srv.conn.in <- []byte{att.HandleValueNotificationCode, 0x03, 0x00, 0x02}
	select {
	case <-second:
	case <-first:
		t.Fatal("notification delivered to the replaced handler")
	case <-time.After(2 * time.Second):
		t.Fatal("notification delivered to no handler")
	}
}

// TestUnsubscribeFailedWriteKeepsHandler: a rejected disable write must keep
// the subscription intact — the peer still believes it's enabled, so late
// notifications must still find the handler, and a retry must write again.
func TestUnsubscribeFailedWriteKeepsHandler(t *testing.T) {
	cln, srv := newTestClient(t)
	ctx := testCtx(t)
	c := &ble.Characteristic{
		ValueHandle: 3,
		CCCD:        &ble.Descriptor{UUID: ble.ClientCharacteristicConfigUUID, Handle: 4},
	}

	notified := make(chan []byte, 1)
	if err := cln.Subscribe(ctx, c, false, func(b []byte) { notified <- b }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	srv.setFailWrites(true)
	if err := cln.Unsubscribe(ctx, c, false); err == nil {
		t.Fatal("Unsubscribe with a rejected CCCD write returned nil")
	}
	srv.conn.in <- []byte{att.HandleValueNotificationCode, 0x03, 0x00, 0x03}
	select {
	case <-notified:
	case <-time.After(2 * time.Second):
		t.Fatal("handler forgotten although the peer still has notifications enabled")
	}

	srv.setFailWrites(false)
	if err := cln.Unsubscribe(ctx, c, false); err != nil {
		t.Fatalf("Unsubscribe retry: %v", err)
	}
	checkCCCDWrite(t, srv, 1, 4, 0x0000)
}
