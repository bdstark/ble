# ble

**ble** is a Go [Bluetooth Low Energy](https://en.wikipedia.org/wiki/Bluetooth_Low_Energy)
package for Linux (raw HCI sockets) and macOS (CoreBluetooth via cbgo).

This is a maintained fork of the dormant [go-ble/ble](https://github.com/go-ble/ble)
(merge base `8c5522f`). The module is renamed `github.com/bdstark/ble` and the
API diverges deliberately — it will not be upstreamed. It exists to run an
unattended BLE telemetry gateway, so the changes are biased toward one goal:
**the stack must never wedge, and every failure must surface as a typed,
bounded error.**

Linux is the production target. The macOS backend is for development; several
of the guarantees below (bounded waits, connection-parameter control) apply
only to the Linux stack.

- Examples: [bdstark/ble-examples](https://github.com/bdstark/ble-examples)
- Test coverage of the fork's changes: [COVERAGE.md](COVERAGE.md)
  (diff coverage of added lines: 91.7% overall, 99.6% excluding `darwin/`)
- CI: GitHub Actions runs build/vet/test (plus `-race` and a repeated-run
  pass over the HCI/ATT/GATT packages) on Linux and macOS

Requires Go 1.26+. On Linux the process needs `CAP_NET_ADMIN` (or root) to
open the HCI socket, and the interface must be free of a competing host stack
(stop or mask `bluetoothd`, or run on a dedicated controller).

```sh
go get github.com/bdstark/ble
```

---

## What changed relative to upstream

### API

- **`context.Context` threads through the entire `ble.Client` request API.**
  Every GATT operation — discovery, reads, writes, subscribe/unsubscribe,
  `ExchangeMTU`, `ReadRSSI` — takes a `ctx` that bounds the caller's wait.
  On expiry you get `ctx.Err()` (possibly wrapped; `errors.Is` against
  `context.Canceled` / `context.DeadlineExceeded` holds). Cancellation is
  best-effort at the transport layer: an in-flight ACL write finishes, but it
  is independently bounded by `hci.ACLWriteTimeout`. A request abandoned
  after it reached the wire still owns the ATT bearer — the next request
  first waits out that transaction (late response, or its 30 s spec deadline,
  which closes the bearer), so a canceled request can never cause a later
  one to consume a stale response. Teardown (`CancelConnection`) deliberately
  takes no context so a client can always be torn down.
- **Typed sentinel errors, stdlib wrapping.** `pkg/errors` is gone. Failures
  wrap sentinels you can test with `errors.Is`: `ble.ErrNotImplemented`,
  `ble.ErrInvalidConnParams`, `ble.ErrInvalidDataLength`, and on Linux
  `hci.ErrClosed` (wraps `io.ErrClosedPipe`), `hci.ErrCreditTimeout`,
  `hci.ErrCommandTimeout`, `hci.ErrConnUpdateTimeout`,
  `att.ErrSeqProtoTimeout`, plus the spec-defined `ble.ATTError` and
  `hci.ErrCommand` code types.
- **`Conn.UpdateParams(ctx, ble.ConnParams{...})`** — LE Connection Update on
  a live central link, expressed in `time.Duration` units, validated against
  the spec ranges before touching the controller, blocking until the
  controller reports completion. Peer-requested (L2CAP) updates and local
  requests share the per-connection update slot, so they cannot misattribute
  each other's completion events.
- **`Conn.SetDataLength(ctx, txOctets, txTime)`** — LE Data Length Extension
  request, validated by `ble.ValidateDataLength`; the negotiated values are
  readable via the Linux conn's `DataLength()` getter as LE Data Length
  Change events arrive.
- **API repairs:** `ReadRSSI` performs a real HCI Read RSSI exchange (it
  returned a fabricated `0` upstream), `DiscoverIncludedServices` is
  implemented (upstream returned `nil, nil`), and `Client.Name()` reads the
  GAP Device Name characteristic, caching deterministic outcomes rather than
  re-querying on every call.
- **`ble.Connect` cancellation race fixed.** Upstream decided "found" by
  which error the scan returned; a parent-context expiry could race the
  match, and `Connect` would block forever on a channel nobody would send
  to. Found-ness is now decided solely by draining a buffered found channel.
  This wedge was hit in production.
- **Options propagate setter errors** instead of silently discarding them,
  and device init failures fail `NewDevice`/`Init` instead of returning a
  booby-trapped half-initialized HCI.
- **Notification handler contract:** the `[]byte` passed to a
  `NotificationHandler` is backed by a pooled buffer on Linux and is valid
  only for the duration of the call — copy it if you keep it.

### Reliability (Linux HCI/ATT)

- **Every indefinite channel wait in the stack is bounded.** Command
  responses, ACL buffer credits, connection updates, disconnects, connection
  cancels — all have timeouts, hoisted into package variables (the exported
  `hci.ACLWriteTimeout` is the public knob; the rest exist so tests can
  shrink them), so a dead controller or peer produces a typed error instead
  of a parked goroutine.
- **Controller-state reconciliation after abandoned commands.** When a
  context expiry abandons an HCI command that the controller later executes
  anyway (the classic "Command Disallowed on every subsequent scan" wedge),
  the stack reconciles its scan/dial state instead of desyncing permanently.
- **`sktLoop` wedge classes closed.** Socket death tears down all
  connections; reads/writes on a dying transport return `hci.ErrClosed`
  rather than stalling; `Socket.Close` is idempotent and no longer depends
  on the controller being alive to complete.
- **Connection lifecycle fixes:** a lost Disconnect command no longer leaves
  a zombie connection; a failed connect no longer leaks its conn; one
  connection's wait for ACL credits no longer stalls writes on every other
  connection; mid-reassembly disconnects no longer panic.
- **ATT bearer discipline per spec:** an ATT transaction timeout or an
  unconfirmed indication poisons the bearer and closes it (Vol 3, Part F),
  rather than leaving a half-dead exchange to corrupt the next request.

### Protocol correctness

- **Client RX paths hardened** against runt, malformed, stale, and hostile
  PDUs — a misbehaving peer gets an error or a disconnect, not a panic.
- **ATT server:** nine inherited bugs fixed, among them three remotely
  triggerable panics, an `ExecuteWrite` panic, and `PrepareWrite` dropping
  the queued value. `ExchangeMTU` handling conforms to Vol 3, Part F
  3.4.2.2.
- **GATT server subscription state** records only peer-acknowledged CCCD
  writes, so notification state cannot outrun the peer.
- **Wire-format fixes:** `CommandReject.Marshal` produced unsendable frames;
  SMP frames parsed the opcode from the wrong offset with an incorrect
  data length. Both fixed.

### Performance

- Advertising delivery runs through a **bounded dispatcher with pooled
  buffers** instead of upstream's goroutine-per-advertisement; the parsed
  advertisement packet is cached across field accesses.
- Receive-path buffers are pooled where lifetimes provably allow it; ACL
  packet headers are built without reflection. Micro-benchmarks cover the
  hot paths.

### Observability & housekeeping

- Logging is stdlib **`log/slog`** via the `ble.Logger` package variable
  (logxi is gone). Hot-path debug sites check `Enabled` first, so debug-off
  costs nothing.
- `examples/` moved to
  [bdstark/ble-examples](https://github.com/bdstark/ble-examples); dead
  pre-cbgo darwin XPC code deleted; modern idiomatic Go throughout
  (`any`, `min`, `slices`, …).
- The darwin backend's `ctx.Done` abandon paths can no longer deadlock the
  CoreBluetooth dispatch thread.
- Diff coverage of every fork change is tracked in
  [COVERAGE.md](COVERAGE.md) and measured with
  [tools/diffcover.py](tools/diffcover.py).

---

## Usage

### Device setup

```go
import (
    "github.com/bdstark/ble"
    "github.com/bdstark/ble/linux"
)

d, err := linux.NewDevice()           // opens and initializes the default HCI device
if err != nil {
    log.Fatalf("can't open BLE device: %v", err)
}
ble.SetDefaultDevice(d)
```

Options are validated and their errors surface:

```go
d, err := linux.NewDevice(
    ble.OptDeviceID(1),               // hci1 instead of hci0
    ble.OptCentralRole(),
)
```

### Scanning

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err := ble.Scan(ctx, false, func(a ble.Advertisement) {
    fmt.Printf("%s %s rssi=%d\n", a.Addr(), a.LocalName(), a.RSSI())
}, nil)
if err != nil && !errors.Is(err, context.DeadlineExceeded) {
    log.Printf("scan failed: %v", err)
}
```

The handler runs on the bounded advertising dispatcher — keep it fast, and
copy anything you retain from the advertisement.

### Connecting and discovering

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Connect scans until an advertisement matches, then dials it.
cln, err := ble.Connect(ctx, func(a ble.Advertisement) bool {
    return a.LocalName() == "my-sensor"
})
if err != nil {
    log.Fatalf("connect: %v", err)   // errors.Is(err, context.DeadlineExceeded) on timeout
}
defer cln.CancelConnection()

p, err := cln.DiscoverProfile(ctx, true)
if err != nil {
    log.Fatalf("discover: %v", err)
}
```

If you already know the address, dial directly: `ble.Dial(ctx, ble.NewAddr("aa:bb:cc:dd:ee:ff"))`.

### Reading and writing characteristics

```go
char := p.FindCharacteristic(ble.NewCharacteristic(ble.MustParse("2a19"))) // Battery Level
if char == nil {
    log.Fatal("characteristic not found")
}

opCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

val, err := cln.ReadCharacteristic(opCtx, char)
if err != nil {
    log.Fatalf("read: %v", err)
}

err = cln.WriteCharacteristic(opCtx, char, []byte{0x01}, false /* with response */)
```

### Notifications and indications

```go
err := cln.Subscribe(ctx, char, false /* notify, true = indicate */, func(data []byte) {
    // data is only valid inside this call — it is backed by a pooled
    // buffer. Copy before sending it anywhere.
    buf := make([]byte, len(data))
    copy(buf, data)
    readings <- buf
})
if err != nil {
    log.Fatalf("subscribe: %v", err)
}

// Block until the peer drops or you decide to leave.
select {
case <-cln.Disconnected():
    log.Print("peer disconnected")
case <-ctx.Done():
    _ = cln.CancelConnection()
}
```

### Connection parameters and data length (Linux, central role)

```go
// Relax the connection interval on a live link, e.g. to share radio time
// across several concurrent connections.
err := cln.Conn().UpdateParams(ctx, ble.ConnParams{
    IntervalMin: 30 * time.Millisecond,
    IntervalMax: 50 * time.Millisecond,
    Latency:     0,
    Timeout:     4 * time.Second,
})
if errors.Is(err, ble.ErrInvalidConnParams) {
    // rejected locally before touching the controller
}

// Ask for the controller's maximum LE packet length (fewer, larger packets).
err = cln.Conn().SetDataLength(ctx, ble.DataLengthMaxTxOctets, ble.DataLengthMaxTxTime)
```

`UpdateParams` blocks until the controller reports the update complete.
`SetDataLength` returns once the controller accepts the command; the
negotiated values arrive asynchronously (readable on the Linux conn via
`DataLength()`). On backends that manage these themselves (CoreBluetooth),
both return a wrapped `ble.ErrNotImplemented`.

### Error handling

Wrapped sentinels make failure modes distinguishable:

```go
_, err := cln.ReadCharacteristic(ctx, char)
switch {
case errors.Is(err, context.DeadlineExceeded):
    // this call's ctx expired; the link may still be fine
case errors.Is(err, att.ErrSeqProtoTimeout):
    // ATT transaction timed out; the bearer is poisoned, reconnect
case errors.Is(err, hci.ErrClosed): // also matches io.ErrClosedPipe
    // transport is gone
}

var attErr ble.ATTError
if errors.As(err, &attErr) && attErr == ble.ErrReadNotPerm {
    // peer refused the read at the protocol level
}
```

### Tuning timeouts

The bounded waits use package variables; adjust them before opening a device
if your controller or peers need different ceilings:

```go
hci.ACLWriteTimeout = 30 * time.Second  // wait for ACL buffer credits
```

### Logging

```go
ble.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
    Level: slog.LevelDebug,             // debug enables packet-level dumps
}))
```

### Peripheral role (advertising a GATT server)

```go
d, _ := linux.NewDevice(ble.OptPeripheralRole())
ble.SetDefaultDevice(d)

svc := ble.NewService(ble.MustParse("19fad5e6-0000-4a86-9e44-95b9cee0f730"))
svc.NewCharacteristic(ble.MustParse("19fad5e6-0001-4a86-9e44-95b9cee0f730")).
    HandleRead(ble.ReadHandlerFunc(func(req ble.Request, rsp ble.ResponseWriter) {
        rsp.Write([]byte("hello"))
    }))
if err := ble.AddService(svc); err != nil {
    log.Fatal(err)
}

ctx := ble.WithSigHandler(context.WithCancel(context.Background()))
log.Fatal(ble.AdvertiseNameAndServices(ctx, "my-device", svc.UUID))
```

---

## Testing

The linux backend is pure Go above the socket layer, so `linux/hci` and
`linux/att` build and test on any platform:

```sh
go build ./...
go vet ./...
go test ./...
```

Diff coverage of the fork's changes (see [COVERAGE.md](COVERAGE.md)):

```sh
go test -count=1 -coverprofile=/tmp/cover.out -coverpkg=./...,github.com/bdstark/ble/... ./...
python3 tools/diffcover.py . 8c5522f..HEAD /tmp/cover.out
```

## License

BSD-3-Clause, inherited from upstream — see [LICENSE](LICENSE).
