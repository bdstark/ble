# Test coverage of this fork's changes

This fork carries a series of changes on top of upstream `go-ble/ble`
(merge base `8c5522f`): bounded channel waits everywhere the HCI/ATT
stack used to park forever, a `context.Context`-threaded client request
API, typed sentinel errors, a bounded advertising dispatcher, receive-path
buffer pooling, ACL header construction without reflection, and
`log/slog` logging.

**Diff coverage** — the fraction of *added* lines that land in coverable
statements and are executed by `go test` — is the tracked metric, measured
with [tools/diffcover.py](tools/diffcover.py):

```sh
go test -count=1 -coverprofile=/tmp/cover.out -coverpkg=./...,github.com/bdstark/ble/... ./...
python3 tools/diffcover.py . 8c5522f..HEAD /tmp/cover.out
```

## Current state (2026-07-21, measured on macOS)

| Scope | Coverage |
|---|---|
| All added lines | **91.7%** (1512/1649) |
| Added lines excluding `darwin/` | **99.6%** (1512/1518) |

Every changed file is at 100% of its added lines except the ones below.
The linux backend is pure Go above the socket layer, so all of it —
including `linux/hci` and `linux/att` — builds and tests on any platform.

## Documented exclusions (why 90%+ overall is barely attainable)

**`darwin/` — 131 uncovered lines (the entire shortfall; `darwin/option.go`
and `darwin/event.go` are fully covered by unit tests).** The uncovered
lines are `ctx.Done()` arms in selects that otherwise wait on CoreBluetooth
delegate events, the context-threaded method signatures around them, the
`awaitSlot` delegate-event waiter, and the delegate callbacks themselves.
The backend drives `cbgo`, whose peripheral/central types are concrete
structs wrapping Objective-C objects — there is no seam to fake them, and
reaching the changed arms requires a live CBCentralManager with a connected
peripheral (i.e. macOS Bluetooth entitlements plus real hardware within
radio range, mid-transaction). These paths get exercised in development use
of the darwin backend, not in CI. Covering them would require an
integration rig, not unit tests. With them excluded from the denominator
the remaining new code sits at 99.6%.

**`linux/device.go` — 4 lines.**
- `38` ("can't create server" wrap): `gatt.NewServerWithNameAndHandler`
  unconditionally returns a nil error; dead branch.
- `45` ("maximum ATT_MTU" guard): guarded by `mtu > ble.MaxMTU` two lines
  after `mtu := ble.MaxMTU`; statically false.
- `73` (ATT-server creation failure inside `loop`): needs a real
  `Accept()`-ed connection whose RxMTU is invalid, but `loop` sets the
  RxMTU itself; effectively dead without hardware.
- `219` (`Dial` success return): needs a real connection-complete event on
  the HCI's unexported master-conn channel; hardware only.

**`linux/hci/hci.go` — 1 line.**
- `197` (`Init` starting the adv dispatcher): `Init` opens the HCI socket
  first and cannot get past that without a device; `advDispatcher` itself
  is fully covered by direct tests. (The former exclusion for `send`'s
  `h.err` done-arm is gone: with `h.err` behind `muErr` the arm is
  race-safely testable and now covered. The former ReadRSSI exclusion in
  conn.go is gone too: the `(int, error)` rework landed with tests.)

**`linux/hci/gap.go` — 1 line.**
- `99` (`sr.Append(adv.ShortName(name))` arm): structurally dead upstream
  code — ShortName appends the full string under the identical length
  check the preceding CompleteName case just failed, so the arm can never
  match. Entered the diff via a rename only; making it reachable would
  mean changing ShortName to truncate, a deliberate upstream behavior
  change tracked separately.

**Not counted at all** (no coverable statements in the diff, or excluded
by policy): `client.go` and `log.go` at the root (interface methods and a
package `var` only), `linux/hci/socket/socket.go` (build-tagged `linux`,
so absent from the darwin-measured profile; its `Close` idempotency and
close-unblocks-Read behavior are covered by `socket_test.go`, which runs
on any linux box via socketpair — no adapter needed — e.g.
`GOOS=linux GOARCH=arm64 go test -c ./linux/hci/socket/` executed on the
Pi), `examples/`, generated `*_gen.go`, and test files themselves.

## Timeouts are variables so their branches stay tested

The bounded waits are this fork's reason to exist, so their timeout
branches are unit-tested by lowering the package-level duration vars
(`hci.ACLWriteTimeout`, `hci.cmdTimeout`, `hci.connCancelTimeout`,
`hci.disconnectTimeout`, `att.seqProtoTimeout`) — never by waiting
wall-clock time. Keep new timeouts in that pattern.

## Bugs found by writing these tests

- `att.Client.ExecuteWrite` panicked on every call (1-byte slice, 2-byte
  PDU) — inherited from upstream, fixed here.
- `att.Client.PrepareWrite` sent stale buffer bytes instead of the part
  value — inherited from upstream, fixed here.
- `hci.CommandReject.Marshal` always failed (`binary.Write` rejects its
  variable-length `Data []byte` field), so the MTU-exceeded Command Reject
  in `handleSignal` was never actually sent. Inherited from upstream,
  fixed here (hand-encoded, with wire tests in linux/hci/signal_test.go).
- `att.Client.Loop` acted on zero-length reads — a header-only L2CAP frame
  arrives as `(0, nil)` — classifying garbage on a stale byte of the
  previous PDU; with a request pending, the resulting empty PDU panicked
  `sendReq`. Runt (< 3 byte) notifications/indications likewise reached
  the gatt dispatcher's unconditional handle parse. Inherited from
  upstream, fixed here.
- `gatt.Client` discovery trusted the peer-controlled entry length byte
  (`DiscoverServices` sliced `b[2:4]`/`b[4:length]` for any length passing
  the divisibility check) and `att.FindInformation` accepted undefined
  format bytes. Both remotely panickable; inherited from upstream, fixed
  here.
- `ble.ATTError(0x11).Error()` ("insufficient resources", the last named
  code) fell through to "unknown error". Inherited; fixed here.
- `darwin` event slots deadlocked cbgo's dispatch-queue thread when a
  waiter abandoned via ctx.Done while a callback was delivering
  (unbuffered send under the slot mutex vs. the waiter's deferred Close).
  Introduced by the ctx work, fixed here (buffered slots, non-blocking
  delivery, closed-slot-means-disconnected receivers).
