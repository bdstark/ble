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
go test -count=1 -coverprofile=/tmp/cover.out -coverpkg=./...,github.com/go-ble/ble/... ./...
python3 tools/diffcover.py . 8c5522f..HEAD /tmp/cover.out
```

## Current state (2026-07-19, measured on macOS)

| Scope | Coverage |
|---|---|
| All added lines | **88.0%** (380/432) |
| Added lines excluding `darwin/` | **98.2%** (380/387) |

Every changed file is at 100% of its added lines except the four below.
The linux backend is pure Go above the socket layer, so all of it —
including `linux/hci` and `linux/att` — builds and tests on any platform.

## Documented exclusions (why 90%+ overall is not attainable)

**`darwin/` — 45 lines, 0% (the entire shortfall).** The changed lines are
`ctx.Done()` arms in selects that otherwise wait on CoreBluetooth delegate
events, plus the context-threaded method signatures around them. The
backend drives `cbgo`, whose peripheral/central types are concrete structs
wrapping Objective-C objects — there is no seam to fake them, and reaching
the changed arms requires a live CBCentralManager with a connected
peripheral (i.e. macOS Bluetooth entitlements plus real hardware within
radio range, mid-transaction). These paths get exercised in development use
of the darwin backend, not in CI. Covering them would require an
integration rig, not unit tests. With all 45 lines excluded from the
denominator the remaining new code sits at 98.2%; with them included the
theoretical maximum for unit tests is 89.6%.

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

**`linux/hci/hci.go` — 3 lines.**
- `162` (`Init` starting the adv dispatcher): `Init` opens the HCI socket
  first and cannot get past that without a device; `advDispatcher` itself
  is fully covered by direct tests.
- `280-281` (`h.err` non-nil arm inside `send`'s done case): `send` already
  checked `h.err == nil` on entry without synchronization, so forcing this
  arm requires an unsynchronized concurrent write to `h.err` that the race
  detector rightly flags; race-unreachable in tests.

**Not counted at all** (no coverable statements in the diff, or excluded
by policy): `client.go` and `log.go` at the root (interface methods and a
package `var` only), `linux/hci/socket/socket.go` (build-tagged
`linux`; its changes are error-message rewording around raw-HCI ioctls
that need `CAP_NET_RAW` and an adapter), `examples/`, generated
`*_gen.go`, and test files themselves.

## Timeouts are variables so their branches stay tested

The bounded waits are this fork's reason to exist, so their timeout
branches are unit-tested by lowering the package-level duration vars
(`hci.ACLWriteTimeout`, `hci.cmdTimeout`, `hci.connCancelTimeout`,
`att.seqProtoTimeout`) — never by waiting wall-clock time. Keep new
timeouts in that pattern.

## Bugs found by writing these tests

- `att.Client.ExecuteWrite` panicked on every call (1-byte slice, 2-byte
  PDU) — inherited from upstream, fixed here.
- `att.Client.PrepareWrite` sent stale buffer bytes instead of the part
  value — inherited from upstream, fixed here.
- `hci.CommandReject.Marshal` always fails (`binary.Write` rejects its
  variable-length `Data []byte` field), so the MTU-exceeded Command Reject
  in `handleSignal` is never actually sent; the error-log path is what's
  covered. Inherited from upstream; not yet fixed.
