# ble

**ble** is a Golang [Bluetooth Low Energy](https://en.wikipedia.org/wiki/Bluetooth_Low_Energy) package for Linux and Mac OS.

This is a maintained fork of the dormant [go-ble/ble](https://github.com/go-ble/ble)
(merge base `8c5522f`). It diverges deliberately and will not be upstreamed:
every indefinite channel wait in the HCI/ATT stack is bounded, the
`ble.Client` request API threads `context.Context`, errors are typed
sentinels wrapped for `errors.Is`, logging is `log/slog`, the advertising
scan path runs through a bounded dispatcher with pooled buffers, and a
long tail of inherited server/client protocol bugs (several remotely
triggerable panics among them) is fixed. Test coverage of the fork's
changes is tracked in [COVERAGE.md](COVERAGE.md). Examples live in
[bdstark/ble-examples](https://github.com/bdstark/ble-examples).

**Note:** The Mac OS portion (CoreBluetooth via cbgo) is a development
backend; Linux is the production target.
