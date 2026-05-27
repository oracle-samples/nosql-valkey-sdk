# Valkey External Tests

This directory contains a small Valkey test-suite subset used for compatibility
testing against an already-running external server.

## Source

- Upstream project: https://github.com/valkey-io/valkey
- Upstream commit: d9ba5aba3854a236298a159241d377532aa2c8e7
- License: BSD-3-Clause; see `COPYING`

## Imported Paths

- `runtest`
- `tests/test_helper.tcl`
- `tests/support/*.tcl`
- `tests/unit/type/string.tcl`
- `tests/unit/type/incr.tcl`
- `tests/unit/type/list.tcl`
- `tests/unit/type/list-2.tcl`
- `tests/unit/type/list-3.tcl`
- `tests/unit/type/hash.tcl`
- `tests/unit/type/set.tcl`
- `tests/unit/type/stream.tcl`
- `tests/unit/type/stream-cgroups.tcl`
- `tests/unit/type/zset.tcl`
- `tests/unit/expire.tcl`
- `tests/unit/scan.tcl`
- `tests/unit/keyspace.tcl`

## Local Changes

- The default test list is restricted to the imported unit subset.
- The harness defaults to external server mode on `127.0.0.1:6379`.
- Tests run with a single client and a single selected DB.
- Tags for unsupported external-server behavior are denied by default.
- Encoding checks are ignored by default, matching the previous Redis external
  harness behavior and avoiding `OBJECT ENCODING` requirements.
- External test setup flushes data only. Script/function cleanup and
  `CONFIG RESETSTAT` are disabled so setup does not require `SCRIPT`,
  `FUNCTION`, or broad `CONFIG` support.
- Replication stream helpers are disabled for this external target, avoiding
  `SYNC` while keeping propagation-focused tests callable.
- `memory_usage` returns a fixed positive value so tests do not require the
  `MEMORY USAGE` command.
- Local compatibility tags such as `not-implemented`, `not-supported`, and
  `not-applicable` are applied directly to imported tests and denied by
  default.
- `tests/unit/type/string.tcl` uses a list key, instead of a set key, for the
  `MGET against non-string key` fixture so the test does not require `SADD`.
- `tests/unit/type/string.tcl` uses the smaller Redis payload sizes for the
  `Very big payload` tests so they stay below this external target's maximum
  string size.
- Error-message expectations are aligned with this external target where they
  differ from upstream Valkey wording, including `LMPOP`/`SINTERCARD` numkeys
  and `HINCRBYFLOAT` NaN/Infinity errors.
- `tests/unit/scan.tcl` includes a local syntax fix for the `ZSCAN with
  PATTERN` test wrapper.
