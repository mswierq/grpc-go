# Implementation Plan: Extract `xds/clients` into a Standalone Go Submodule (`google.golang.org/grpc/xds/clients`)

## 1. Summary & Architecture Overview

### Goal
Externalize `internal/xds/clients` (including `clients`, `xdsclient`, and `lrsclient`) into a **single** standalone Go submodule at `xds/clients` (`module google.golang.org/grpc/xds/clients`, `xds/clients/go.mod`) with **zero dependency on `google.golang.org/grpc` (`grpc-go`)**, and provide a standalone example module verifying external consumption without `grpc-go`:

1. **`google.golang.org/grpc/xds/clients`** (`xds/clients/go.mod`):
   - `xds/clients` (`package clients`): Common configuration types (`Node`, `Locality`, `ServerIdentifier`) and extensibility interfaces (`TransportBuilder`, `Transport`, `Stream`, `MetricsReporter`, `AsyncReporter`, `MetricsRecorder`, `Logger`).
   - `xds/clients/xdsclient` (`package xdsclient`): The transport-agnostic xDS ADS client (`XDSClient`, `Config`, `ResourceType`, `Decoder`, `ResourceWatcher`, `ResourceData`, `ErrorType`, `ErrType`, `NewError`, `NewErrorf`), `xds/clients/xdsclient/metrics`, `xds/clients/xdsclient/internal`, and `xds/clients/xdsclient/internal/xdsresource`.
   - `xds/clients/lrsclient` (`package lrsclient`): The transport-agnostic LRS (Load Reporting Service) client (`LRSClient`, `Config`, `LoadStore`, `PerClusterReporter`) and `xds/clients/lrsclient/internal`.
   - `xds/clients/internal`: Shared `grpc`-independent internal utilities (`internal.go`, `backoff`, `buffer`, `envconfig`, `grpclog`, `pretty`, `syncutil`, `testutils`, and `testutils/faketransport`).
   - Unit tests that test internal implementation details or access `xds/clients/{,xdsclient/,lrsclient/}internal/...`, refactored to use `faketransport` (`xds/clients/internal/testutils/faketransport`) and standard `testing.T` functions (without `grpctest`).
2. **`google.golang.org/grpc/xds/clients/examples`** (`xds/clients/examples/go.mod`):
   - A standalone example module with its own `go.mod` requiring `google.golang.org/grpc/xds/clients` (with zero dependency on `google.golang.org/grpc`), demonstrating and verifying `xdsclient` and `lrsclient` usage with a custom `clients.TransportBuilder` and `clients.Logger`.

Packages and e2e tests that remain in the root `google.golang.org/grpc` module (because they depend on `google.golang.org/grpc`, including `internal/grpctest` and `internal/leakcheck`):
- [`internal/xds/clients/grpctransport`](internal/xds/clients/grpctransport) (including `grpc_transport_test.go`, `grpc_transport_ext_test.go`, and `examples_test.go`)
- [`internal/xds/clients/internal/testutils/e2e`](internal/xds/clients/internal/testutils/e2e), [`internal/xds/clients/internal/testutils/fakeserver`](internal/xds/clients/internal/testutils/fakeserver), [`internal/xds/clients/internal/testutils/restartable_listener.go`](internal/xds/clients/internal/testutils/restartable_listener.go), and [`internal/xds/clients/internal/testutils/wrappers.go`](internal/xds/clients/internal/testutils/wrappers.go) (plus `channel.go` and `marshal_any.go` retained for root tests)
- [`internal/xds/clients/xdsclient/test`](internal/xds/clients/xdsclient/test) (e2e tests using `grpctransport`, `e2e.ManagementServer`, `fakeserver`, and `internal/grpctest`, including `xdsclient_test.go` moved from `internal/xds/clients/xdsclient/xdsclient_test.go` as `package xdsclient_test`)
- [`internal/xds/clients/lrsclient/loadreport_test.go`](internal/xds/clients/lrsclient/loadreport_test.go) (`package lrsclient_test`, e2e tests using `grpctransport`, `e2e.ManagementServer`, `fakeserver`, and `internal/grpctest`)

### Architectural Decisions & Requirement Mapping
- **Single Submodule Topology (`xds/clients`)**:
  - Move `internal/xds/clients/{config.go,transport_builder.go}` to `xds/clients/` (`package clients`), `internal/xds/clients/xdsclient` to `xds/clients/xdsclient/`, `internal/xds/clients/lrsclient` to `xds/clients/lrsclient/`, and shared non-gRPC internal utilities to `xds/clients/internal/` under a single `xds/clients/go.mod` (`module google.golang.org/grpc/xds/clients`).
  - Update [`internal/xds/clients/lrsclient/lrs_stream.go`](internal/xds/clients/lrsclient/lrs_stream.go) to import `google.golang.org/grpc/xds/clients/internal/backoff` and `google.golang.org/grpc/xds/clients/internal/pretty` instead of `google.golang.org/grpc/internal/{backoff,pretty}`.
  - Require `google.golang.org/grpc/xds/clients` in the root [`go.mod`](go.mod) with `replace google.golang.org/grpc/xds/clients => ./xds/clients`, and update all callers across `grpc-go` to import `google.golang.org/grpc/xds/clients{,/xdsclient,/xdsclient/metrics,/lrsclient}` directly (no type aliases left in `internal/xds/clients/`).
- **Test Separation (e2e Tests vs. Unit Tests), `faketransport` Usage & `grpctest` Strategy**:
  - **e2e tests** (`package xdsclient_test` and `package lrsclient_test`) that use `grpctransport`, `e2e.ManagementServer`, `fakeserver`, and `internal/grpctest` stay in `internal/xds/clients/xdsclient/test/` and `internal/xds/clients/lrsclient/loadreport_test.go` in the root module.
  - **Unit tests** that test internal implementation details or access `xds/clients/{,xdsclient/,lrsclient/}internal/...` (which Go's `internal` visibility rules forbid importing from `google.golang.org/grpc/internal/...`) move to `xds/clients/` and use `faketransport` (`xds/clients/internal/testutils/faketransport`):
    - `channel_test.go`, `helpers_test.go`, `ads_stream_backoff_test.go`, `ads_stream_watch_test.go`, and `logging_test.go` in `xds/clients/xdsclient/` (`package xdsclient`)
    - `load_store_test.go` and `lrsclient_test.go` (containing `TestReportLoad_LoadReportInterval`, which overrides `lrsclient/internal.TimeNow`, and ORCA-to-LRS propagation unit tests) in `xds/clients/lrsclient/` (`package lrsclient`)
    - `internal_test.go`, `buffer/unbounded_test.go`, `grpclog/prefix_logger_test.go`, `syncutil/{callback_serializer_test.go,event_test.go}`, `testutils/faketransport/xds_fake_transport_test.go` in `xds/clients/internal/`
  - **Resigning from `grpctest` in `xds/clients` Unit Tests**:
    - We intentionally resign from `grpctest` in the `xds/clients` unit tests (rather than duplicating a stripped-down `grpctest` package inside `xds/clients/internal/`) and convert those unit tests to standard `func TestXxx(t *testing.T)` functions.
    - Because `internal/grpctest` (which integrates `internal/leakcheck` goroutine and buffer-pool leak detection and `grpclog` test logging) lives in the root `google.golang.org/grpc` module, we rely on the **e2e tests** (`internal/xds/clients/xdsclient/test/*_test.go`, `internal/xds/clients/lrsclient/loadreport_test.go`, and `internal/xds/clients/grpctransport/*_test.go`) for full `grpctest` leak detection and lifecycle verification. This is also a key reason why the e2e tests are kept internal in the root module (`internal/xds/clients/`).
  - Export `ErrorType` (`ErrorTypeUnknown`, `ErrorTypeConnection`, `ErrorTypeResourceNotFound`, `ErrorTypeResourceTypeUnsupported`, `ErrTypeStreamFailedAfterRecv`, `ErrorTypeNACKed`), `ErrType(err error) ErrorType`, `NewError`, and `NewErrorf` in `xdsclient` so e2e tests in `internal/xds/clients/xdsclient/test/` and `grpc-go` callers can inspect watcher errors without importing `xds/clients/xdsclient/internal/xdsresource`.
- **Decoupling `envconfig` from `grpc-go`**:
  - Define both `XDSRecoverPanicInResourceParsing` (`GRPC_GO_EXPERIMENTAL_XDS_RESOURCE_PANIC_RECOVERY`, used by `xdsclient`) and `XDSORCAToLRSPropEnabled` (`GRPC_EXPERIMENTAL_XDS_ORCA_LRS_PROPAGATION`, used by `lrsclient`) in `xds/clients/internal/envconfig/envconfig.go`.
  - Remove `XDSRecoverPanicInResourceParsing` from root [`internal/envconfig/envconfig.go`](internal/envconfig/envconfig.go#L114), while keeping `XDSORCAToLRSPropEnabled` in root [`internal/envconfig/xds.go`](internal/envconfig/xds.go#L90) for root packages (`internal/xds/balancer/clusterimpl` and `internal/xds/xdsclient/xdsresource`).
- **Decoupling `grpclog` from `grpc-go`**:
  - Define `clients.Logger` interface (`InfoDepth`, `WarningDepth`, `ErrorDepth`, `V(l int) bool`) in `xds/clients/config.go` and add `Logger clients.Logger` to both `xdsclient.Config` and `lrsclient.Config`.
  - Provide a `grpc`-free `PrefixLogger` in `xds/clients/internal/grpclog/prefix_logger.go` where, when `Config.Logger == nil`, `PrefixLogger` methods (`Infof`, `Warningf`, `Errorf`, `Debugf`) are safe no-ops and `V(l)` returns `false`.
  - Wire both `xdsclient` (`xdsclient.go`, `logging.go`, `authority.go`, `channel.go`, `ads_stream.go`, `clientimpl_watchers.go`) and `lrsclient` (`lrsclient.go`, `logging.go`, `lrs_stream.go`) to use `Config.Logger`.
  - Update [`internal/xds/xdsclient/clientimpl.go`](internal/xds/xdsclient/clientimpl.go) in `grpc-go` to pass `Logger: logger` (`grpclog.Component("xds")`) in both `xdsclient.Config` and `lrsclient.Config`.

---

## 2. PR Task Split Table

| Task ID | Depends On | Story Points (Fibonacci) | Summary |
| :--- | :--- | :---: | :--- |
| **`t1`** | None | **5** | `xds/clients: decouple xdsclient and lrsclient from grpc-go and refactor unit tests to faketransport` |
| **`t2`** | `t1` | **5** | `xds/clients: extract google.golang.org/grpc/xds/clients submodule` |
| **`t3`** | `t2` | **2** | `xds/clients/examples: add standalone xds/clients example module and CI check` |

---

## 3. Detailed Per-PR Task Cards

### Task `t1`: Decouple `internal/xds/clients` (`xdsclient` & `lrsclient`) from `google.golang.org/grpc` & Refactor Unit Tests to `faketransport`

- **Task ID**: `t1`
- **Depends On**: None
- **Story Point Score (Fibonacci)**: **5** (~2.5–3.5 days of work)
- **Summary**: `xds/clients: decouple xdsclient and lrsclient from grpc-go and refactor unit tests to faketransport`
- **Release Notes**: `RELEASE NOTES: N/A` (all changes are in `internal/` in this PR).

#### Description
Before moving files or introducing `xds/clients/go.mod`, eliminate all imports of `google.golang.org/grpc` (`grpclog`, `internal/grpclog`, `internal/grpctest`, `internal/envconfig`, `internal/backoff`, `internal/pretty`, `internal/testutils`, `credentials/insecure`, `grpctransport`, `e2e`, `fakeserver`) from `internal/xds/clients/xdsclient` (excluding `test/`), `internal/xds/clients/lrsclient` (excluding `loadreport_test.go`), and the non-`grpc` packages in `internal/xds/clients/internal/`:

1. **Logger Interface & `PrefixLogger` for `xdsclient` and `lrsclient`**:
   - In [`internal/xds/clients/config.go`](internal/xds/clients/config.go), add:
     ```go
     // Logger is the logging interface used by xDS and LRS clients. It is
     // satisfied by grpclog.DepthLoggerV2 (for example, grpclog.Component("xds")).
     type Logger interface {
         InfoDepth(depth int, args ...any)
         WarningDepth(depth int, args ...any)
         ErrorDepth(depth int, args ...any)
         V(l int) bool
     }
     ```
   - Add `Logger clients.Logger` to `xdsclient.Config` in [`internal/xds/clients/xdsclient/xdsconfig.go`](internal/xds/clients/xdsclient/xdsconfig.go) and to `lrsclient.Config` in [`internal/xds/clients/lrsclient/lrsconfig.go`](internal/xds/clients/lrsclient/lrsconfig.go).
   - Create `internal/xds/clients/internal/grpclog/prefix_logger.go` (and `prefix_logger_test.go`) implementing `PrefixLogger` (`NewPrefixLogger(logger clients.Logger, prefix string) *PrefixLogger`, `Infof`, `Warningf`, `Errorf`, `Debugf`, `V`) with zero imports of `google.golang.org/grpc`. When `pl == nil` or `pl.logger == nil`, `Infof`, `Warningf`, `Errorf`, and `Debugf` are safe no-ops and `V(l)` returns `false`.
   - Update `xdsclient` ([`logging.go`](internal/xds/clients/xdsclient/logging.go), [`xdsclient.go`](internal/xds/clients/xdsclient/xdsclient.go), [`authority.go`](internal/xds/clients/xdsclient/authority.go), [`channel.go`](internal/xds/clients/xdsclient/channel.go), [`ads_stream.go`](internal/xds/clients/xdsclient/ads_stream.go), and [`clientimpl_watchers.go`](internal/xds/clients/xdsclient/clientimpl_watchers.go)) to import `internal/xds/clients/internal/grpclog` and thread `config.Logger` into `prefixLogger(...)`, replacing package-level `logger.Warningf` calls in `clientimpl_watchers.go` with `c.logger.Warningf`. Add `internal/xds/clients/xdsclient/logging_test.go`.
   - Update `lrsclient` ([`logging.go`](internal/xds/clients/lrsclient/logging.go), [`lrsclient.go`](internal/xds/clients/lrsclient/lrsclient.go), and [`lrs_stream.go`](internal/xds/clients/lrsclient/lrs_stream.go)) to import `internal/xds/clients/internal/grpclog` and thread `config.Logger` into `prefixLogger(config.Logger, c)` and `streamOpts.logger` (removing `"google.golang.org/grpc/grpclog"` and `"google.golang.org/grpc/internal/grpclog"`).
   - Update [`internal/xds/xdsclient/clientimpl.go`](internal/xds/xdsclient/clientimpl.go) to pass `Logger: logger` (`grpclog.Component("xds")`) in both `xdsclient.Config` (in `BuildXDSClientConfig`) and `lrsclient.Config` (in `newClientImpl`), and update [`internal/xds/xdsclient/clientimpl_test.go`](internal/xds/xdsclient/clientimpl_test.go).

2. **Test Harness (`internal/grpctest`) Decoupling in Unit Tests**:
   - Resign from using `grpctest` in the unit tests that will move into the `xds/clients` submodule (do **not** create `xds/clients/internal/grpctest`). Instead, rely on the **e2e tests** (`internal/xds/clients/xdsclient/test/*_test.go`, `internal/xds/clients/lrsclient/loadreport_test.go`, and `internal/xds/clients/grpctransport/*_test.go`) for `grpctest` (`leakcheck` goroutine/buffer-pool leak detection and `grpclog` test integration), which is a key reason why the e2e tests remain internal in the root module (`internal/xds/clients/`).
   - Remove `"google.golang.org/grpc/internal/grpctest"`, `type s struct { grpctest.Tester }`, `func Test(t *testing.T) { grpctest.RunSubTests(t, s{}) }`, and `(s)` receiver prefixes from [`internal/xds/clients/internal/buffer/unbounded_test.go`](internal/xds/clients/internal/buffer/unbounded_test.go), [`internal/xds/clients/internal/internal_test.go`](internal/xds/clients/internal/internal_test.go), [`internal/xds/clients/internal/syncutil/callback_serializer_test.go`](internal/xds/clients/internal/syncutil/callback_serializer_test.go), [`internal/xds/clients/internal/syncutil/event_test.go`](internal/xds/clients/internal/syncutil/event_test.go), [`internal/xds/clients/xdsclient/helpers_test.go`](internal/xds/clients/xdsclient/helpers_test.go), [`internal/xds/clients/xdsclient/channel_test.go`](internal/xds/clients/xdsclient/channel_test.go), and the unit tests moved into `internal/xds/clients/xdsclient/` (`ads_stream_backoff_test.go`, `ads_stream_watch_test.go`) and `internal/xds/clients/lrsclient/` (`lrsclient_test.go`), using standard `func TestXxx(t *testing.T)` test functions.

3. **Environment Config (`XDSRecoverPanicInResourceParsing` & `XDSORCAToLRSPropEnabled`) and `lrs_stream.go` Imports Decoupling**:
   - Create `internal/xds/clients/internal/envconfig/envconfig.go` defining:
     - `XDSRecoverPanicInResourceParsing = boolFromEnv("GRPC_GO_EXPERIMENTAL_XDS_RESOURCE_PANIC_RECOVERY", true)`
     - `XDSORCAToLRSPropEnabled = boolFromEnv("GRPC_EXPERIMENTAL_XDS_ORCA_LRS_PROPAGATION", true)`
   - Remove `XDSRecoverPanicInResourceParsing` from [`internal/envconfig/envconfig.go`](internal/envconfig/envconfig.go#L114) (keep `XDSORCAToLRSPropEnabled` in [`internal/envconfig/xds.go`](internal/envconfig/xds.go#L90) for root packages `clusterimpl` and `xdsresource`).
   - Create `internal/xds/clients/internal/testutils/envconfig.go` defining `SetEnvConfig[T any](t *testing.T, envVar *T, val T)`.
   - Update [`internal/xds/clients/xdsclient/channel.go`](internal/xds/clients/xdsclient/channel.go) and [`internal/xds/clients/xdsclient/channel_test.go`](internal/xds/clients/xdsclient/channel_test.go) to import `"google.golang.org/grpc/internal/xds/clients/internal/envconfig"` and use `xdstestutils.SetEnvConfig`.
   - Update [`internal/xds/clients/lrsclient/lrs_stream.go`](internal/xds/clients/lrsclient/lrs_stream.go) to import `"google.golang.org/grpc/internal/xds/clients/internal/backoff"`, `"google.golang.org/grpc/internal/xds/clients/internal/envconfig"`, and `"google.golang.org/grpc/internal/xds/clients/internal/pretty"` instead of `google.golang.org/grpc/internal/{backoff,envconfig,pretty}`.

4. **Export `ErrorType`, `ErrType`, `NewError`, and `NewErrorf` in `xdsclient`**:
   - Create `internal/xds/clients/xdsclient/errors.go` exporting `ErrorType` (`ErrorTypeUnknown`, `ErrorTypeConnection`, `ErrorTypeResourceNotFound`, `ErrorTypeResourceTypeUnsupported`, `ErrTypeStreamFailedAfterRecv`, `ErrorTypeNACKed`), `ErrType(err error) ErrorType`, `NewError`, and `NewErrorf` (and delegate/alias in [`internal/xds/clients/xdsclient/internal/xdsresource/errors.go`](internal/xds/clients/xdsclient/internal/xdsresource/errors.go)).
   - Update `internal/xds/clients/xdsclient/test/{ads_stream_ack_nack_test.go,ads_stream_flow_control_test.go,ads_stream_restart_test.go,authority_test.go,dump_test.go,helpers_test.go,lds_watchers_test.go,metrics_test.go,misc_watchers_test.go}` to use `xdsclient.ErrType`, `xdsclient.ErrorType*`, `xdsclient.NewError`, and root-module `internal/xds/xdsclient/xdsresource` so `internal/xds/clients/xdsclient/test/` has zero imports of `internal/xds/clients/xdsclient/internal/...`.

5. **Refactor Unit Tests in `xdsclient` and `lrsclient` to `faketransport`**:
   - Enhance [`internal/xds/clients/internal/testutils/faketransport/xds_fake_transport.go`](internal/xds/clients/internal/testutils/faketransport/xds_fake_transport.go) (and add `xds_fake_transport_test.go`) to support unbounded request buffering, server-side stream error injection (`CloseWithError`, `SetNewStreamError`, `NextStream`), and LRS request/response helpers (`RecvLRS` / `SendLRS`).
   - Refactor [`internal/xds/clients/xdsclient/channel_test.go`](internal/xds/clients/xdsclient/channel_test.go) (`package xdsclient`) to use `faketransport` instead of `grpctransport`, `credentials/insecure`, `fakeserver`, and `e2e`.
   - Move [`internal/xds/clients/xdsclient/test/ads_stream_backoff_test.go`](internal/xds/clients/xdsclient/test/ads_stream_backoff_test.go) and [`internal/xds/clients/xdsclient/test/ads_stream_watch_test.go`](internal/xds/clients/xdsclient/test/ads_stream_watch_test.go) into `internal/xds/clients/xdsclient/` (`package xdsclient`) and refactor them to use `faketransport`.
   - Move [`internal/xds/clients/xdsclient/xdsclient_test.go`](internal/xds/clients/xdsclient/xdsclient_test.go) to `internal/xds/clients/xdsclient/test/xdsclient_test.go` (`package xdsclient_test`).
   - Move `TestReportLoad_LoadReportInterval` (which accesses `lrsclient/internal.TimeNow`) from [`internal/xds/clients/lrsclient/loadreport_test.go`](internal/xds/clients/lrsclient/loadreport_test.go#L635-L730) into `internal/xds/clients/lrsclient/lrsclient_test.go` (`package lrsclient`) refactored to use `faketransport`, and remove the `lrsclientinternal` and `envconfig` imports from `internal/xds/clients/lrsclient/loadreport_test.go`.

#### File-by-File Changes
- **Modified**:
  - [`internal/envconfig/envconfig.go`](internal/envconfig/envconfig.go)
  - [`internal/xds/clients/config.go`](internal/xds/clients/config.go)
  - [`internal/xds/clients/internal/buffer/unbounded_test.go`](internal/xds/clients/internal/buffer/unbounded_test.go)
  - [`internal/xds/clients/internal/internal_test.go`](internal/xds/clients/internal/internal_test.go)
  - [`internal/xds/clients/internal/syncutil/callback_serializer_test.go`](internal/xds/clients/internal/syncutil/callback_serializer_test.go)
  - [`internal/xds/clients/internal/syncutil/event_test.go`](internal/xds/clients/internal/syncutil/event_test.go)
  - [`internal/xds/clients/internal/testutils/faketransport/xds_fake_transport.go`](internal/xds/clients/internal/testutils/faketransport/xds_fake_transport.go)
  - [`internal/xds/clients/lrsclient/loadreport_test.go`](internal/xds/clients/lrsclient/loadreport_test.go)
  - [`internal/xds/clients/lrsclient/logging.go`](internal/xds/clients/lrsclient/logging.go)
  - [`internal/xds/clients/lrsclient/lrs_stream.go`](internal/xds/clients/lrsclient/lrs_stream.go)
  - [`internal/xds/clients/lrsclient/lrsclient.go`](internal/xds/clients/lrsclient/lrsclient.go)
  - [`internal/xds/clients/lrsclient/lrsconfig.go`](internal/xds/clients/lrsclient/lrsconfig.go)
  - [`internal/xds/clients/xdsclient/ads_stream.go`](internal/xds/clients/xdsclient/ads_stream.go)
  - [`internal/xds/clients/xdsclient/authority.go`](internal/xds/clients/xdsclient/authority.go)
  - [`internal/xds/clients/xdsclient/channel.go`](internal/xds/clients/xdsclient/channel.go)
  - [`internal/xds/clients/xdsclient/channel_test.go`](internal/xds/clients/xdsclient/channel_test.go)
  - [`internal/xds/clients/xdsclient/clientimpl_watchers.go`](internal/xds/clients/xdsclient/clientimpl_watchers.go)
  - [`internal/xds/clients/xdsclient/helpers_test.go`](internal/xds/clients/xdsclient/helpers_test.go)
  - [`internal/xds/clients/xdsclient/logging.go`](internal/xds/clients/xdsclient/logging.go)
  - [`internal/xds/clients/xdsclient/xdsclient.go`](internal/xds/clients/xdsclient/xdsclient.go)
  - [`internal/xds/clients/xdsclient/xdsconfig.go`](internal/xds/clients/xdsclient/xdsconfig.go)
  - [`internal/xds/clients/xdsclient/test/ads_stream_ack_nack_test.go`](internal/xds/clients/xdsclient/test/ads_stream_ack_nack_test.go)
  - [`internal/xds/clients/xdsclient/test/ads_stream_flow_control_test.go`](internal/xds/clients/xdsclient/test/ads_stream_flow_control_test.go)
  - [`internal/xds/clients/xdsclient/test/ads_stream_restart_test.go`](internal/xds/clients/xdsclient/test/ads_stream_restart_test.go)
  - [`internal/xds/clients/xdsclient/test/authority_test.go`](internal/xds/clients/xdsclient/test/authority_test.go)
  - [`internal/xds/clients/xdsclient/test/dump_test.go`](internal/xds/clients/xdsclient/test/dump_test.go)
  - [`internal/xds/clients/xdsclient/test/helpers_test.go`](internal/xds/clients/xdsclient/test/helpers_test.go)
  - [`internal/xds/clients/xdsclient/test/lds_watchers_test.go`](internal/xds/clients/xdsclient/test/lds_watchers_test.go)
  - [`internal/xds/clients/xdsclient/test/metrics_test.go`](internal/xds/clients/xdsclient/test/metrics_test.go)
  - [`internal/xds/clients/xdsclient/test/misc_watchers_test.go`](internal/xds/clients/xdsclient/test/misc_watchers_test.go)
  - [`internal/xds/xdsclient/clientimpl.go`](internal/xds/xdsclient/clientimpl.go)
  - [`internal/xds/xdsclient/clientimpl_test.go`](internal/xds/xdsclient/clientimpl_test.go)
- **Added / Moved**:
  - `internal/xds/clients/internal/envconfig/envconfig.go`
  - `internal/xds/clients/internal/grpclog/prefix_logger.go`, `internal/xds/clients/internal/grpclog/prefix_logger_test.go`
  - `internal/xds/clients/internal/testutils/envconfig.go`
  - `internal/xds/clients/internal/testutils/faketransport/xds_fake_transport_test.go`
  - `internal/xds/clients/lrsclient/lrsclient_test.go` (containing `TestReportLoad_LoadReportInterval` and LRS unit tests using `faketransport`)
  - `internal/xds/clients/xdsclient/errors.go`
  - `internal/xds/clients/xdsclient/logging_test.go`
  - `internal/xds/clients/xdsclient/ads_stream_backoff_test.go` (moved & refactored from `internal/xds/clients/xdsclient/test/ads_stream_backoff_test.go`)
  - `internal/xds/clients/xdsclient/ads_stream_watch_test.go` (moved & refactored from `internal/xds/clients/xdsclient/test/ads_stream_watch_test.go`)
  - `internal/xds/clients/xdsclient/test/xdsclient_test.go` (moved from `internal/xds/clients/xdsclient/xdsclient_test.go`)

#### Test Plan
- `go test -race -cpu 1,4 -timeout 7m ./internal/xds/clients/... ./internal/xds/xdsclient/...`
- `./scripts/vet.sh`

---

### Task `t2`: Extract `google.golang.org/grpc/xds/clients` Submodule (`xds/clients/go.mod`)

- **Task ID**: `t2`
- **Depends On**: `t1`
- **Story Point Score (Fibonacci)**: **5** (~2.5–3.5 days of work)
- **Summary**: `xds/clients: extract google.golang.org/grpc/xds/clients submodule`
- **Release Notes**:
  ```
  RELEASE NOTES:
  * xds/clients: introduce `google.golang.org/grpc/xds/clients` submodule providing standalone, transport-agnostic xDS (`xdsclient`) and LRS (`lrsclient`) client implementations.
  ```

#### Description
Extract `clients`, `xdsclient`, `lrsclient`, and shared non-gRPC `internal` utilities into a single standalone Go submodule at `xds/clients` (`module google.golang.org/grpc/xds/clients`, `xds/clients/go.mod`), and update all callers across `grpc-go`:

1. **Create `xds/clients/go.mod` & `xds/clients/go.sum`**:
   - Module path: `google.golang.org/grpc/xds/clients` (`go 1.25.0`).
   - Direct dependencies: `github.com/envoyproxy/go-control-plane/envoy`, `github.com/google/go-cmp`, `google.golang.org/protobuf` (plus transitive protobuf dependencies; **zero** dependency on `google.golang.org/grpc`).

2. **Move `clients`, `xdsclient`, `lrsclient`, and Shared `internal` Packages to `xds/clients/`**:
   - Move [`internal/xds/clients/config.go`](internal/xds/clients/config.go) and [`internal/xds/clients/transport_builder.go`](internal/xds/clients/transport_builder.go) to `xds/clients/` (remove originals from `internal/xds/clients/`).
   - Move `internal/xds/clients/xdsclient/` (`ads_stream.go`, `ads_stream_backoff_test.go`, `ads_stream_watch_test.go`, `authority.go`, `channel.go`, `channel_test.go`, `clientimpl_watchers.go`, `errors.go`, `helpers_test.go`, `logging.go`, `logging_test.go`, `resource_type.go`, `resource_watcher.go`, `xdsclient.go`, `xdsconfig.go`, `metrics/metrics.go`, `internal/internal.go`, and `internal/xdsresource/{ads_stream.go,errors.go,name.go,type.go,version.go}`) to `xds/clients/xdsclient/` (leaving `internal/xds/clients/xdsclient/test/` in the root module).
   - Move `internal/xds/clients/lrsclient/` (`load_store.go`, `load_store_test.go`, `logging.go`, `lrs_stream.go`, `lrsclient.go`, `lrsclient_test.go`, `lrsconfig.go`, and `internal/internal.go`) to `xds/clients/lrsclient/` (leaving `internal/xds/clients/lrsclient/loadreport_test.go` in the root module).
   - Move shared non-gRPC internal packages from `internal/xds/clients/internal/` to `xds/clients/internal/`:
     - `internal.go`, `internal_test.go` -> `xds/clients/internal/`
     - `backoff/backoff.go` -> `xds/clients/internal/backoff/backoff.go`
     - `buffer/{unbounded.go,unbounded_test.go}` -> `xds/clients/internal/buffer/`
     - `envconfig/envconfig.go` -> `xds/clients/internal/envconfig/envconfig.go`
     - `grpclog/{prefix_logger.go,prefix_logger_test.go}` -> `xds/clients/internal/grpclog/`
     - `pretty/pretty.go` -> `xds/clients/internal/pretty/pretty.go`
     - `syncutil/{callback_serializer.go,callback_serializer_test.go,event.go,event_test.go}` -> `xds/clients/internal/syncutil/`
     - `testutils/envconfig.go` and `testutils/faketransport/{xds_fake_transport.go,xds_fake_transport_test.go}` -> `xds/clients/internal/testutils/`
     - Copy `internal/xds/clients/internal/testutils/{channel.go,marshal_any.go}` to `xds/clients/internal/testutils/` (keeping the originals in `internal/xds/clients/internal/testutils/` alongside `wrappers.go`, `restartable_listener.go`, `e2e/`, and `fakeserver/` for root module e2e tests).
   - Update all internal imports inside `xds/clients/...` from `"google.golang.org/grpc/internal/xds/clients/..."` to `"google.golang.org/grpc/xds/clients/..."`.

3. **Update Root `go.mod` & All Callers in `grpc-go`**:
   - Add `google.golang.org/grpc/xds/clients v0.0.0` and `replace google.golang.org/grpc/xds/clients => ./xds/clients` to the root [`go.mod`](go.mod).
   - Update all imports of `google.golang.org/grpc/internal/xds/clients`, `google.golang.org/grpc/internal/xds/clients/xdsclient`, `google.golang.org/grpc/internal/xds/clients/xdsclient/metrics`, and `google.golang.org/grpc/internal/xds/clients/lrsclient` across `internal/xds/...`, `xds/...`, `internal/xds/clients/grpctransport/...`, `internal/xds/clients/xdsclient/test/...`, and `internal/xds/clients/lrsclient/loadreport_test.go` to `google.golang.org/grpc/xds/clients{,/xdsclient,/xdsclient/metrics,/lrsclient}`.
   - Update `internal/xds/clients/xdsclient/test/*_test.go` to import `"google.golang.org/grpc/internal/buffer"`, `"google.golang.org/grpc/internal/pretty"`, and `"google.golang.org/grpc/internal/grpcsync"` instead of `internal/xds/clients/internal/{buffer,pretty,syncutil}`.

#### File-by-File Changes
- **Added / Moved to `xds/clients/`**:
  - `xds/clients/go.mod`, `xds/clients/go.sum`
  - `xds/clients/{config.go,transport_builder.go}`
  - `xds/clients/xdsclient/{ads_stream.go,ads_stream_backoff_test.go,ads_stream_watch_test.go,authority.go,channel.go,channel_test.go,clientimpl_watchers.go,errors.go,helpers_test.go,logging.go,logging_test.go,resource_type.go,resource_watcher.go,xdsclient.go,xdsconfig.go}`
  - `xds/clients/xdsclient/metrics/metrics.go`
  - `xds/clients/xdsclient/internal/internal.go`
  - `xds/clients/xdsclient/internal/xdsresource/{ads_stream.go,errors.go,name.go,type.go,version.go}`
  - `xds/clients/lrsclient/{load_store.go,load_store_test.go,logging.go,lrs_stream.go,lrsclient.go,lrsclient_test.go,lrsconfig.go}`
  - `xds/clients/lrsclient/internal/internal.go`
  - `xds/clients/internal/{internal.go,internal_test.go}`
  - `xds/clients/internal/backoff/backoff.go`
  - `xds/clients/internal/buffer/{unbounded.go,unbounded_test.go}`
  - `xds/clients/internal/envconfig/envconfig.go`
  - `xds/clients/internal/grpclog/{prefix_logger.go,prefix_logger_test.go}`
  - `xds/clients/internal/pretty/pretty.go`
  - `xds/clients/internal/syncutil/{callback_serializer.go,callback_serializer_test.go,event.go,event_test.go}`
  - `xds/clients/internal/testutils/{channel.go,envconfig.go,marshal_any.go}`
  - `xds/clients/internal/testutils/faketransport/{xds_fake_transport.go,xds_fake_transport_test.go}`
- **Deleted from `internal/xds/clients/`**:
  - All moved files in `internal/xds/clients/`, `internal/xds/clients/xdsclient/`, `internal/xds/clients/lrsclient/`, and `internal/xds/clients/internal/`.
- **Modified in Root Module**:
  - [`go.mod`](go.mod), [`go.sum`](go.sum)
  - All files across `internal/xds/...` and `xds/...` updated to import `google.golang.org/grpc/xds/clients{,/xdsclient,/xdsclient/metrics,/lrsclient}`.

#### Test Plan
- In `xds/clients`: `go test -race -cpu 1,4 -timeout 7m ./...`
- Verify zero `google.golang.org/grpc` dependency in `xds/clients`: `cd xds/clients && go list -m all` (must not contain `google.golang.org/grpc`).
- In root module: `go test -race -cpu 1,4 -timeout 7m ./internal/xds/... ./xds/...`
- `./scripts/vet.sh` and `./scripts/gen-deps.sh`

---

### Task `t3`: Add Standalone `xds/clients` Example Module & CI Verification (`xds/clients/examples`)

- **Task ID**: `t3`
- **Depends On**: `t2`
- **Story Point Score (Fibonacci)**: **2** (~0.5–1 day of work)
- **Summary**: `xds/clients/examples: add standalone xds/clients example module and CI check`
- **Release Notes**: `RELEASE NOTES: N/A`

#### Description
Create a standalone example module at `xds/clients/examples/` with its own `go.mod` requiring `google.golang.org/grpc/xds/clients` (and **not** `google.golang.org/grpc`) to demonstrate and verify in CI that an external consumer can import and run `google.golang.org/grpc/xds/clients`, `google.golang.org/grpc/xds/clients/xdsclient`, and `google.golang.org/grpc/xds/clients/lrsclient` without `grpc-go` in its dependency graph:

1. **Create `xds/clients/examples/go.mod` & `go.sum`**:
   - Module path: `google.golang.org/grpc/xds/clients/examples` (`go 1.25.0`).
   - Require `google.golang.org/grpc/xds/clients v0.0.0`, `github.com/envoyproxy/go-control-plane/envoy`, `google.golang.org/protobuf`.
   - Local `replace` directive: `replace google.golang.org/grpc/xds/clients => ..`.
   - Must have **zero** dependency on `google.golang.org/grpc` in `go.mod` and `go.sum`.
2. **Implement Standalone Example & Test (`xds/clients/examples/`)**:
   - Provide a runnable example and test (`main.go` and/or `example_test.go`) demonstrating:
     - Implementing `clients.TransportBuilder`, `clients.Transport`, and `clients.Stream` without `grpc-go`.
     - Implementing `clients.Logger`.
     - Using `xdsclient.New(cfg)` and `WatchResource` with custom `xdsclient.Decoder`, `xdsclient.ResourceData`, and `xdsclient.ResourceWatcher`.
     - Using `lrsclient.New(cfg)` and `ReportLoad(si)` to report cluster loads over the custom transport.
3. **CI / Script Integration**:
   - Because [`scripts/common.sh`](scripts/common.sh) discovers all `go.mod` files via `find . -name "go.mod"`, both `xds/clients/go.mod` and `xds/clients/examples/go.mod` are automatically included in `./scripts/vet.sh`, `./scripts/gen-deps.sh`, and unit test runs in [`.github/workflows/testing.yml`](.github/workflows/testing.yml).
   - If the example includes a `main.go` binary or `examples_test.sh`, wire it into [`.github/workflows/testing.yml`](.github/workflows/testing.yml#L69-L71).

#### File-by-File Changes
- **Added**:
  - `xds/clients/examples/go.mod`, `xds/clients/examples/go.sum`
  - `xds/clients/examples/main.go` (and/or `xds/clients/examples/example_test.go`, `xds/clients/examples/examples_test.sh`)
- **Modified (if `examples_test.sh` is used)**:
  - [`.github/workflows/testing.yml`](.github/workflows/testing.yml)

#### Test Plan
- In `xds/clients/examples`: `go test -race -v ./...` and `go list -m all` (confirming `google.golang.org/grpc` is absent).
- `./scripts/vet.sh` and `./scripts/gen-deps.sh`

---

## 4. Concurrency, Lifecycle & Vet/CI Analysis

1. **Logger Concurrency & Nil-Safety (`PrefixLogger`)**:
   - `PrefixLogger` is immutable after construction (`NewPrefixLogger(logger, prefix)`) and safe for concurrent use across `XDSClient`, `authority`, `xdsChannel`, `adsStreamImpl`, `LRSClient`, and `lrsclient.streamImpl` goroutines.
   - In [`internal/xds/clients/xdsclient/clientimpl_watchers.go`](internal/xds/clients/xdsclient/clientimpl_watchers.go#L56-L77), lines 56, 67, and 77 previously used the package-level `logger.Warningf` instead of `c.logger.Warningf`; changing these to `c.logger.Warningf` ensures all logs honor the per-client `Config.Logger` and prefix.
   - Similarly, in [`internal/xds/clients/lrsclient/lrs_stream.go`](internal/xds/clients/lrsclient/lrs_stream.go#L92-L93), `newStreamImpl` previously called `grpclog.Component("xds")` directly; threading `opts.logger` from `LRSClient` ensures both `LRSClient` and `streamImpl` honor `lrsclient.Config.Logger`.
   - When `Config.Logger == nil`, `PrefixLogger` checks `if pl == nil || pl.logger == nil` and safely no-ops (`V(l)` returns `false`).
2. **`scripts/vet.sh` and `scripts/gen-deps.sh` Compatibility**:
   - [`scripts/common.sh`](scripts/common.sh) dynamically discovers all `go.mod` files via `find . -name "go.mod"`, so `xds/clients/go.mod` and `xds/clients/examples/go.mod` will automatically be checked by `./scripts/vet.sh` (`go vet`, `gofmt`, `goimports`, `go mod tidy -compat=1.26`, `revive`, `staticcheck`), `.github/workflows/testing.yml`, `.github/workflows/deps.yml` (`scripts/gen-deps.sh`), and `.github/workflows/govulncheck.yaml`.
   - [`scripts/vet.sh`](scripts/vet.sh#L58) flags `grpclog.I`, `grpclog.W`, `grpclog.E`, `grpclog.F`, `grpclog.V`. Because `xds/clients/internal/grpclog/prefix_logger.go` invokes methods on `pl.logger` (e.g., `pl.logger.InfoDepth(...)`) rather than package-level `grpclog.*`, it passes this check without requiring an exclusion in `scripts/vet.sh`.
