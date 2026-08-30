# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Upgraded to gRPC 1.0.** `grpc_connection_pool ~> 0.5.2`, which brings
  `grpc 1.0.4` (client/server split into separate packages) and `grpc_core
  1.0.4`. Huginn's application module no longer starts `GRPC.Client.Supervisor`:
  as of grpc 1.0 the `:grpc` application supervises client connections itself,
  and the old child spec referenced a module that no longer exists, so the
  application could not boot.
- **Regenerated the ClickHouse protobuf bindings** from ClickHouse `master`
  with `protoc-gen-elixir` 0.17.0. `LogsLevel` gains `LOG_TEST` (mapped to
  `:test`), and the generated file now lives at
  `lib/huginn/proto/clickhouse/grpc/clickhouse_grpc.pb.ex`.
- **`:protobuf` and `:jason` are now declared dependencies.** grpc 1.0 no
  longer depends on `:protobuf`, and `:jason` was only ever reached
  transitively despite being used for `JSONEachRow`.
- **`ssl: true` now verifies the peer certificate** against the system CA
  store, and `:ssl` accepts a keyword list for a private CA or client
  certificates. It previously sent `ssl: []`, which overrode the pool's own
  verifying default and left `verify` unset — `verify_none` on OTP < 26.

### Fixed

- **`stream_query/2` silently dropped most rows of a large result.** ClickHouse
  sets `output_format`/`output_columns` only on the *first* `Result` of a
  stream; every later chunk was therefore parsed with an unknown format and
  collapsed into a single opaque row. A 200,000-row query returned 65,412 rows.
  The format and columns from the first chunk are now carried forward, and the
  gRPC stream is enumerated exactly once instead of being restarted per chunk.
- **`stream_query/2` swallowed connection failures**, returning an empty stream
  instead of an `{:error, reason}` item.
- **`insert_stream/3` ignored its documented `:format` and `:chunk_size`
  options**, passing each element straight into `input_data`; structured rows
  raised `Protobuf.EncodeError`. It now encodes and chunks via
  `Huginn.Clickhouse.Stream.input_stream/3`, which also terminates every row
  with a newline so adjacent chunks cannot merge rows.
- **`input_stream/3` re-enumerated its source once per chunk** (`Enum.drop` +
  `Enum.any?` lookahead), which was quadratic and re-ran side effects for
  non-restartable sources such as `File.stream!/2`.
- **Retries never fired for an unavailable pool.** `:not_connected`, the only
  error `GrpcConnectionPool.get_channel/1` returns, was missing from the
  transient set.
- **A trailers-only gRPC reply** (`grpc-status 0` with no message) raised a
  `FunctionClauseError` instead of returning `{:error, {:unexpected_reply, _}}`.

## [0.4.0] - 2026-06-06

### Added

- **Telemetry instrumentation.** `query/2`, `insert/3`, and `insert_stream/3`
  now emit `[:huginn, :query, :start | :stop | :exception]` span events with
  metadata (`:method`, `:sql`, `:query_id`, `:pool`, and `:rows`/`:stats` on
  stop). See `Huginn.Clickhouse.Telemetry`.
- **Built-in default logger.** `Huginn.attach_default_logger/1` /
  `detach_default_logger/0` attach a ready-made `Logger` handler for the
  telemetry events. Off by default.
- **Opt-in retries.** `query/2` and `insert/3` accept `:retries` and
  `:retry_backoff` options to retry transient transport failures
  (connection errors, gRPC `UNAVAILABLE`/`DEADLINE_EXCEEDED`) with exponential
  backoff. ClickHouse query errors are never retried. See
  `Huginn.Clickhouse.Retry`.
- `Huginn.Clickhouse.SQL.escape/1` for safe single-quoted string literals.
- `Result.parse_output/3`, a column-aware variant of `parse_output/2`.

### Fixed

- **Streaming insert (`insert_stream/3`) actually works now.** It was calling
  the client-streaming gRPC stub incorrectly (passing the request enumerable as
  the options argument), so no data was ever sent. It now opens the stream,
  pushes each `QueryInfo` with `GRPC.Stub.send_request/3` (setting
  `next_query_info` correctly and emitting a final END_STREAM frame), and reads
  the reply with `recv/2`. Verified end-to-end against a live ClickHouse.
- **Bidirectional streaming (`stream_io/1`) actually works now.** Same
  underlying stub-call bug, plus the gRPC stream is now driven from a single
  owner process (gun delivers all stream messages to one process), so sending
  and consuming replies no longer dead-locks. Verified end-to-end.
- **CSV parsing** now correctly handles quoted fields containing commas,
  embedded quotes (`""`), and is symmetric with the library's CSV writer.
  Previously a naive comma split corrupted such rows.
- **`JSONEachRow` results** now extract values in column order instead of
  relying on JSON object key order, which could misalign values with columns.
- **SQL string escaping** in `cancel/2` now escapes backslashes before quotes,
  so a `query_id` containing a trailing backslash can no longer break out of the
  string literal.
- **`stream_io/1`** now returns `{:error, reason}` when a channel cannot be
  acquired instead of raising a `MatchError` in the caller.

### Changed

- **Upgraded `grpc_connection_pool` to `~> 0.4.0`** (from `~> 0.2.1`). The 0.4.x
  line is a rewrite with an ETS/atomics zero-GenServer hot path and pluggable
  selection strategies; `Huginn.Clickhouse.Config.to_pool_config/1`'s `endpoint:`/`pool:` keyword
  output remains compatible, so no application changes were required.
- Public functions now read the application config once per call instead of
  twice (no behavior change).
- `cancel_where/2` documents that its condition is interpolated verbatim and
  must come from trusted input only.

### Tooling

- Added GitHub Actions CI (`compile --warnings-as-errors`, `format`, `credo`,
  `test`, `dialyzer`).
- Added a tag-triggered release workflow that publishes to Hex on `v*` tags
  (requires a `HEX_API_KEY` repository secret).
- Added Credo (`.credo.exs`) and Dialyzer (`dialyxir`) to the toolchain.
- Added `:telemetry` as a direct dependency; bumped `ex_doc` to `~> 0.34`.
- Added an integration test suite (tagged `:integration`, excluded by default;
  run with `mix test --include integration` against the docker-compose
  ClickHouse) covering the request/response and streaming paths end-to-end.

## [0.3.0] - 2025

- Initial public release on Hex: ClickHouse gRPC client supporting all four
  ClickHouse gRPC methods (`ExecuteQuery`, `ExecuteQueryWithStreamInput`,
  `ExecuteQueryWithStreamOutput`, `ExecuteQueryWithStreamIO`), connection
  pooling, password/JWT auth, query cancellation, and result parsing.

[0.4.0]: https://github.com/nyo16/huginn/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nyo16/huginn/releases/tag/v0.3.0
