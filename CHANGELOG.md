# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
