# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- **Elixir 1.18+ is required** (`googleapis`, pulled in by grpc, needs it).
- **One error type.** Every function returns `{:error, %Huginn.Error{}}`
  (`:reason` is `:server`, `:connection`, `:timeout`, `:grpc`, `:decode`,
  `:unexpected_reply` or `:not_configured`; `:code` is the ClickHouse error
  code for `:server`). `query!/2` raises `Huginn.Error` instead of a
  `RuntimeError`.
- **`stream_rows/2` and `stream_maps/2` raise `Huginn.Error`** on failure.
  They used to drop errors, so an outage looked like an empty result.
  `stream_query/2` errors are terminal: an error is always the last element.
- **`stream_io/1` is replaced by `stream_io(sql, input, opts)`.** ClickHouse's
  `ExecuteQueryWithStreamIO` runs one query with streamed input and output;
  the old `{output, send_fn}` API lost sends made after reading began and
  skipped the pool's credentials. The new function runs one
  `INSERT ... SELECT FROM input(...)` and returns a lazy stream of results.
- **TabSeparated and CSV now use ClickHouse's exact escaping.** Values
  containing tabs, newlines or backslashes used to split columns or, on
  insert, forge extra rows. `NULL` (`\N`) decodes to `nil` and `nil` encodes
  to `\N`. CSV is parsed over the whole buffer, so quoted newlines work, and
  the CSV writer always quotes strings.
- **`insert_stream/3` `:chunk_size` is bytes per message** (default 1 MiB),
  as the docs always said; it used to count elements, so the documented
  `File.stream!` import built ~64 MiB messages. An oversized element is sent
  alone. An empty source still sends the INSERT.
- **Structured rows for `JSONEachRow` must be maps**; list rows raise
  `ArgumentError` — use the new `format: "JSONCompactEachRow"`. Structured
  rows for formats the encoder doesn't support raise instead of being joined
  with tabs.
- **`Result.parse_output/3` returns `{:ok, rows} | {:error, %Huginn.Error{}}`**
  and malformed output (bad JSON or escapes) is a `:decode` error instead of
  a raise. Non-UTF-8 String columns decode to raw binaries.
- **Retries only cover `:connection` errors.** `DEADLINE_EXCEEDED`/timeouts
  are no longer retried. `insert/3`, `insert_stream/3` and the streams retry
  only channel acquisition, so nothing that reached the server is sent twice
  (a timed-out insert used to be re-sent and could duplicate rows). Backoff is
  capped by the new `:retry_max_backoff` (default 5 s) with full jitter.
  `Retry.transient?/1` takes a `Huginn.Error`.
- **Streaming calls default to `timeout: :infinity`** (was 60 s, which cut off
  long streams and large imports).
- **Config:**
  - `from_env/0` is removed from `Huginn.Clickhouse.Config`. There is no silent fallback to
    `localhost`; a call on a pool that was never started returns a
    `:not_configured` error.
  - The `:compression` option is removed. It never reached the server, and
    passed per query it returned compressed bytes that were parsed as rows.
  - `Huginn.Clickhouse.Config.new/1` raises `ArgumentError` for a missing or invalid `:host`,
    `:port`, `:pool_size`, `:database`, `:auth` or `:ssl`, and for unknown
    options. Messages never include values.
  - `with_auth/2` is replaced by `Huginn.Clickhouse.Query.with_config/2`.
  - `Huginn.Clickhouse.Query.build_insert/3` no longer maps `:format` to the
    output format; the input format is the SQL `FORMAT` clause.
- **TLS fails closed.** `ssl: true` raises if peer verification isn't
  available. An `ssl:` keyword list without `:verify` gets
  `verify: :verify_peer`, the system CAs (unless you pass `:cacerts`/
  `:cacertfile`) and hostname checking merged underneath; your keys win. Opt
  out with an explicit `verify: :verify_none`.
- **Telemetry `:stop` metadata:** `:rows` comes from the server's
  `stats.rows`, `:error` is a `Huginn.Error`, and early-stopped streams report
  `halted: true`.
- Removed `Result.merge/1` and the unused `Huginn.Clickhouse.Stream` helpers
  `collect_output/1`, `transform_output/2`, `parse_stream/1`, `row_stream/1`
  and `map_stream/1`. They also mis-decoded every chunk after the first.

### Added

- **Multiple pools:** `Huginn.child_spec/1`, `Huginn.start_link/1` and
  `Huginn.Clickhouse.Pool` start pools in your supervision tree, each with its
  own host, database and credentials, selected per call with `pool:`.
  `Huginn.await_ready/2` blocks until a pool has a connection.
- Telemetry spans for `insert_stream/3`, `stream_query/2` and `stream_io/3`,
  with the `query_id` the server actually runs. New
  `[:huginn, :query, :retry]` event.
- Output formats `JSONCompactEachRow`, `JSONStringsEachRow` and
  `JSONCompactStringsEachRow`.

### Fixed

- **The configured `:database` is sent with every query.** It used to be
  ignored, so queries ran against the user's default database.
- **Stopping a stream early cancels the server query.** This covers
  `Enum.take/2`, an exception, or the consumer process dying. `stream_query/2`
  now runs over `ExecuteQueryWithStreamIO` with the input side closed. A
  raising `insert_stream/3` source cancels the insert.
- **Streaming is flow-controlled in both directions, so memory stays
  bounded.** grpc-elixir's gun adapter refilled the HTTP/2 receive window on
  its own and sent input without waiting for the send window, so a slow
  consumer buffered the whole result and a fast source buffered the whole
  input. `insert_stream/3`, `stream_query/2` and `stream_io/3` now run on
  their own HTTP/2 connection (Mint, passive mode) that is only read as fast
  as the stream is consumed and only written as fast as the server grants
  window. Unary calls still use the pool. Reading a large result is also
  about 3.5× faster.
- Cancelling a stream no longer logs grpc's `received unexpected message:
  {:gun_error, ..., :badstate, ...}` warning, because stream cancellation no
  longer goes through grpc's shared connection process.
- **Credentials no longer leak.** `inspect/1` on a config hides `:auth`, and
  boot and validation errors don't echo the options.
- `stream_query/2` reads replies lazily, one at a time, and no longer falls
  back to grpc's 10 s receive timeout.

### Dependencies

- `:protobuf` `~> 0.17.1` (keeps dependents off 0.17.0, affected by
  EEF-CVE-2026-104635). The bindings were regenerated with `protoc-gen-elixir`
  0.17.1; `priv/protos/clickhouse_grpc.proto` already matched ClickHouse
  `master`.
- `:grpc` is now a direct dependency at `~> 1.0.5`. grpc_core 1.0.5 stops
  rounding the `grpc-timeout` sent to the server down, e.g. 90 s → 1 min.
- grpc_connection_pool 0.5.3, cowlib 2.21.0 (fixes CVE-2026-43971 and
  CVE-2026-43969, which were not reachable from huginn), plus dev-tool patch
  updates.
- Test coverage excludes the generated `Clickhouse.Grpc.*` modules.
- New runtime dependency `:mint` `~> 1.11` for the streaming transport.

### CI

- New jobs: a dependency audit (`hex.audit`, `deps.audit` via mix_audit,
  unused-lock check) and the ClickHouse integration suite. Actions are pinned
  to commit SHAs and the workflow token is read-only.
- Releases publish only after the full CI suite passes, and only when the tag
  matches `@version` in `mix.exs`; the tag no longer rewrites the version.
  `HEX_API_KEY` moves to the `hex` GitHub environment.

## [0.5.0] - 2026-08-29

### Breaking

- **`ssl: true` now verifies the peer certificate.** It previously sent
  `ssl: []`, which is truthy and so overrode the connection pool's own
  verifying default, leaving `verify` unset — `verify_none` on OTP < 26. A
  connection to a server with a self-signed or otherwise untrusted certificate
  that used to succeed will now fail during the TLS handshake. That is the
  point, but it is a visible break. To keep connecting, supply the trust chain
  explicitly: `ssl: [verify: :verify_peer, cacertfile: "/path/to/ca.pem"]`.
- **`insert_stream/3` now honours `:format` and `:chunk_size`.** Both were
  previously ignored: every element of the enumerable was sent verbatim as
  `input_data`, so structured rows raised `Protobuf.EncodeError` and neither
  option had any effect. Elements that are already binaries are still
  concatenated verbatim, so `File.stream!/2` pipelines are unaffected. If you
  pass structured rows, make sure the SQL `FORMAT` clause matches `:format`
  (which defaults to `"TabSeparated"`) — a mismatch is now a ClickHouse parse
  error rather than a silent no-op.
- **`stream_query/2` now emits `{:error, reason}` instead of ending empty.** On
  a connection failure it previously produced an empty stream, indistinguishable
  from an empty result set. Consumers that match only `{:ok, result}` will now
  raise on the error element instead of silently observing zero rows.

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
- **`Huginn.Clickhouse.Result.output_format` is now `nil` rather than `""`** when
  the server omits it — which it does on every chunk after the first of a
  streaming response. Code pattern-matching on `""` needs updating.
- **Added `Huginn.Clickhouse.Result.from_grpc/2`**, which accepts `:format` and
  `:columns` defaults so a streaming consumer can decode a later chunk with the
  metadata established by the first one. `from_grpc/1` is unchanged.

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

[0.5.0]: https://github.com/nyo16/huginn/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/nyo16/huginn/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nyo16/huginn/releases/tag/v0.3.0
