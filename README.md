# Huginn

ClickHouse client for Elixir using gRPC with connection pooling.

## Features

- gRPC protocol for efficient binary communication
- Connection pooling with health monitoring for requests; streaming calls get
  their own flow-controlled connection, so large reads and writes stay
  bounded in memory
- Support for all 4 ClickHouse gRPC methods:
  - `ExecuteQuery` - simple request/response
  - `ExecuteQueryWithStreamInput` - streaming inserts
  - `ExecuteQueryWithStreamIO` - lazy, cancellable result streams and
    streamed `INSERT ... SELECT FROM input(...)`
- Both password and JWT authentication
- Query cancellation support
- Automatic result parsing with exact ClickHouse TSV/CSV escaping (`NULL` ↔ `nil`)
- Multiple pools with their own host, database and credentials
- A single error type, `Huginn.Error`
- Telemetry instrumentation and an opt-in default logger
- Opt-in retries for connection failures

## Installation

Add `huginn` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:huginn, "~> 0.6.0"}
  ]
end
```

## Configuration

Configure the ClickHouse connection in your `config/config.exs`:

```elixir
config :huginn, :clickhouse,
  host: "localhost",
  port: 9100,
  database: "default",
  auth: {:password, "default", ""},
  pool_size: 5
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `:host` | (required) | ClickHouse server hostname |
| `:port` | `9100` | gRPC port |
| `:database` | `"default"` | Default database |
| `:auth` | `nil` | `{:password, user, pass}` or `{:jwt, token}` |
| `:pool_size` | `5` | Number of connections |
| `:ssl` | `false` | `true` verifies the peer against the system CA store. A keyword list is passed to `:ssl` with `verify: :verify_peer`, the system CAs and hostname checking merged underneath (your keys win; opt out with an explicit `verify: :verify_none`) |
| `:pool_name` | `:clickhouse_pool` | Name used to select the pool with `pool:` |

`Huginn.Clickhouse.Config.new/1` raises `ArgumentError` for a missing `:host`
or any invalid/unknown option; the message never includes credentials, and
`inspect/1` on a config hides `:auth`.

### Multiple Pools

The pool configured under `:huginn, :clickhouse` is started by Huginn's own
application. Start more pools — different hosts, databases or credentials — in
your supervision tree and select them per call with `pool:`:

```elixir
children = [
  {Huginn, host: "analytics.internal", database: "events", pool_name: :events,
   auth: {:password, "reader", System.fetch_env!("EVENTS_PASSWORD")}}
]

:ok = Huginn.await_ready(:events, 5_000)
Huginn.query("SELECT count() FROM page_views", pool: :events)
```

A call naming a pool that was never started returns
`{:error, %Huginn.Error{reason: :not_configured}}`.

### Production Configuration

Use `config/runtime.exs`, not `config/prod.exs`. `config/prod.exs` is evaluated
at *build* time, which bakes the ClickHouse password into the release. Reading
secrets at runtime with `System.fetch_env!/1` also fails loudly on a missing
variable instead of silently authenticating with an empty password.

```elixir
# config/runtime.exs
import Config

if config_env() == :prod do
  config :huginn, :clickhouse,
    host: System.fetch_env!("CLICKHOUSE_HOST"),
    port: String.to_integer(System.get_env("CLICKHOUSE_PORT", "9100")),
    database: System.get_env("CLICKHOUSE_DATABASE", "default"),
    auth: {:password, System.fetch_env!("CLICKHOUSE_USER"), System.fetch_env!("CLICKHOUSE_PASSWORD")},
    pool_size: 10,
    ssl: true
end
```

For a private CA or client certificates, pass `:ssl` options through directly:

```elixir
  ssl: [verify: :verify_peer, cacertfile: "/etc/ssl/clickhouse-ca.pem"]
```

## Usage

### gRPC Methods Overview

| gRPC Method | Function | Use Case |
|-------------|----------|----------|
| `ExecuteQuery` | `query/2`, `insert/3` | Simple queries, small inserts |
| `ExecuteQueryWithStreamInput` | `insert_stream/3` | Large imports |
| `ExecuteQueryWithStreamIO` | `stream_query/2`, `stream_rows/2`, `stream_maps/2` | Large result sets (input side closed immediately, so the stream can be cancelled) |
| `ExecuteQueryWithStreamIO` | `stream_io/3` | `INSERT ... SELECT FROM input(...)` with streamed input |

### Errors

Every function returns `{:error, %Huginn.Error{}}` on failure; `query!/2`,
`stream_rows/2` and `stream_maps/2` raise it. Branch on `:reason`
(`:server`, `:connection`, `:timeout`, `:grpc`, `:decode`,
`:unexpected_reply`, `:not_configured`); `:code` holds the ClickHouse error
code for `:server` errors.

```elixir
case Huginn.query("SELEC 1") do
  {:ok, result} -> result.rows
  {:error, %Huginn.Error{reason: :server, code: 62} = e} -> Logger.warning(e.message)
  {:error, %Huginn.Error{} = e} -> raise e
end
```

### Simple Queries (ExecuteQuery)

```elixir
{:ok, result} = Huginn.query("SELECT name, engine FROM system.tables LIMIT 10")

# Rows as maps keyed by column name
maps = Huginn.Clickhouse.Result.to_maps(result)

# With options
{:ok, result} =
  Huginn.query("SELECT * FROM users WHERE status = 'active'",
    database: "mydb",
    format: "JSONEachRow",
    timeout: 30_000
  )

# Raising version
result = Huginn.query!("SELECT 1")
```

TabSeparated and CSV values are decoded with ClickHouse's escaping rules, so
values containing tabs, newlines or backslashes come back intact and `NULL` is
`nil`. JSON formats keep JSON types.

Interpolating values into SQL: escape them with
`Huginn.Clickhouse.SQL.escape/1` and always place them inside single quotes:

```elixir
alias Huginn.Clickhouse.SQL
Huginn.query("SELECT * FROM users WHERE name = '#{SQL.escape(name)}'")
```

### Inserts (ExecuteQuery)

`insert/3` takes the already-encoded payload; its format is the SQL `FORMAT`
clause.

```elixir
{:ok, _} = Huginn.insert("INSERT INTO users (name, age) FORMAT TabSeparated", "john\t25\njane\t30")

{:ok, _} =
  Huginn.insert("INSERT INTO users FORMAT JSONEachRow",
    ~s({"name":"john","age":25}\n{"name":"jane","age":30})
  )
```

### Streaming Inserts (ExecuteQueryWithStreamInput)

The data is the second argument. Elements may be pre-encoded binaries (sent
verbatim) or rows, encoded according to `:format` (`"TabSeparated"` by
default; `"CSV"`, `"JSONEachRow"` for maps, `"JSONCompactEachRow"` for lists).
`:chunk_size` is the target size of each gRPC message in bytes (default 1 MiB).

```elixir
# Stream a file; pre-encoded chunks are passed through untouched
File.stream!("large_data.csv", 65_536)
|> then(&Huginn.insert_stream("INSERT INTO logs FORMAT CSV", &1))

# Structured rows, escaped for you; nil becomes NULL
rows = [["neo", "1"], ["trinity", nil]]
{:ok, _} = Huginn.insert_stream("INSERT INTO users (name, team) FORMAT TabSeparated", rows)

# Maps for JSONEachRow
{:ok, _} =
  Huginn.insert_stream("INSERT INTO users FORMAT JSONEachRow", [%{name: "neo", age: 30}],
    format: "JSONEachRow"
  )
```

Inserts are never re-sent once data reached the server (see [Retries](#retries)).
If the source raises mid-stream, the insert is cancelled and nothing is
committed.

### Streaming Results

`stream_query/2` is lazy and enumerated once. It yields `{:ok, result}` chunks;
an error is always the last element. `stream_rows/2` and `stream_maps/2`
flatten the chunks and raise `Huginn.Error` on failure, like `File.stream!/2`.
Stopping early (`Enum.take/2`, `Enum.find/2`, an exception, the consumer
process dying) cancels the query on the server. Streams have no timeout unless
you pass `:timeout`.

```elixir
Huginn.stream_query("SELECT * FROM large_table")
|> Enum.each(fn
  {:ok, result} -> process_chunk(result.rows)
  {:error, error} -> Logger.error(Exception.message(error))
end)

# Rows, in batches
Huginn.stream_rows("SELECT * FROM events")
|> Stream.chunk_every(1_000)
|> Enum.each(&process_batch/1)

# Maps; stops (and cancels the query) after the first match
Huginn.stream_maps("SELECT id, name FROM users")
|> Enum.find(fn %{"name" => name} -> name == "neo" end)
```

> #### Memory {: .tip}
>
> Streaming calls are flow-controlled in both directions. Each one runs on
> its own HTTP/2 connection to the pool's server (same TLS settings and
> credentials), which is only read as fast as you consume the stream: a slow
> consumer makes ClickHouse wait instead of buffering the result in memory,
> and stalling one stream stalls no other query. On the way in, the next
> message (about `:chunk_size` bytes) is only pulled from your source once
> the previous one fits in the server's flow-control window. ClickHouse sends
> about one block per message, so lower the `max_block_size` setting if
> single blocks of your rows are very large.

### Streamed Input and Output (ExecuteQueryWithStreamIO)

`stream_io/3` runs one `INSERT ... SELECT FROM input(...)` statement: the input
enumerable is encoded like `insert_stream/3`, and the returned lazy stream
carries the server's results (progress, logs, stats). Pool credentials and
database apply.

```elixir
Huginn.stream_io(
  "INSERT INTO squares SELECT x, x * x FROM input('x UInt32') FORMAT TabSeparated",
  Stream.map(1..1_000_000, &[&1])
)
|> Enum.to_list()
```

### Query Cancellation

```elixir
query_id = "my-long-query-#{System.unique_integer([:positive])}"

task = Task.async(fn -> Huginn.query("SELECT sleep(3)", query_id: query_id) end)

:ok = Huginn.cancel(query_id)

# Trusted input only: the condition is interpolated verbatim
Huginn.cancel_where("elapsed > 60")

# List running queries
{:ok, result} = Huginn.running_queries()
Huginn.Clickhouse.Result.to_maps(result)
```

### Health Check

```elixir
case Huginn.ping() do
  :ok -> IO.puts("Connected!")
  {:error, error} -> IO.puts("Failed: #{Exception.message(error)}")
end
```

## Telemetry

Every request — `query/2`, `insert/3`, `insert_stream/3`, `stream_query/2`,
`stream_io/3` — is wrapped in a [`:telemetry`](https://hexdocs.pm/telemetry)
span. For lazy streams, `:start` fires when enumeration begins and `:stop`
when it ends.

| Event | Measurements | Metadata |
| ----- | ------------ | -------- |
| `[:huginn, :query, :start]` | `:system_time`, `:monotonic_time` | `:method`, `:sql`, `:query_id`, `:pool` |
| `[:huginn, :query, :stop]` | `:duration`, `:monotonic_time` | above + `:stats` and `:rows`, or `:error` (a `Huginn.Error`), or `halted: true` |
| `[:huginn, :query, :exception]` | `:duration`, `:monotonic_time` | above + `:kind`, `:reason`, `:stacktrace` |
| `[:huginn, :query, :retry]` | `:attempt`, `:delay` | `:method`, `:sql`, `:query_id`, `:pool`, `:error` |

Attach the built-in logger (off by default), or your own handler:

```elixir
# Logs each completed query with its duration; failures log at :error.
Huginn.attach_default_logger(:info)

# ...or attach a custom handler (a module function capture avoids
# telemetry's local-function performance warning)
:telemetry.attach("my-handler", [:huginn, :query, :stop], &MyApp.Metrics.handle_event/4, nil)
```

See `Huginn.Clickhouse.Telemetry` for the full reference.

## Retries

Retries are off by default and only retry `%Huginn.Error{reason: :connection}`
failures; ClickHouse errors and timeouts are never retried. The delay before
attempt `n` is drawn uniformly from `1..min(retry_backoff * 2^(n-1), retry_max_backoff)`.

- `query/2` repeats the whole request, so only enable retries for statements
  that are safe to run twice.
- `insert/3`, `insert_stream/3` and the streams retry only channel
  acquisition — nothing that reached the server is sent twice. For retries of
  your own, set ClickHouse's `insert_deduplication_token` setting.

```elixir
Huginn.query("SELECT 1", retries: 3, retry_backoff: 100, retry_max_backoff: 2_000)
```

See `Huginn.Clickhouse.Retry` for details.

## Development

### Prerequisites

- Elixir 1.18+
- Docker and Docker Compose
- `protoc` compiler (`brew install protobuf` on macOS)

### Start ClickHouse

```bash
docker-compose up -d
```

This starts ClickHouse with gRPC enabled on port 9100.

### Run Tests

```bash
# Unit tests (no ClickHouse required)
mix test

# Include the end-to-end suite (requires ClickHouse via docker-compose up -d)
mix test --include integration
```

### Generate Documentation

```bash
mix docs
```

### Regenerate Proto Files

`priv/protos/clickhouse_grpc.proto` is vendored from
[ClickHouse](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto).
To refresh it and regenerate the bindings:

```bash
# Pin the plugin to the `:protobuf` version in mix.lock so the generated code
# and the runtime library never disagree.
mix escript.install hex protobuf 0.17.1

curl -o priv/protos/clickhouse_grpc.proto \
  https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/src/Server/grpc_protos/clickhouse_grpc.proto

protoc --elixir_out=plugins=grpc:./lib/huginn/proto \
  --proto_path=./priv/protos \
  clickhouse_grpc.proto
```

The plugin nests output under the proto package, so the generated file lands at
`lib/huginn/proto/clickhouse/grpc/clickhouse_grpc.pb.ex`. `mix format` skips
`lib/huginn/proto/` (see `.formatter.exs`), so generated code is left as emitted.

If the regenerated proto adds a `LogsLevel` member, the guard test in
`test/huginn/clickhouse/result_test.exs` fails until it is mapped in
`Huginn.Clickhouse.Result`.

## Architecture

```
lib/huginn/
├── clickhouse/
│   ├── client.ex      # High-level API (all 4 gRPC methods)
│   ├── config.ex      # Configuration management
│   ├── query.ex       # QueryInfo message builders
│   ├── result.ex      # Result parsing utilities
│   ├── retry.ex       # Transient-failure retries
│   ├── sql.ex         # SQL escaping helpers
│   ├── stream.ex      # Streaming helpers
│   └── telemetry.ex   # Telemetry events
└── proto/
    └── clickhouse/grpc/
        └── clickhouse_grpc.pb.ex  # Generated protobuf code
```

## License

MIT
