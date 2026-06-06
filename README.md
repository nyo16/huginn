# Huginn

ClickHouse client for Elixir using gRPC with connection pooling.

## Features

- gRPC protocol for efficient binary communication
- Connection pooling with health monitoring
- Support for all 4 ClickHouse gRPC methods:
  - `ExecuteQuery` - simple request/response
  - `ExecuteQueryWithStreamInput` - streaming inserts
  - `ExecuteQueryWithStreamOutput` - streaming large results
  - `ExecuteQueryWithStreamIO` - bidirectional streaming
- Both password and JWT authentication
- Query cancellation support
- Automatic result parsing
- Telemetry instrumentation and an opt-in default logger
- Opt-in retries for transient transport failures

## Installation

Add `huginn` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:huginn, "~> 0.3.0"}
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
| `:ssl` | `false` | Enable SSL/TLS |
| `:pool_name` | `:clickhouse_pool` | Pool name for multiple pools |

### Production Configuration

```elixir
# config/prod.exs
config :huginn, :clickhouse,
  host: System.get_env("CLICKHOUSE_HOST"),
  port: String.to_integer(System.get_env("CLICKHOUSE_PORT", "9100")),
  database: System.get_env("CLICKHOUSE_DATABASE", "default"),
  auth: {:password, System.get_env("CLICKHOUSE_USER"), System.get_env("CLICKHOUSE_PASSWORD")},
  pool_size: 10,
  ssl: true
```

## Usage

### gRPC Methods Overview

| gRPC Method | Function | Use Case |
|-------------|----------|----------|
| `ExecuteQuery` | `query/2`, `insert/3` | Simple queries, small inserts |
| `ExecuteQueryWithStreamInput` | `insert_stream/3` | Large file imports |
| `ExecuteQueryWithStreamOutput` | `stream_query/2` | Large result sets |
| `ExecuteQueryWithStreamIO` | `stream_io/1` | Bidirectional streaming |

### Simple Queries (ExecuteQuery)

```elixir
# Execute a query
{:ok, result} = Huginn.query("SELECT * FROM system.tables LIMIT 10")

# Get results as maps
maps = Huginn.Clickhouse.Result.to_maps(result)

# Query with options
{:ok, result} = Huginn.query(
  "SELECT * FROM users WHERE status = 'active'",
  database: "mydb",
  format: "JSONEachRow",
  timeout: 30_000
)

# Raising version
result = Huginn.query!("SELECT 1")
```

### Inserts (ExecuteQuery)

```elixir
# Simple insert with TabSeparated data
data = "john\t25\njane\t30"
{:ok, _} = Huginn.insert(
  "INSERT INTO users (name, age) FORMAT TabSeparated",
  data
)

# JSONEachRow format
data = ~s({"name":"john","age":25}\n{"name":"jane","age":30})
{:ok, _} = Huginn.insert("INSERT INTO users FORMAT JSONEachRow", data)
```

### Streaming Inserts (ExecuteQueryWithStreamInput)

```elixir
# Stream from a file
File.stream!("large_data.csv", [], 65_536)
|> Huginn.insert_stream("INSERT INTO logs FORMAT CSV")

# Stream with progress tracking using Agent
{:ok, counter} = Agent.start_link(fn -> 0 end)

large_data
|> Stream.chunk_every(1000)
|> Stream.map(fn chunk ->
  Agent.update(counter, &(&1 + length(chunk)))
  Enum.join(chunk, "\n")
end)
|> Huginn.insert_stream("INSERT INTO events FORMAT TabSeparated")

IO.puts("Inserted #{Agent.get(counter, & &1)} rows")
Agent.stop(counter)
```

### Streaming Results (ExecuteQueryWithStreamOutput)

```elixir
# Basic streaming
Huginn.stream_query("SELECT * FROM large_table")
|> Enum.each(fn
  {:ok, result} -> process_chunk(result)
  {:error, error} -> Logger.error("Error: #{inspect(error)}")
end)

# Stream rows directly
Huginn.stream_rows("SELECT * FROM events")
|> Stream.take(1000)
|> Enum.to_list()

# Stream as maps
Huginn.stream_maps("SELECT name, age FROM users")
|> Stream.filter(fn %{"age" => age} -> String.to_integer(age) > 18 end)
|> Enum.to_list()
```

### Using Agent to Accumulate Results

```elixir
# Accumulate all rows with error counting
{:ok, agent} = Agent.start_link(fn -> %{rows: [], errors: 0} end)

Huginn.stream_query("SELECT * FROM metrics")
|> Enum.each(fn
  {:ok, result} ->
    Agent.update(agent, fn state ->
      %{state | rows: state.rows ++ result.rows}
    end)
  {:error, _} ->
    Agent.update(agent, fn state ->
      %{state | errors: state.errors + 1}
    end)
end)

final_state = Agent.get(agent, & &1)
IO.puts("Got #{length(final_state.rows)} rows, #{final_state.errors} errors")
Agent.stop(agent)
```

### Custom Stream.resource Pattern

```elixir
defmodule MyApp.ClickHouseStream do
  @moduledoc "Custom streaming with backpressure control"

  def stream_with_backpressure(query, batch_size \\ 100) do
    Stream.resource(
      fn -> init_query(query) end,
      fn state -> next_batch(state, batch_size) end,
      fn _state -> :ok end
    )
  end

  defp init_query(query) do
    stream = Huginn.stream_rows(query)
    %{stream: stream, buffer: [], done: false}
  end

  defp next_batch(%{done: true} = state, _batch_size) do
    {:halt, state}
  end

  defp next_batch(%{stream: stream, buffer: buffer} = state, batch_size) do
    {rows, rest} =
      stream
      |> Stream.take(batch_size - length(buffer))
      |> Enum.to_list()
      |> then(fn new_rows -> {buffer ++ new_rows, stream} end)

    if length(rows) < batch_size do
      {[rows], %{state | done: true}}
    else
      {[Enum.take(rows, batch_size)], %{state | buffer: Enum.drop(rows, batch_size)}}
    end
  end
end

# Usage
MyApp.ClickHouseStream.stream_with_backpressure("SELECT * FROM events")
|> Enum.each(fn batch ->
  process_batch(batch)
  Process.sleep(100)  # Rate limiting
end)
```

### Bidirectional Streaming (ExecuteQueryWithStreamIO)

```elixir
# Interactive query session
{output, send} = Huginn.stream_io()

# Send queries
send.(Huginn.Clickhouse.Query.build("SELECT 1"))
send.(Huginn.Clickhouse.Query.build("SELECT 2"))

# Process results
Task.async(fn ->
  Enum.each(output, fn
    {:ok, result} -> IO.inspect(result.rows)
    {:error, err} -> IO.puts("Error: #{inspect(err)}")
  end)
end)
```

### Query Cancellation

```elixir
# Start a long-running query with custom ID
query_id = "my-long-query-#{System.unique_integer()}"

task = Task.async(fn ->
  Huginn.query("SELECT sleep(300)", query_id: query_id)
end)

# Cancel after some time
Process.sleep(5_000)
:ok = Huginn.cancel(query_id)

# Cancel queries by condition
Huginn.cancel_where("elapsed > 60")
Huginn.cancel_where("user = 'admin'")

# List running queries
{:ok, result} = Huginn.running_queries()
Huginn.Clickhouse.Result.to_maps(result)
|> Enum.each(fn q -> IO.puts("#{q["query_id"]}: #{q["query"]}") end)
```

### Health Check

```elixir
case Huginn.ping() do
  :ok -> IO.puts("Connected!")
  {:error, reason} -> IO.puts("Failed: #{inspect(reason)}")
end
```

## Telemetry

Each request issued through `query/2`, `insert/3`, and `insert_stream/3` is
wrapped in a [`:telemetry`](https://hexdocs.pm/telemetry) span:

| Event | Measurements | Metadata |
| ----- | ------------ | -------- |
| `[:huginn, :query, :start]` | `:system_time`, `:monotonic_time` | `:method`, `:sql`, `:query_id`, `:pool` |
| `[:huginn, :query, :stop]` | `:duration`, `:monotonic_time` | above + `:rows`, `:stats` (or `:error`) |
| `[:huginn, :query, :exception]` | `:duration`, `:monotonic_time` | above + `:kind`, `:reason`, `:stacktrace` |

Attach the built-in logger (off by default), or your own handler:

```elixir
# Logs each completed query with its duration; failures log at :error.
Huginn.attach_default_logger(:info)

# ...or attach a custom handler
:telemetry.attach(
  "my-handler",
  [:huginn, :query, :stop],
  fn _event, %{duration: d}, meta, _ ->
    ms = System.convert_time_unit(d, :native, :millisecond)
    MyMetrics.histogram("clickhouse.query.duration", ms, tags: [meta.method])
  end,
  nil
)
```

See `Huginn.Clickhouse.Telemetry` for the full reference.

## Retries

`query/2` and `insert/3` can retry transient transport failures (connection
errors and gRPC `UNAVAILABLE`/`DEADLINE_EXCEEDED`) with exponential backoff.
Retries are off by default, and ClickHouse query errors are never retried.

```elixir
# Up to 3 extra attempts, backing off 100ms, 200ms, 400ms.
Huginn.query("SELECT 1", retries: 3, retry_backoff: 100)
```

See `Huginn.Clickhouse.Retry` for details.

## Development

### Prerequisites

- Elixir 1.14+
- Docker and Docker Compose
- `protoc` compiler (`brew install protobuf` on macOS)

### Start ClickHouse

```bash
docker-compose up -d
```

This starts ClickHouse with gRPC enabled on port 9100.

### Run Tests

```bash
mix test
```

### Generate Documentation

```bash
mix docs
```

### Regenerate Proto Files

If you need to regenerate the proto files after updating the `.proto` file:

```bash
# Install protoc-gen-elixir
mix escript.install hex protobuf

# Generate Elixir code
protoc --elixir_out=plugins=grpc:./lib/huginn/proto \
  --proto_path=./priv/protos \
  clickhouse_grpc.proto
```

## Architecture

```
lib/huginn/
├── clickhouse/
│   ├── client.ex      # High-level API (all 4 gRPC methods)
│   ├── config.ex      # Configuration management
│   ├── query.ex       # QueryInfo message builders
│   ├── result.ex      # Result parsing utilities
│   └── stream.ex      # Streaming helpers
└── proto/
    └── clickhouse_grpc.pb.ex  # Generated protobuf code
```

## License

MIT
