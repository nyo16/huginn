defmodule Huginn do
  @moduledoc """
  ClickHouse client for Elixir using gRPC.

  Huginn provides a simple interface for querying ClickHouse databases
  over gRPC with connection pooling and streaming support.

  ## Configuration

  Configure ClickHouse connection in your `config.exs`:

      config :huginn, :clickhouse,
        host: "localhost",
        port: 9100,
        database: "default",
        auth: {:password, "default", ""},
        pool_size: 5

  ## Usage

      # Simple query
      {:ok, result} = Huginn.query("SELECT * FROM system.tables LIMIT 10")

      # Get results as maps
      maps = Huginn.Clickhouse.Result.to_maps(result)

      # Insert data
      data = [["john", "25"], ["jane", "30"]]
      {:ok, _} = Huginn.insert("INSERT INTO users (name, age) VALUES", data)

      # Stream large results
      Huginn.stream_query("SELECT * FROM large_table")
      |> Enum.each(&process_row/1)

  ## Telemetry

  Requests emit `[:huginn, :query, :start | :stop | :exception]` events. Attach
  the built-in logger with `attach_default_logger/1`, or your own handler — see
  `Huginn.Clickhouse.Telemetry` for the full event reference.

  ## Retries

  `query/2` and `insert/3` accept `:retries` and `:retry_backoff` options to
  retry transient transport failures with exponential backoff. Retries are
  off by default; ClickHouse query errors are never retried. See
  `Huginn.Clickhouse.Retry`.

  """

  alias Huginn.Clickhouse.{Client, Telemetry}

  @doc """
  Executes a query and returns the result.

  See `Huginn.Clickhouse.Client.query/2` for options.
  """
  defdelegate query(sql, opts \\ []), to: Client

  @doc """
  Executes a query and raises on error.

  See `Huginn.Clickhouse.Client.query!/2` for options.
  """
  defdelegate query!(sql, opts \\ []), to: Client

  @doc """
  Inserts data using a single request.

  See `Huginn.Clickhouse.Client.insert/3` for options.
  """
  defdelegate insert(sql, data, opts \\ []), to: Client

  @doc """
  Inserts data using streaming input for large datasets.

  See `Huginn.Clickhouse.Client.insert_stream/3` for options.
  """
  defdelegate insert_stream(sql, data_stream, opts \\ []), to: Client

  @doc """
  Executes a query and returns a stream of results.

  See `Huginn.Clickhouse.Client.stream_query/2` for options.
  """
  defdelegate stream_query(sql, opts \\ []), to: Client

  @doc """
  Streams rows from a query result.

  See `Huginn.Clickhouse.Client.stream_rows/2` for options.
  """
  defdelegate stream_rows(sql, opts \\ []), to: Client

  @doc """
  Streams rows as maps from a query result.

  See `Huginn.Clickhouse.Client.stream_maps/2` for options.
  """
  defdelegate stream_maps(sql, opts \\ []), to: Client

  @doc """
  Pings the ClickHouse server.

  See `Huginn.Clickhouse.Client.ping/1` for options.
  """
  defdelegate ping(opts \\ []), to: Client

  @doc """
  Cancels a running query by ID.

  See `Huginn.Clickhouse.Client.cancel/2` for options.
  """
  defdelegate cancel(query_id, opts \\ []), to: Client

  @doc """
  Cancels queries matching a condition.

  See `Huginn.Clickhouse.Client.cancel_where/2` for options.
  """
  defdelegate cancel_where(condition, opts \\ []), to: Client

  @doc """
  Lists currently running queries.

  See `Huginn.Clickhouse.Client.running_queries/1` for options.
  """
  defdelegate running_queries(opts \\ []), to: Client

  @doc """
  Opens a bidirectional streaming connection.

  See `Huginn.Clickhouse.Client.stream_io/1` for options.
  """
  defdelegate stream_io(opts \\ []), to: Client

  @doc """
  Attaches the built-in `Logger` handler for Huginn telemetry events.

  See `Huginn.Clickhouse.Telemetry.attach_default_logger/1`.
  """
  defdelegate attach_default_logger(level \\ :info), to: Telemetry

  @doc """
  Detaches the built-in telemetry logger.

  See `Huginn.Clickhouse.Telemetry.detach_default_logger/0`.
  """
  defdelegate detach_default_logger(), to: Telemetry
end
