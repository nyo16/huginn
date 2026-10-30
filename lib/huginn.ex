defmodule Huginn do
  @moduledoc """
  ClickHouse client for Elixir using gRPC.

  Huginn provides a simple interface for querying ClickHouse databases
  over gRPC with connection pooling and streaming support.

  ## Setup

  Add a pool to your supervision tree:

      children = [
        {Huginn,
         host: "localhost",
         port: 9100,
         database: "default",
         auth: {:password, "default", ""},
         pool_size: 5}
      ]

  or let Huginn's own application start it from config:

      config :huginn, :clickhouse,
        host: "localhost",
        port: 9100,
        database: "default",
        auth: {:password, "default", ""},
        pool_size: 5

  Several pools can run side by side under different `:pool_name`s. Select
  one per call with the `:pool` option. See `Huginn.Clickhouse.Pool`.

  ## Usage

      # Simple query
      {:ok, result} = Huginn.query("SELECT * FROM system.tables LIMIT 10")

      # Get results as maps
      maps = Huginn.Clickhouse.Result.to_maps(result)

      # Insert pre-encoded data
      {:ok, _} = Huginn.insert("INSERT INTO users (name, age) FORMAT TabSeparated", "john\\t25\\n")

      # Insert rows, streamed and encoded for you
      {:ok, _} =
        Huginn.insert_stream("INSERT INTO users (name, age) FORMAT TabSeparated", [
          ["john", 25],
          ["jane", 30]
        ])

      # Stream large results row by row
      "SELECT * FROM large_table"
      |> Huginn.stream_rows()
      |> Enum.each(&process_row/1)

  ## Errors

  Failures are returned as `{:error, %Huginn.Error{}}`. Bang and row-streaming
  functions raise `Huginn.Error`. See `Huginn.Error` for the reasons.

  ## Telemetry

  Requests emit `[:huginn, :query, :start | :stop | :exception]` events, plus
  `[:huginn, :query, :retry]` before each retry. Attach the built-in logger
  with `attach_default_logger/1`, or your own handler. See
  `Huginn.Clickhouse.Telemetry` for the full event reference.

  ## Retries

  All request functions accept `:retries`, `:retry_backoff` and
  `:retry_max_backoff` to retry connection failures with capped, jittered
  exponential backoff. Retries are off by default. ClickHouse errors and
  timeouts are never retried, and inserts are never re-sent. See
  `Huginn.Clickhouse.Retry`.
  """

  alias Huginn.Clickhouse.{Client, Config, Pool, Result, Telemetry}

  @doc """
  Returns a child spec that starts a connection pool.

  Takes the options of `Huginn.Clickhouse.Config.new/1`. See
  `Huginn.Clickhouse.Pool.child_spec/1`.
  """
  @spec child_spec(keyword() | map() | Config.t()) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: Pool

  @doc """
  Starts a connection pool linked to the caller.

  See `Huginn.Clickhouse.Pool.start_link/1`.
  """
  @spec start_link(keyword() | map() | Config.t()) :: Supervisor.on_start()
  defdelegate start_link(opts), to: Pool

  @doc """
  Blocks until the pool has a connected channel, or `timeout` milliseconds
  pass.

  See `Huginn.Clickhouse.Pool.await_ready/2`.
  """
  @spec await_ready(atom(), pos_integer()) :: :ok | {:error, Huginn.Error.t()}
  defdelegate await_ready(pool_name \\ Config.default_pool(), timeout \\ 10_000), to: Pool

  @doc """
  Executes a query and returns the result.

  See `Huginn.Clickhouse.Client.query/2` for options.
  """
  @spec query(String.t(), Client.query_opts()) :: {:ok, Result.t()} | {:error, Huginn.Error.t()}
  defdelegate query(sql, opts \\ []), to: Client

  @doc """
  Executes a query and raises `Huginn.Error` on failure.

  See `Huginn.Clickhouse.Client.query!/2` for options.
  """
  @spec query!(String.t(), Client.query_opts()) :: Result.t()
  defdelegate query!(sql, opts \\ []), to: Client

  @doc """
  Inserts already encoded data using a single request.

  See `Huginn.Clickhouse.Client.insert/3` for options.
  """
  @spec insert(String.t(), binary(), Client.query_opts()) ::
          {:ok, Result.t()} | {:error, Huginn.Error.t()}
  defdelegate insert(sql, data, opts \\ []), to: Client

  @doc """
  Inserts an enumerable of rows or encoded chunks as a stream.

  See `Huginn.Clickhouse.Client.insert_stream/3` for options.
  """
  @spec insert_stream(String.t(), Enumerable.t(), Client.query_opts()) ::
          {:ok, Result.t()} | {:error, Huginn.Error.t()}
  defdelegate insert_stream(sql, data, opts \\ []), to: Client

  @doc """
  Executes a query and returns a lazy stream of result chunks.

  See `Huginn.Clickhouse.Client.stream_query/2` for options.
  """
  @spec stream_query(String.t(), Client.query_opts()) :: Enumerable.t(Client.stream_element())
  defdelegate stream_query(sql, opts \\ []), to: Client

  @doc """
  Streams rows from a query result. Raises `Huginn.Error` on failure.

  See `Huginn.Clickhouse.Client.stream_rows/2` for options.
  """
  @spec stream_rows(String.t(), Client.query_opts()) :: Enumerable.t(Result.row())
  defdelegate stream_rows(sql, opts \\ []), to: Client

  @doc """
  Streams rows as maps from a query result. Raises `Huginn.Error` on failure.

  See `Huginn.Clickhouse.Client.stream_maps/2` for options.
  """
  @spec stream_maps(String.t(), Client.query_opts()) :: Enumerable.t(map())
  defdelegate stream_maps(sql, opts \\ []), to: Client

  @doc """
  Runs a query over streamed input and returns a lazy stream of result chunks.

  See `Huginn.Clickhouse.Client.stream_io/3` for options.
  """
  @spec stream_io(String.t(), Enumerable.t(), Client.query_opts()) ::
          Enumerable.t(Client.stream_element())
  defdelegate stream_io(sql, input, opts \\ []), to: Client

  @doc """
  Pings the ClickHouse server.

  See `Huginn.Clickhouse.Client.ping/1` for options.
  """
  @spec ping(Client.query_opts()) :: :ok | {:error, Huginn.Error.t()}
  defdelegate ping(opts \\ []), to: Client

  @doc """
  Cancels a running query by ID.

  See `Huginn.Clickhouse.Client.cancel/2` for options.
  """
  @spec cancel(String.t(), Client.query_opts()) :: :ok | {:error, Huginn.Error.t()}
  defdelegate cancel(query_id, opts \\ []), to: Client

  @doc """
  Cancels queries matching a condition. `condition` must be trusted input.

  See `Huginn.Clickhouse.Client.cancel_where/2` for options.
  """
  @spec cancel_where(String.t(), Client.query_opts()) :: :ok | {:error, Huginn.Error.t()}
  defdelegate cancel_where(condition, opts \\ []), to: Client

  @doc """
  Lists currently running queries.

  See `Huginn.Clickhouse.Client.running_queries/1` for options.
  """
  @spec running_queries(Client.query_opts()) :: {:ok, Result.t()} | {:error, Huginn.Error.t()}
  defdelegate running_queries(opts \\ []), to: Client

  @doc """
  Attaches the built-in `Logger` handler for Huginn telemetry events.

  See `Huginn.Clickhouse.Telemetry.attach_default_logger/1`.
  """
  @spec attach_default_logger(Logger.level()) :: :ok | {:error, :already_exists}
  defdelegate attach_default_logger(level \\ :info), to: Telemetry

  @doc """
  Detaches the built-in telemetry logger.

  See `Huginn.Clickhouse.Telemetry.detach_default_logger/0`.
  """
  @spec detach_default_logger() :: :ok | {:error, :not_found}
  defdelegate detach_default_logger(), to: Telemetry
end
