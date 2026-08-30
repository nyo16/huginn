defmodule Huginn.Clickhouse.Client do
  @moduledoc """
  High-level ClickHouse gRPC client.

  Provides a simple interface for executing queries against ClickHouse
  using gRPC with connection pooling. Supports all 4 ClickHouse gRPC methods:

  ## gRPC Methods

  | Method | Function | Use Case |
  |--------|----------|----------|
  | `ExecuteQuery` | `query/2`, `insert/3` | Simple queries and small inserts |
  | `ExecuteQueryWithStreamInput` | `insert_stream/3` | Large data inserts |
  | `ExecuteQueryWithStreamOutput` | `stream_query/2` | Large result sets |
  | `ExecuteQueryWithStreamIO` | `stream_io/1` | Bidirectional streaming |

  ## Examples

  ### Simple Query (ExecuteQuery)

      {:ok, result} = Huginn.Clickhouse.Client.query("SELECT 1")

  ### Streaming Insert (ExecuteQueryWithStreamInput)

      data_chunks = ["row1\\tval1", "row2\\tval2", "row3\\tval3"]
      {:ok, result} = Huginn.Clickhouse.Client.insert_stream(
        "INSERT INTO t FORMAT TabSeparated",
        data_chunks
      )

  ### Streaming Results (ExecuteQueryWithStreamOutput)

      Huginn.Clickhouse.Client.stream_query("SELECT * FROM large_table")
      |> Enum.each(fn {:ok, result} -> process(result) end)

  ### Using Agent to Accumulate Results

      {:ok, agent} = Agent.start_link(fn -> [] end)

      Huginn.Clickhouse.Client.stream_query("SELECT * FROM events")
      |> Enum.each(fn
        {:ok, result} ->
          Agent.update(agent, fn acc -> acc ++ result.rows end)
        {:error, _} ->
          :skip
      end)

      all_rows = Agent.get(agent, & &1)
      Agent.stop(agent)

  ### Stream.resource Pattern

      defmodule MyApp.ClickHouseStream do
        def stream_events(query) do
          Stream.resource(
            fn -> start_query(query) end,
            &next_chunk/1,
            &cleanup/1
          )
        end

        defp start_query(query) do
          case Huginn.Clickhouse.Client.query(query) do
            {:ok, result} -> {:ok, result.rows}
            {:error, _} = err -> err
          end
        end

        defp next_chunk({:error, _} = err), do: {:halt, err}
        defp next_chunk({:ok, []}), do: {:halt, :done}
        defp next_chunk({:ok, [row | rest]}), do: {[row], {:ok, rest}}

        defp cleanup(_), do: :ok
      end

  """

  alias Clickhouse.Grpc.ClickHouse.Stub
  alias Huginn.Clickhouse.{Config, Query, Result, Retry, SQL}

  @type query_opts :: [
          pool: atom(),
          database: String.t(),
          format: String.t(),
          settings: map(),
          timeout: non_neg_integer(),
          query_id: String.t(),
          retries: non_neg_integer(),
          retry_backoff: non_neg_integer()
        ]

  # =============================================================================
  # ExecuteQuery - Simple request/response
  # =============================================================================

  @doc """
  Executes a query using the `ExecuteQuery` gRPC method.

  This is a simple request/response pattern suitable for:
  - SELECT queries with reasonable result sizes
  - DDL statements (CREATE, ALTER, DROP)
  - Small INSERT statements

  ## Options

    * `:pool` - Pool name (default: configured pool)
    * `:database` - Database to use
    * `:format` - Output format (default: "TabSeparated")
    * `:settings` - ClickHouse settings map
    * `:timeout` - Query timeout in milliseconds (default: 60000)
    * `:query_id` - Custom query ID for tracking/cancellation

  ## Examples

      # Simple query
      {:ok, result} = Huginn.Clickhouse.Client.query("SELECT 1")

      # With options
      {:ok, result} = Huginn.Clickhouse.Client.query(
        "SELECT * FROM users",
        database: "mydb",
        format: "JSONEachRow",
        timeout: 30_000
      )

      # Get results as maps
      maps = Huginn.Clickhouse.Result.to_maps(result)

  """
  @spec query(String.t(), query_opts()) :: {:ok, Result.t()} | {:error, term()}
  def query(sql, opts \\ []) do
    config = get_config()
    pool = Keyword.get(opts, :pool, config.pool_name)
    query_opts = Query.with_auth(opts, config)
    query_info = Query.build(sql, query_opts)

    instrument(:query, sql, query_info.query_id, pool, fn ->
      Retry.with_retry(fn -> execute(pool, query_info, opts) end, opts)
    end)
  end

  @doc """
  Executes a query and raises on error.

  See `query/2` for options.
  """
  @spec query!(String.t(), query_opts()) :: Result.t()
  def query!(sql, opts \\ []) do
    case query(sql, opts) do
      {:ok, result} -> result
      {:error, error} -> raise "ClickHouse query error: #{inspect(error)}"
    end
  end

  @doc """
  Inserts data using the `ExecuteQuery` gRPC method.

  Suitable for small to medium inserts where data fits in a single request.
  For large inserts, use `insert_stream/3`.

  ## Options

    * `:format` - Input data format (default: "TabSeparated")
    * All options from `query/2`

  ## Examples

      # TabSeparated format
      data = "john\\t25\\njane\\t30"
      {:ok, _} = Huginn.Clickhouse.Client.insert(
        "INSERT INTO users (name, age) FORMAT TabSeparated",
        data
      )

      # JSONEachRow format
      data = ~s({"name":"john","age":25}\\n{"name":"jane","age":30})
      {:ok, _} = Huginn.Clickhouse.Client.insert(
        "INSERT INTO users FORMAT JSONEachRow",
        data
      )

  """
  @spec insert(String.t(), binary(), keyword()) :: {:ok, Result.t()} | {:error, term()}
  def insert(sql, data, opts \\ []) do
    config = get_config()
    pool = Keyword.get(opts, :pool, config.pool_name)
    query_opts = Query.with_auth(opts, config)
    query_info = Query.build_insert(sql, data, query_opts)

    instrument(:insert, sql, query_info.query_id, pool, fn ->
      Retry.with_retry(fn -> execute(pool, query_info, opts) end, opts)
    end)
  end

  # =============================================================================
  # ExecuteQueryWithStreamInput - Streaming inserts
  # =============================================================================

  @doc """
  Inserts data using the `ExecuteQueryWithStreamInput` gRPC method.

  This method streams data to ClickHouse in chunks, suitable for:
  - Large file imports
  - Continuous data ingestion
  - Memory-efficient bulk inserts

  The data stream is sent as multiple `QueryInfo` messages where:
  - First message contains the SQL query and initial data
  - Subsequent messages contain only data chunks

  ## Options

    * `:format` - Input data format (default: "TabSeparated")
    * `:chunk_size` - Bytes per chunk
    * All options from `query/2`

  ## Examples

      # Stream from a file
      File.stream!("large_data.csv", [], 65_536)
      |> Huginn.Clickhouse.Client.insert_stream("INSERT INTO logs FORMAT CSV")

      # Stream from enumerable
      data_chunks = ["row1\\tval1\\n", "row2\\tval2\\n", "row3\\tval3\\n"]
      {:ok, _} = Huginn.Clickhouse.Client.insert_stream(
        "INSERT INTO t FORMAT TabSeparated",
        data_chunks
      )

      # With Agent to track progress
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      large_data
      |> Stream.chunk_every(1000)
      |> Stream.map(fn chunk ->
        Agent.update(counter, &(&1 + length(chunk)))
        Enum.join(chunk, "\\n")
      end)
      |> Huginn.Clickhouse.Client.insert_stream("INSERT INTO t FORMAT TabSeparated")

      IO.puts("Inserted \#{Agent.get(counter, & &1)} rows")

  """
  @spec insert_stream(String.t(), Enumerable.t(), keyword()) ::
          {:ok, Result.t()} | {:error, term()}
  def insert_stream(sql, data_stream, opts \\ []) do
    config = get_config()
    pool = Keyword.get(opts, :pool, config.pool_name)
    query_opts = Query.with_auth(opts, config)

    messages = Huginn.Clickhouse.Stream.input_stream(sql, data_stream, query_opts)

    instrument(:insert_stream, sql, nil, pool, fn ->
      with {:ok, channel} <- GrpcConnectionPool.get_channel(pool) do
        # ExecuteQueryWithStreamInput is a client-streaming RPC: the stub call
        # opens the stream, each QueryInfo is pushed with `send_request/3`, and
        # a final END_STREAM frame closes it. The reply is a single Result.
        channel
        |> Stub.execute_query_with_stream_input(grpc_opts(opts))
        |> send_input_messages(messages)
        |> GRPC.Stub.recv(grpc_opts(opts))
        |> from_grpc_reply()
      end
    end)
  end

  # =============================================================================
  # ExecuteQueryWithStreamOutput - Streaming results
  # =============================================================================

  @doc """
  Executes a query using the `ExecuteQueryWithStreamOutput` gRPC method.

  Returns a lazy stream of results, suitable for:
  - Large result sets that don't fit in memory
  - Processing data as it arrives
  - Implementing pagination-like behavior

  ## Options

    * All options from `query/2`

  ## Returns

  Returns a `Stream` that yields `{:ok, Result.t()}` or `{:error, term()}` tuples.

  ## Examples

      # Basic streaming
      Huginn.Clickhouse.Client.stream_query("SELECT * FROM large_table")
      |> Enum.each(fn
        {:ok, result} -> process_chunk(result)
        {:error, error} -> Logger.error("Error: \#{inspect(error)}")
      end)

      # Collect all results
      results =
        Huginn.Clickhouse.Client.stream_query("SELECT * FROM events")
        |> Enum.reduce([], fn
          {:ok, result}, acc -> acc ++ result.rows
          {:error, _}, acc -> acc
        end)

      # Using Stream.resource pattern with early termination
      Huginn.Clickhouse.Client.stream_query("SELECT * FROM logs")
      |> Stream.flat_map(fn {:ok, r} -> r.rows; _ -> [] end)
      |> Stream.take_while(fn [ts | _] -> ts > cutoff_time end)
      |> Enum.to_list()

      # Agent accumulator pattern
      {:ok, agent} = Agent.start_link(fn -> %{rows: [], errors: 0} end)

      Huginn.Clickhouse.Client.stream_query("SELECT * FROM metrics")
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

  """
  @spec stream_query(String.t(), query_opts()) :: Enumerable.t()
  def stream_query(sql, opts \\ []) do
    config = get_config()
    pool = Keyword.get(opts, :pool, config.pool_name)
    query_opts = Query.with_auth(opts, config)
    query_info = Query.build(sql, query_opts)

    # `flat_map` over a single seed keeps initialization lazy (nothing connects
    # until the stream is enumerated) while enumerating the gRPC stream exactly
    # once. Enumerating it more than once restarts gun's decode unfold from its
    # initial empty buffer, silently dropping every message after the first in
    # each DATA frame.
    Stream.flat_map([:init], fn :init ->
      case init_stream(pool, query_info, opts) do
        {:stream, stream} -> decode_output_stream(stream)
        {:error, reason} -> [{:error, reason}]
      end
    end)
  end

  defp init_stream(pool, query_info, opts) do
    case GrpcConnectionPool.get_channel(pool) do
      {:ok, channel} ->
        case Stub.execute_query_with_stream_output(channel, query_info, grpc_opts(opts)) do
          {:ok, stream} -> {:stream, stream}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ClickHouse sets `output_format` and `output_columns` only on the first
  # Result of a streaming response, so each chunk after it must be decoded with
  # the format and columns established by that first chunk. Without this the
  # later chunks hit the unknown-format clause and collapse into one opaque row.
  defp decode_output_stream(stream) do
    Stream.transform(stream, %{format: nil, columns: []}, &decode_output_chunk/2)
  end

  defp decode_output_chunk({:error, _} = error, acc), do: {[error], acc}
  defp decode_output_chunk({:ok, grpc_result}, acc), do: decode_output_chunk(grpc_result, acc)

  defp decode_output_chunk(grpc_result, acc) do
    case Result.from_grpc(grpc_result, format: acc.format, columns: acc.columns) do
      {:ok, result} ->
        {[{:ok, result}], %{format: result.output_format, columns: result.columns}}

      {:error, _} = error ->
        {[error], acc}
    end
  end

  @doc """
  Streams individual rows from a query result.

  Convenience wrapper around `stream_query/2` that flattens results into rows.

  ## Examples

      Huginn.Clickhouse.Client.stream_rows("SELECT id, name FROM users")
      |> Stream.take(100)
      |> Enum.each(fn [id, name] -> IO.puts("\#{id}: \#{name}") end)

  """
  @spec stream_rows(String.t(), query_opts()) :: Enumerable.t(list())
  def stream_rows(sql, opts \\ []) do
    sql
    |> stream_query(opts)
    |> Stream.flat_map(fn
      {:ok, result} -> result.rows
      {:error, _} -> []
    end)
  end

  @doc """
  Streams rows as maps from a query result.

  Convenience wrapper that converts each row to a map with column names as keys.

  ## Examples

      Huginn.Clickhouse.Client.stream_maps("SELECT id, name, age FROM users")
      |> Stream.filter(fn %{"age" => age} -> age > 18 end)
      |> Enum.to_list()

  """
  @spec stream_maps(String.t(), query_opts()) :: Enumerable.t(map())
  def stream_maps(sql, opts \\ []) do
    sql
    |> stream_query(opts)
    |> Stream.flat_map(fn
      {:ok, result} -> Result.to_maps(result)
      {:error, _} -> []
    end)
  end

  # =============================================================================
  # ExecuteQueryWithStreamIO - Bidirectional streaming
  # =============================================================================

  @doc """
  Opens a bidirectional streaming connection using `ExecuteQueryWithStreamIO`.

  This is the most flexible but complex method, suitable for:
  - Interactive query sessions
  - Real-time data processing pipelines
  - Custom streaming protocols

  Returns a tuple of `{output_stream, send_function}` where:
  - `output_stream` is an enumerable of results
  - `send_function` sends QueryInfo messages to the server

  ## Examples

      # Basic bidirectional streaming
      {output, send} = Huginn.Clickhouse.Client.stream_io()

      # Send a query
      query_info = Huginn.Clickhouse.Query.build("SELECT 1")
      send.(query_info)

      # Read response
      Enum.take(output, 1)

      # Send cancellation
      send.(Huginn.Clickhouse.Query.build_cancel())

  ## Agent-based Bidirectional Example

      defmodule MyApp.StreamProcessor do
        def process do
          {output, send} = Huginn.Clickhouse.Client.stream_io()

          # Start result collector
          {:ok, results} = Agent.start_link(fn -> [] end)

          # Spawn output reader
          reader = Task.async(fn ->
            Enum.each(output, fn
              {:ok, result} ->
                Agent.update(results, &[result | &1])
              {:error, _} ->
                :skip
            end)
          end)

          # Send queries
          send.(Huginn.Clickhouse.Query.build("SELECT 1"))
          send.(Huginn.Clickhouse.Query.build("SELECT 2"))

          # Wait and get results
          Task.await(reader)
          Agent.get(results, &Enum.reverse/1)
        end
      end

  """
  @spec stream_io(query_opts()) :: {Enumerable.t(), (struct() -> :ok)} | {:error, term()}
  def stream_io(opts \\ []) do
    config = get_config()
    pool = Keyword.get(opts, :pool, config.pool_name)

    case GrpcConnectionPool.get_channel(pool) do
      {:ok, channel} ->
        # ExecuteQueryWithStreamIO is bidirectional, and the underlying gun
        # stream is owned by a single process (it receives all stream messages).
        # We therefore run the whole stream in one dedicated owner process:
        # `send_fn` forwards QueryInfo messages to it, and the output stream
        # pulls replies back. Sending and consuming can happen from any process.
        owner = spawn_link(fn -> io_owner(channel, opts) end)

        send_fn = fn query_info ->
          send(owner, {:send, query_info})
          :ok
        end

        {io_output_stream(owner), send_fn}

      {:error, _} = error ->
        error
    end
  end

  # Owner process: opens the bidi stream, pushes queued QueryInfo messages, and
  # once a consumer subscribes, half-closes the stream and forwards each reply.
  defp io_owner(channel, opts) do
    stream = Stub.execute_query_with_stream_io(channel, grpc_opts(opts))
    io_owner_loop(stream, opts)
  end

  defp io_owner_loop(stream, opts) do
    receive do
      {:send, query_info} ->
        io_owner_loop(GRPC.Stub.send_request(stream, query_info, []), opts)

      {:recv, sub} ->
        stream
        |> GRPC.Stub.end_stream()
        |> GRPC.Stub.recv(grpc_opts(opts))
        |> io_forward(sub)

      :stop ->
        :ok
    end
  end

  defp io_forward({:ok, replies}, sub), do: io_forward({:ok, replies, nil}, sub)

  defp io_forward({:ok, replies, _headers}, sub) do
    Enum.each(replies, fn reply -> send(sub, {:reply, self(), reply}) end)
    send(sub, {:done, self()})
  end

  defp io_forward({:error, _} = error, sub) do
    send(sub, {:reply, self(), error})
    send(sub, {:done, self()})
  end

  defp io_output_stream(owner) do
    Stream.resource(
      fn ->
        send(owner, {:recv, self()})
        owner
      end,
      &next_io_reply/1,
      fn owner -> send(owner, :stop) end
    )
  end

  defp next_io_reply(owner) do
    receive do
      {:reply, ^owner, {:ok, grpc_result}} -> {[Result.from_grpc(grpc_result)], owner}
      {:reply, ^owner, {:error, _} = error} -> {[error], owner}
      {:done, ^owner} -> {:halt, owner}
    end
  end

  # =============================================================================
  # Query Management
  # =============================================================================

  @doc """
  Pings the ClickHouse server to check connectivity.

  ## Examples

      case Huginn.Clickhouse.Client.ping() do
        :ok -> IO.puts("Connected!")
        {:error, reason} -> IO.puts("Failed: \#{inspect(reason)}")
      end

  """
  @spec ping(keyword()) :: :ok | {:error, term()}
  def ping(opts \\ []) do
    case query("SELECT 1", opts) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Cancels a running query by its query ID.

  Uses ClickHouse's `KILL QUERY` command to stop execution.

  ## Examples

      # Start a long query with custom ID
      query_id = "my-query-123"
      Task.async(fn ->
        Huginn.Clickhouse.Client.query(
          "SELECT sleep(60)",
          query_id: query_id
        )
      end)

      # Cancel it
      :ok = Huginn.Clickhouse.Client.cancel(query_id)

  """
  @spec cancel(String.t(), keyword()) :: :ok | {:error, term()}
  def cancel(query_id, opts \\ []) do
    sql = "KILL QUERY WHERE query_id = '#{SQL.escape(query_id)}'"

    case query(sql, opts) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Cancels all queries matching a pattern.

  > #### Trusted input only {: .warning}
  >
  > `condition` is interpolated verbatim into a `KILL QUERY WHERE` statement.
  > Never pass untrusted/user-supplied input — build the condition from trusted
  > values only. To cancel a single known query id safely, use `cancel/2`.

  ## Examples

      # Cancel all queries from a specific user
      Huginn.Clickhouse.Client.cancel_where("user = 'admin'")

      # Cancel queries running longer than 60 seconds
      Huginn.Clickhouse.Client.cancel_where("elapsed > 60")

  """
  @spec cancel_where(String.t(), keyword()) :: :ok | {:error, term()}
  def cancel_where(condition, opts \\ []) do
    sql = "KILL QUERY WHERE #{condition}"

    case query(sql, opts) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Lists currently running queries.

  ## Examples

      {:ok, result} = Huginn.Clickhouse.Client.running_queries()
      queries = Huginn.Clickhouse.Result.to_maps(result)

      Enum.each(queries, fn q ->
        IO.puts("\#{q["query_id"]}: \#{q["query"]}")
      end)

  """
  @spec running_queries(keyword()) :: {:ok, Result.t()} | {:error, term()}
  def running_queries(opts \\ []) do
    query("SELECT query_id, user, query, elapsed, memory_usage FROM system.processes", opts)
  end

  # =============================================================================
  # Private Functions
  # =============================================================================

  defp get_config do
    Config.from_env()
  end

  defp grpc_opts(opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    [timeout: timeout]
  end

  # Single request/response over `ExecuteQuery`: acquire a channel, call the
  # stub, parse the result. Shared by `query/2` and `insert/3`.
  defp execute(pool, query_info, opts) do
    with {:ok, channel} <- GrpcConnectionPool.get_channel(pool),
         {:ok, grpc_result} <- Stub.execute_query(channel, query_info, grpc_opts(opts)) do
      Result.from_grpc(grpc_result)
    end
  end

  # Wraps a request in a `:telemetry` span, emitting
  # `[:huginn, :query, :start | :stop | :exception]`. On `:stop`, row count and
  # stats are merged into the metadata when the result is a `Result` struct.
  defp instrument(method, sql, query_id, pool, fun) do
    metadata = %{method: method, sql: sql, query_id: query_id, pool: pool}

    :telemetry.span([:huginn, :query], metadata, fn ->
      result = fun.()

      extra =
        case result do
          {:ok, %Result{} = r} -> %{rows: length(r.rows), stats: r.stats}
          {:error, reason} -> %{error: reason}
          _ -> %{}
        end

      {result, Map.merge(metadata, extra)}
    end)
  end

  # Pushes each QueryInfo onto a client-streaming gRPC stream and closes it.
  #
  # One message is buffered so we always know which is last: every non-final
  # message is sent with `next_query_info: true` (ClickHouse keeps reading), and
  # the final one is sent with `next_query_info: false` plus an END_STREAM frame.
  # Buffering a single message keeps the send lazy for large inputs.
  defp send_input_messages(grpc_stream, messages) do
    case Enum.reduce(messages, {grpc_stream, :none}, &send_pending/2) do
      {grpc_stream, :none} -> GRPC.Stub.end_stream(grpc_stream)
      {grpc_stream, last} -> send_message(grpc_stream, last, false, end_stream: true)
    end
  end

  defp send_pending(message, {grpc_stream, :none}), do: {grpc_stream, message}

  defp send_pending(message, {grpc_stream, pending}) do
    {send_message(grpc_stream, pending, true, []), message}
  end

  defp send_message(grpc_stream, message, next_query_info, opts) do
    GRPC.Stub.send_request(grpc_stream, %{message | next_query_info: next_query_info}, opts)
  end

  # Normalizes a unary gRPC reply (client-streaming returns a single Result,
  # optionally with headers when `:return_headers` is set).
  defp from_grpc_reply({:ok, grpc_result}), do: Result.from_grpc(grpc_result)
  defp from_grpc_reply({:ok, grpc_result, _headers}), do: Result.from_grpc(grpc_result)
  defp from_grpc_reply({:error, _} = error), do: error
end
