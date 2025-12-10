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

  alias Huginn.Clickhouse.{Config, Query, Result}
  alias Clickhouse.Grpc.ClickHouse.Stub

  @type query_opts :: [
          pool: atom(),
          database: String.t(),
          format: String.t(),
          settings: map(),
          timeout: non_neg_integer(),
          query_id: String.t()
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
    pool = Keyword.get(opts, :pool, pool_name())
    config = get_config()
    query_opts = Query.with_auth(opts, config)
    query_info = Query.build(sql, query_opts)

    with {:ok, channel} <- GrpcConnectionPool.get_channel(pool),
         {:ok, grpc_result} <- Stub.execute_query(channel, query_info, grpc_opts(opts)) do
      Result.from_grpc(grpc_result)
    end
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
    pool = Keyword.get(opts, :pool, pool_name())
    config = get_config()
    query_opts = Query.with_auth(opts, config)
    query_info = Query.build_insert(sql, data, query_opts)

    with {:ok, channel} <- GrpcConnectionPool.get_channel(pool),
         {:ok, grpc_result} <- Stub.execute_query(channel, query_info, grpc_opts(opts)) do
      Result.from_grpc(grpc_result)
    end
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
  @spec insert_stream(String.t(), Enumerable.t(), keyword()) :: {:ok, Result.t()} | {:error, term()}
  def insert_stream(sql, data_stream, opts \\ []) do
    pool = Keyword.get(opts, :pool, pool_name())
    config = get_config()
    query_opts = Query.with_auth(opts, config)

    with {:ok, channel} <- GrpcConnectionPool.get_channel(pool) do
      input_stream = build_insert_stream(sql, data_stream, query_opts)

      case Stub.execute_query_with_stream_input(channel, input_stream) do
        {:ok, grpc_result} -> Result.from_grpc(grpc_result)
        {:error, _} = error -> error
      end
    end
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
    pool = Keyword.get(opts, :pool, pool_name())
    config = get_config()
    query_opts = Query.with_auth(opts, config)
    query_info = Query.build(sql, query_opts)

    Stream.resource(
      fn -> init_stream(pool, query_info, opts) end,
      &next_stream_chunk/1,
      fn _ -> :ok end
    )
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

  defp next_stream_chunk({:error, reason}) do
    {:halt, {:error, reason}}
  end

  defp next_stream_chunk({:stream, stream}) do
    case Enum.take(stream, 1) do
      [] ->
        {:halt, :done}

      [{:ok, grpc_result}] ->
        case Result.from_grpc(grpc_result) do
          {:ok, result} -> {[{:ok, result}], {:stream, stream}}
          {:error, _} = error -> {[error], {:stream, stream}}
        end

      [{:error, _} = error] ->
        {[error], {:stream, stream}}

      [grpc_result] ->
        case Result.from_grpc(grpc_result) do
          {:ok, result} -> {[{:ok, result}], {:stream, stream}}
          {:error, _} = error -> {[error], {:stream, stream}}
        end
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
  @spec stream_io(query_opts()) :: {Enumerable.t(), (Clickhouse.Grpc.QueryInfo.t() -> :ok)}
  def stream_io(opts \\ []) do
    pool = Keyword.get(opts, :pool, pool_name())

    {:ok, channel} = GrpcConnectionPool.get_channel(pool)

    {input_stream, send_fn} = create_send_stream()

    output_stream =
      Stub.execute_query_with_stream_io(channel, input_stream)
      |> parse_output_stream()

    {output_stream, send_fn}
  end

  defp create_send_stream do
    {:ok, agent} = Agent.start_link(fn -> {:queue.new(), nil} end)

    send_fn = fn query_info ->
      Agent.update(agent, fn {queue, waiting} ->
        case waiting do
          nil ->
            {:queue.in(query_info, queue), nil}

          pid ->
            send(pid, {:item, query_info})
            {queue, nil}
        end
      end)

      :ok
    end

    stream =
      Stream.resource(
        fn -> agent end,
        fn agent ->
          result =
            Agent.get_and_update(agent, fn {queue, _} ->
              case :queue.out(queue) do
                {{:value, item}, new_queue} ->
                  {item, {new_queue, nil}}

                {:empty, queue} ->
                  {nil, {queue, self()}}
              end
            end)

          case result do
            nil ->
              receive do
                {:item, item} -> {[item], agent}
              after
                100 -> {[], agent}
              end

            item ->
              {[item], agent}
          end
        end,
        fn agent -> Agent.stop(agent) end
      )

    {stream, send_fn}
  end

  defp parse_output_stream(stream) do
    Stream.map(stream, fn
      {:ok, grpc_result} -> Result.from_grpc(grpc_result)
      {:error, _} = error -> error
    end)
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
    sql = "KILL QUERY WHERE query_id = '#{escape_string(query_id)}'"

    case query(sql, opts) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Cancels all queries matching a pattern.

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

  defp pool_name do
    config = get_config()
    config.pool_name
  end

  defp grpc_opts(opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    [timeout: timeout]
  end

  defp build_insert_stream(sql, data_stream, opts) do
    data_stream
    |> Stream.with_index()
    |> Stream.map(fn {data, index} ->
      if index == 0 do
        Query.build_insert(sql, data, opts)
      else
        Query.build_continuation(data, has_more: true)
      end
    end)
  end

  defp escape_string(str) do
    String.replace(str, "'", "\\'")
  end
end
