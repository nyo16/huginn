defmodule Huginn.Clickhouse.Client do
  @moduledoc """
  High-level ClickHouse gRPC client.

  Every function takes a `:pool` option naming the pool to use (default: the
  `:pool_name` of `config :huginn, :clickhouse`, else `:clickhouse_pool`). The
  pool's `Huginn.Clickhouse.Config` supplies the database and credentials;
  per-call options win. See `Huginn.Clickhouse.Pool` for starting pools.

  ## gRPC methods

  | Method | Function | Use case |
  |--------|----------|----------|
  | `ExecuteQuery` | `query/2`, `insert/3` | Queries and inserts that fit in one message |
  | `ExecuteQueryWithStreamInput` | `insert_stream/3` | Large or continuous inserts |
  | `ExecuteQueryWithStreamIO` | `stream_query/2`, `stream_io/3` | Large results; inserts that report back while running |

  The streaming functions (`insert_stream/3`, `stream_query/2`,
  `stream_io/3`) do not use the pool's connections: each call opens its own
  HTTP/2 connection to the pool's server, with the pool's TLS settings and
  credentials, and closes it when the call ends. That connection is
  flow-controlled in both directions (see "Streaming and memory"), and a slow
  consumer stalling it stalls no other query. `stream_query/2` sends its
  single `QueryInfo` over `ExecuteQueryWithStreamIO` and closes the input side
  right away, which ClickHouse handles exactly like
  `ExecuteQueryWithStreamOutput`.

  ## Errors

  Every function returns `{:error, %Huginn.Error{}}` on failure. `query!/2`
  raises it, and `stream_rows/2`/`stream_maps/2` raise it during enumeration.
  `stream_query/2` and `stream_io/3` yield it as their last element.

  ## Timeouts

  `:timeout` (milliseconds or `:infinity`) is sent to the server as the gRPC
  deadline. It defaults to `60_000` for `query/2` and `insert/3`. The streaming
  functions (`insert_stream/3`, `stream_query/2`, `stream_io/3`) default to
  `:infinity`, because a long ingest or export is not a hung one. Use
  ClickHouse settings such as `max_execution_time` to bound them server-side.
  A timed-out request returns `%Huginn.Error{reason: :timeout}`.

  ## Retries

  `:retries` (default `0`), `:retry_backoff` and `:retry_max_backoff` retry
  `%Huginn.Error{reason: :connection}` failures, see `Huginn.Clickhouse.Retry`.
  `query/2` repeats the whole request. Inserts and streams repeat only
  acquiring a connection, so nothing that reached the server is ever sent twice.

  ## Streaming and memory

  Streams are lazy and enumerate the call exactly once. Stopping early (e.g.
  `Enum.take/2`, `Enum.find/2`, an exception) cancels the call, and so does the
  consuming process exiting (it owns the connection), so ClickHouse stops the
  query. Memory is bounded in both directions:

    * **Receive side.** The connection is only read when the consumer asks
      for the next element. A consumer slower than the server makes TCP and
      HTTP/2 flow control push back, and ClickHouse waits instead of the
      result piling up in memory. At most what one socket read returns is
      buffered.
    * **Send side.** Input is pulled from the source one message (about
      `:chunk_size` bytes) at a time, and the next message is only pulled once
      the previous one fits in the server's HTTP/2 flow-control window. A
      source faster than the server waits for it.

  ## Examples

      {:ok, result} = Huginn.Clickhouse.Client.query("SELECT 1")

      {:ok, _} =
        Huginn.Clickhouse.Client.insert_stream(
          "INSERT INTO t FORMAT TabSeparated",
          [["a", 1], ["b", 2]]
        )

      "SELECT * FROM large_table"
      |> Huginn.Clickhouse.Client.stream_rows()
      |> Stream.chunk_every(10_000)
      |> Enum.each(&process_batch/1)

  """

  alias Clickhouse.Grpc.ClickHouse.Stub
  alias Huginn.Clickhouse.{Config, Query, Result, Retry, SQL, Transport}

  @unary_timeout 60_000
  @stream_timeout :infinity

  @type query_opts :: [
          pool: atom(),
          database: String.t(),
          format: String.t(),
          settings: map(),
          timeout: timeout(),
          query_id: String.t(),
          chunk_size: pos_integer(),
          retries: non_neg_integer(),
          retry_backoff: non_neg_integer(),
          retry_max_backoff: non_neg_integer()
        ]

  @typedoc "An element of `stream_query/2` and `stream_io/3`."
  @type stream_element :: {:ok, Result.t()} | {:error, Huginn.Error.t()}

  # =============================================================================
  # ExecuteQuery - Simple request/response
  # =============================================================================

  @doc """
  Executes a query using the `ExecuteQuery` gRPC method.

  The whole result is returned in one message, so this suits queries with
  bounded results, DDL, and small inserts. Use `stream_query/2` for large
  results.

  ## Options

    * `:pool` - pool name (default: `Huginn.Clickhouse.Config.default_pool/0`)
    * `:database` - database to use (default: the pool's `:database`)
    * `:format` - output format (default: `"TabSeparated"`)
    * `:settings` - map of ClickHouse settings
    * `:timeout` - deadline in milliseconds or `:infinity` (default: `60_000`)
    * `:query_id` - custom query ID, e.g. for `cancel/2`
    * `:retries`, `:retry_backoff`, `:retry_max_backoff` - retry
      `:connection` errors by repeating the whole request, see
      `Huginn.Clickhouse.Retry`. Only use them for statements that are safe
      to run twice. Inserts belong in `insert/3`, which never re-sends.

  ## Examples

      {:ok, result} = Huginn.Clickhouse.Client.query("SELECT 1")

      {:ok, result} =
        Huginn.Clickhouse.Client.query(
          "SELECT * FROM users",
          database: "mydb",
          format: "JSONEachRow",
          timeout: 30_000
        )

      maps = Huginn.Clickhouse.Result.to_maps(result)

      {:error, %Huginn.Error{reason: :server, code: 62}} =
        Huginn.Clickhouse.Client.query("SELEC 1")

  """
  @spec query(String.t(), query_opts()) :: {:ok, Result.t()} | {:error, Huginn.Error.t()}
  def query(sql, opts \\ []) do
    {pool, query_id} = target(opts)

    instrument(:query, sql, query_id, pool, fn metadata ->
      with {:ok, config} <- Config.fetch(pool) do
        query_info = Query.build(sql, query_opts(opts, config, query_id))

        Retry.with_retry(
          fn -> execute(pool, query_info, opts) end,
          opts,
          metadata
        )
      end
    end)
  end

  @doc """
  Executes a query and raises `Huginn.Error` on failure.

  See `query/2` for options.
  """
  @spec query!(String.t(), query_opts()) :: Result.t()
  def query!(sql, opts \\ []) do
    case query(sql, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Inserts `data`, an already encoded binary, using the `ExecuteQuery` gRPC
  method.

  The data format is set by the `FORMAT` clause of `sql`. For large inserts,
  or to have rows encoded for you, use `insert_stream/3`.

  ## Options

  All options from `query/2`. `:retries` only retries acquiring a channel.
  Once the request has been sent, an insert is never retried, because a
  failure after sending (e.g. a timeout) does not tell whether the server
  committed the rows. To retry inserts yourself safely, give each one an
  `insert_deduplication_token` setting (and enable deduplication on the
  table), so the server drops a repeated block.

  ## Examples

      {:ok, _} =
        Huginn.Clickhouse.Client.insert(
          "INSERT INTO users (name, age) FORMAT TabSeparated",
          "john\\t25\\njane\\t30\\n"
        )

      {:ok, _} =
        Huginn.Clickhouse.Client.insert(
          "INSERT INTO users FORMAT JSONEachRow",
          ~s({"name":"john","age":25}\\n{"name":"jane","age":30}\\n),
          settings: %{"insert_deduplication_token" => "batch-42"}
        )

  """
  @spec insert(String.t(), binary(), query_opts()) ::
          {:ok, Result.t()} | {:error, Huginn.Error.t()}
  def insert(sql, data, opts \\ []) when is_binary(data) do
    {pool, query_id} = target(opts)

    instrument(:insert, sql, query_id, pool, fn metadata ->
      with {:ok, config} <- Config.fetch(pool),
           {:ok, channel} <- checkout_with_retry(pool, opts, metadata) do
        query_info = Query.build_insert(sql, data, query_opts(opts, config, query_id))
        unary(channel, query_info, opts)
      end
    end)
  end

  # =============================================================================
  # ExecuteQueryWithStreamInput - Streaming inserts
  # =============================================================================

  @doc """
  Inserts an enumerable of rows or encoded chunks using the
  `ExecuteQueryWithStreamInput` gRPC method.

  `data` is enumerated once, lazily, and only as fast as the server ingests
  it. Elements are packed into messages of about `:chunk_size` bytes, and one
  message at a time is held in memory:

    * binaries are sent verbatim, so they must already be encoded in the
      format named by the `FORMAT` clause of `sql`
    * lists (and maps, for `JSONEachRow`) are encoded as rows according to
      `:format`, see `Huginn.Clickhouse.Stream.input_stream/3`

  If enumerating `data` raises, the RPC is cancelled (nothing is committed)
  and the exception propagates.

  ## Options

    * `:chunk_size` - target bytes of input data per message
      (default: `1_048_576`)
    * `:format` - how list/map rows are encoded (default: `"TabSeparated"`).
      It must match the `FORMAT` clause of `sql`.
    * `:timeout` - deadline in milliseconds (default: `:infinity`)
    * All other options from `query/2`. As with `insert/3`, `:retries` only
      retries acquiring a connection.

  ## Examples

      # Structured rows, encoded for you
      {:ok, _} =
        Huginn.Clickhouse.Client.insert_stream(
          "INSERT INTO t FORMAT TabSeparated",
          [["a", 1], ["b", 2]]
        )

      # A file, streamed as-is (data is the 2nd argument, so pipe through then/2)
      File.stream!("large_data.csv", 65_536)
      |> then(&Huginn.Clickhouse.Client.insert_stream("INSERT INTO logs FORMAT CSV", &1))

      # Rows produced lazily
      Stream.map(events, fn e -> %{"id" => e.id, "kind" => e.kind} end)
      |> then(
        &Huginn.Clickhouse.Client.insert_stream(
          "INSERT INTO events FORMAT JSONEachRow",
          &1,
          format: "JSONEachRow"
        )
      )

  """
  @spec insert_stream(String.t(), Enumerable.t(), query_opts()) ::
          {:ok, Result.t()} | {:error, Huginn.Error.t()}
  def insert_stream(sql, data, opts \\ []) do
    {pool, query_id} = target(opts)

    instrument(:insert_stream, sql, query_id, pool, fn metadata ->
      with {:ok, config} <- Config.fetch(pool),
           {:ok, transport} <- open_call(config, "ExecuteQueryWithStreamInput", opts, metadata) do
        messages =
          Huginn.Clickhouse.Stream.input_stream(sql, data, query_opts(opts, config, query_id))

        run_insert(transport, messages)
      end
    end)
  end

  # =============================================================================
  # Streaming results
  # =============================================================================

  @doc """
  Executes a query and returns a lazy stream of result chunks.

  The query starts when the stream is enumerated, and each enumeration runs
  it again. Each element is `{:ok, %Huginn.Clickhouse.Result{}}`, one per
  message the server sends. The rows of a chunk are decoded with the format
  and columns of the first chunk, because ClickHouse only sends them once.
  A failure is yielded as `{:error, %Huginn.Error{}}` and is always the last
  element.

  Stopping early (`Enum.take/2`, `Stream.take_while/2`, an exception) or the
  consuming process exiting cancels the call, and ClickHouse stops the query.
  The server is only read as fast as the stream is consumed, so a slow
  consumer slows the query down rather than buffering its result, see
  "Streaming and memory" in the module docs.

  ## Options

  All options from `query/2`, except that `:timeout` defaults to `:infinity`
  and `:retries` only retries acquiring a connection.

  ## Examples

      "SELECT * FROM large_table"
      |> Huginn.Clickhouse.Client.stream_query()
      |> Enum.each(fn
        {:ok, result} -> process_chunk(result.rows)
        {:error, error} -> Logger.error(Exception.message(error))
      end)

      # Stops (and cancels) the query after the first chunk with rows
      "SELECT * FROM logs"
      |> Huginn.Clickhouse.Client.stream_query()
      |> Enum.find(&match?({:ok, %{rows: [_ | _]}}, &1))

  """
  @spec stream_query(String.t(), query_opts()) :: Enumerable.t(stream_element())
  def stream_query(sql, opts \\ []) do
    {pool, query_id} = target(opts)

    output_stream(:stream_query, sql, query_id, pool, fn config, metadata ->
      with {:ok, transport} <- open_call(config, "ExecuteQueryWithStreamIO", opts, metadata) do
        query_info = Query.build(sql, query_opts(opts, config, query_id))

        transport =
          transport
          |> Transport.put_message(%{query_info | next_query_info: false})
          |> Transport.finish_input()

        {:ok, %{transport: transport, input: nil, pending: nil}}
      end
    end)
  end

  @doc """
  Streams the rows of a query result.

  A wrapper around `stream_query/2` that yields each row. Like `File.stream!/2`,
  it raises `Huginn.Error` during enumeration if the query fails, so a failure
  never looks like an empty or truncated result.

  ## Examples

      "SELECT id, name FROM users"
      |> Huginn.Clickhouse.Client.stream_rows()
      |> Stream.take(100)
      |> Enum.each(fn [id, name] -> IO.puts("\#{id}: \#{name}") end)

      # Batches of rows, without holding the whole result
      "SELECT * FROM events"
      |> Huginn.Clickhouse.Client.stream_rows()
      |> Stream.chunk_every(10_000)
      |> Enum.each(&process_batch/1)

  """
  @spec stream_rows(String.t(), query_opts()) :: Enumerable.t(Result.row())
  def stream_rows(sql, opts \\ []) do
    sql
    |> stream_query(opts)
    |> Stream.flat_map(fn
      {:ok, result} -> result.rows
      {:error, error} -> raise error
    end)
  end

  @doc """
  Streams the rows of a query result as maps keyed by column name.

  Raises `Huginn.Error` during enumeration on failure, like `stream_rows/2`.

  ## Examples

      "SELECT id, name, age FROM users"
      |> Huginn.Clickhouse.Client.stream_maps(format: "JSONEachRow")
      |> Stream.filter(fn %{"age" => age} -> age > 18 end)
      |> Enum.to_list()

  """
  @spec stream_maps(String.t(), query_opts()) :: Enumerable.t(map())
  def stream_maps(sql, opts \\ []) do
    sql
    |> stream_query(opts)
    |> Stream.flat_map(fn
      {:ok, result} -> Result.to_maps(result)
      {:error, error} -> raise error
    end)
  end

  @doc """
  Runs one query over streamed input and returns the server's replies as a
  lazy stream, using the `ExecuteQueryWithStreamIO` gRPC method.

  `input` becomes the query's input data. Elements are encoded and packed
  exactly as in `insert_stream/3`, and the pool's database and credentials
  apply. ClickHouse only reads input data in an `INSERT`, either directly or
  through the `input()` table function in `INSERT ... SELECT ... FROM input(...)`,
  so `sql` is one of those. Its `FORMAT` clause names the input format.

  Unlike `insert_stream/3`, which returns once with the final result, this
  yields every `Huginn.Clickhouse.Result` the server sends while the query
  runs: `progress` updates, server `logs` (with the `send_logs_level`
  setting), and the final `stats`. Elements and cancellation behave as in
  `stream_query/2`: a failure is the last element, and stopping early cancels
  the query.

  `input` is enumerated while the result is consumed: one message is pulled
  from it only once the previous one has fit in the server's flow-control
  window, and replies are yielded as they arrive in between. `:format`
  describes the *input* rows, as in `insert_stream/3`.

  ## Options

  Same as `insert_stream/3`.

  ## Examples

      "INSERT INTO doubled SELECT x * 2 FROM input('x UInt32') FORMAT TabSeparated"
      |> Huginn.Clickhouse.Client.stream_io(Stream.map(1..1_000_000, &[&1]),
        settings: %{"send_logs_level" => "information"}
      )
      |> Enum.each(fn
        {:ok, result} -> Enum.each(result.logs, &IO.inspect/1)
        {:error, error} -> raise error
      end)

  """
  @spec stream_io(String.t(), Enumerable.t(), query_opts()) :: Enumerable.t(stream_element())
  def stream_io(sql, input, opts \\ []) do
    {pool, query_id} = target(opts)

    output_stream(:stream_io, sql, query_id, pool, fn config, metadata ->
      with {:ok, transport} <- open_call(config, "ExecuteQueryWithStreamIO", opts, metadata) do
        messages =
          Huginn.Clickhouse.Stream.input_stream(sql, input, query_opts(opts, config, query_id))

        {:ok, %{transport: transport, input: suspend(messages), pending: nil}}
      end
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
        {:error, error} -> IO.puts("Failed: \#{Exception.message(error)}")
      end

  """
  @spec ping(query_opts()) :: :ok | {:error, Huginn.Error.t()}
  def ping(opts \\ []) do
    case query("SELECT 1", opts) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Cancels a running query by its query ID.

  Uses ClickHouse's `KILL QUERY` command, which returns once the query has
  been signalled, before it has necessarily stopped.

  ## Examples

      query_id = "my-query-123"

      Task.async(fn ->
        Huginn.Clickhouse.Client.query("SELECT sleep(3)", query_id: query_id)
      end)

      :ok = Huginn.Clickhouse.Client.cancel(query_id)

  """
  @spec cancel(String.t(), query_opts()) :: :ok | {:error, Huginn.Error.t()}
  def cancel(query_id, opts \\ []) do
    sql = "KILL QUERY WHERE query_id = '#{SQL.escape(query_id)}'"

    case query(sql, opts) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Cancels all queries matching a condition.

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
  @spec cancel_where(String.t(), query_opts()) :: :ok | {:error, Huginn.Error.t()}
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

      result
      |> Huginn.Clickhouse.Result.to_maps()
      |> Enum.each(fn q -> IO.puts("\#{q["query_id"]}: \#{q["query"]}") end)

  """
  @spec running_queries(query_opts()) :: {:ok, Result.t()} | {:error, Huginn.Error.t()}
  def running_queries(opts \\ []) do
    query("SELECT query_id, user, query, elapsed, memory_usage FROM system.processes", opts)
  end

  # =============================================================================
  # Request plumbing
  # =============================================================================

  # The pool and query id are resolved before anything else so that every
  # telemetry event, including the one for a misconfigured pool, carries them.
  defp target(opts) do
    {Keyword.get_lazy(opts, :pool, &Config.default_pool/0),
     Keyword.get_lazy(opts, :query_id, &Query.generate_query_id/0)}
  end

  defp query_opts(opts, config, query_id) do
    opts
    |> Keyword.put(:query_id, query_id)
    |> Query.with_config(config)
  end

  defp stream_timeout(opts), do: Keyword.get(opts, :timeout, @stream_timeout)

  defp checkout(pool) do
    case GrpcConnectionPool.get_channel(pool) do
      {:ok, channel} -> {:ok, channel}
      {:error, reason} -> {:error, Huginn.Error.wrap(reason)}
    end
  end

  # Acquiring a channel sends nothing to the server, so it is always safe to
  # retry, unlike the request that follows it.
  defp checkout_with_retry(pool, opts, metadata) do
    Retry.with_retry(fn -> checkout(pool) end, opts, metadata)
  end

  defp execute(pool, query_info, opts) do
    with {:ok, channel} <- checkout(pool), do: unary(channel, query_info, opts)
  end

  # One request/response over `ExecuteQuery`. Shared by `query/2` and
  # `insert/3`.
  defp unary(channel, query_info, opts) do
    timeout = Keyword.get(opts, :timeout, @unary_timeout)

    channel
    |> Stub.execute_query(query_info, timeout: timeout)
    |> from_reply()
  catch
    # The pool's connection process died (or stalled) under us.
    :exit, reason -> {:error, exit_error(reason)}
  end

  defp from_reply({:ok, grpc_result}), do: Result.from_grpc(grpc_result)
  defp from_reply({:ok, grpc_result, _headers}), do: Result.from_grpc(grpc_result)
  defp from_reply({:error, reason}), do: {:error, Huginn.Error.wrap(reason)}

  # The adapter talks to the pool's connection process with `GenServer.call`,
  # whose exit reason embeds the call arguments, i.e. the encoded request with
  # its credentials. Keep only the cause.
  defp exit_error(reason) do
    case exit_cause(reason) do
      :timeout ->
        %Huginn.Error{
          reason: :timeout,
          message: "the gRPC connection process did not reply in time",
          details: {:exit, :timeout}
        }

      cause ->
        %Huginn.Error{
          reason: :connection,
          message: "the gRPC connection process exited: #{inspect(cause)}",
          details: {:exit, cause}
        }
    end
  end

  defp exit_cause({cause, {GenServer, :call, _args}}), do: exit_cause(cause)
  defp exit_cause(cause), do: cause

  # =============================================================================
  # Streaming calls (insert_stream/3, stream_query/2, stream_io/3)
  # =============================================================================

  # Streaming calls run over a dedicated, flow-controlled connection, see
  # `Huginn.Clickhouse.Transport`. Opening one sends only the request headers,
  # so it is always safe to retry, unlike what follows.
  defp open_call(config, method, opts, metadata) do
    Retry.with_retry(
      fn -> Transport.open(config, method, stream_timeout(opts)) end,
      opts,
      metadata
    )
  end

  # Client-streaming `ExecuteQueryWithStreamInput`: send every message,
  # half-close, then read the single Result.
  defp run_insert(transport, messages) do
    transport =
      transport
      |> send_all(messages)
      |> Transport.finish_input()
      |> drain()
      |> await_end()

    {results, transport} = Transport.take(transport)
    Transport.close(transport)
    insert_reply(List.last(results), Transport.status(transport))
  end

  # A ClickHouse exception arrives as a Result, which is more specific than the
  # gRPC status that may follow it.
  defp insert_reply(nil, :done) do
    {:error,
     %Huginn.Error{
       reason: :unexpected_reply,
       message: "the server ended the insert without a Result",
       details: nil
     }}
  end

  defp insert_reply(nil, {:error, error}), do: {:error, error}

  defp insert_reply(grpc_result, status) do
    case {Result.from_grpc(grpc_result), status} do
      {{:error, _} = error, _status} -> error
      {{:ok, _} = ok, :done} -> ok
      {{:ok, _}, {:error, error}} -> {:error, error}
    end
  end

  # Sends every message, one at a time: the next one is only pulled from the
  # source once the previous one has left (see `drain/1`), so memory holds at
  # most one message whatever the speed of the source. One message is held
  # back so we know which is last: every non-final one is sent with
  # `next_query_info: true` (ClickHouse keeps reading), the last with `false`.
  defp send_all(transport, messages), do: send_loop(transport, suspend(messages), nil)

  defp send_loop(transport, next, pending) do
    case pull_input(next, transport) do
      {:ok, message, next} ->
        transport = if pending, do: send_message(transport, pending, true), else: transport

        if Transport.status(transport) == :open do
          send_loop(transport, next, message)
        else
          # The server ended the call (it failed it): stop reading the source.
          halt_input(next)
          transport
        end

      :done ->
        if pending, do: send_message(transport, pending, false), else: transport
    end
  end

  # If enumerating the caller's source raises, the call is reset (so nothing is
  # committed and the server does not wait for input forever) and the exception
  # propagates.
  defp pull_input(next, transport) do
    case next.({:cont, nil}) do
      {:suspended, message, next} -> {:ok, message, next}
      {done, _acc} when done in [:done, :halted] -> :done
    end
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__
      Transport.close(transport)
      :erlang.raise(kind, reason, stacktrace)
  end

  defp halt_input(nil), do: :ok

  defp halt_input(next) do
    next.({:halt, nil})
    :ok
  catch
    # Best effort: the call is over either way.
    _kind, _reason -> :ok
  end

  defp send_message(transport, message, next_query_info) do
    transport
    |> Transport.put_message(%{message | next_query_info: next_query_info})
    |> drain()
  end

  # Blocks until everything queued has been sent, waiting for the server to
  # open its flow-control window as it consumes the data.
  defp drain(transport) do
    transport = Transport.flush(transport)

    if Transport.sending?(transport),
      do: transport |> Transport.await() |> drain(),
      else: transport
  end

  defp await_end(transport) do
    if Transport.status(transport) == :open,
      do: transport |> Transport.await() |> await_end(),
      else: transport
  end

  # =============================================================================
  # Lazy output streams (stream_query/2, stream_io/3)
  # =============================================================================

  # `open` receives the pool config and the span metadata and returns
  # `{:ok, %{transport: t, input: continuation | nil, pending: message | nil}}`
  # or an error. It runs when enumeration starts, in the enumerating process,
  # which therefore owns the connection. The returned stream yields one element
  # per Result, ends after the first error, and cancels the call unless it was
  # read to the end.
  defp output_stream(method, sql, query_id, pool, open) do
    Stream.resource(
      fn -> start_output(method, sql, query_id, pool, open) end,
      &next_output/1,
      &finish_output/1
    )
  end

  defp start_output(method, sql, query_id, pool, open) do
    span = start_span(method, sql, query_id, pool)

    try do
      with {:ok, config} <- Config.fetch(pool), do: open.(config, span.metadata)
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__
        end_span(span, :exception, %{kind: kind, reason: reason, stacktrace: stacktrace})
        :erlang.raise(kind, reason, stacktrace)
    else
      {:ok, conn} -> output_state(span, conn, :running)
      {:error, error} -> output_state(span, nil, {:pending, error})
    end
  end

  defp output_state(span, conn, status) do
    %{span: span, conn: conn, status: status, format: nil, columns: [], stats: nil}
  end

  defp next_output(%{status: :running} = state) do
    pull_output(state)
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__

      if claim_end(state.span) do
        close_conn(state.conn)
        emit_end(state.span, :exception, %{kind: kind, reason: reason, stacktrace: stacktrace})
      end

      :erlang.raise(kind, reason, stacktrace)
  end

  defp next_output(%{status: {:pending, error}} = state),
    do: {[{:error, error}], %{state | status: {:error, error}}}

  defp next_output(state), do: {:halt, state}

  # Yields what has been received; otherwise advances the call by one step
  # (send queued data, feed one input message, or read once) and yields
  # nothing, which `Stream.resource/3` turns into another call. Only reading
  # once per step is what lets a slow consumer slow the server down.
  defp pull_output(%{conn: conn} = state) do
    {results, transport} = Transport.take(conn.transport)
    state = %{state | conn: %{conn | transport: transport}}

    case {results, Transport.status(transport)} do
      {[_ | _], _status} -> handle_results(results, state)
      {[], :open} -> {[], %{state | conn: advance(state.conn)}}
      {[], :done} -> {:halt, end_conn(state, :done)}
      {[], {:error, error}} -> {[{:error, error}], end_conn(state, {:error, error})}
    end
  end

  defp advance(%{transport: transport} = conn) do
    cond do
      Transport.sending?(transport) ->
        transport = Transport.flush(transport)

        transport =
          if Transport.sending?(transport), do: Transport.await(transport), else: transport

        %{conn | transport: transport}

      conn.input ->
        feed_input(conn)

      true ->
        %{conn | transport: Transport.await(transport)}
    end
  end

  # Pulls one input message (`stream_io/3`). The previous one is sent now that
  # we know it is not the last; at the end, the held-back one goes out as the
  # last and the request is half-closed. A raising source propagates to
  # `next_output/1`, which resets the call.
  defp feed_input(%{input: next, pending: pending, transport: transport} = conn) do
    case next.({:cont, nil}) do
      {:suspended, message, next} ->
        transport =
          if pending, do: put_input(transport, pending, true), else: transport

        %{conn | transport: transport, input: next, pending: message}

      {done, _acc} when done in [:done, :halted] ->
        transport = if pending, do: put_input(transport, pending, false), else: transport
        %{conn | transport: Transport.finish_input(transport), input: nil, pending: nil}
    end
  end

  defp put_input(transport, message, next_query_info) do
    transport
    |> Transport.put_message(%{message | next_query_info: next_query_info})
    |> Transport.flush()
  end

  defp handle_results(results, state) do
    {elements, state} =
      Enum.reduce_while(results, {[], state}, fn grpc_result, {acc, state} ->
        case Result.from_grpc(grpc_result, format: state.format, columns: state.columns) do
          # ClickHouse sets `output_format` and `output_columns` only on the
          # first Result of a stream, so later chunks are decoded with the ones
          # carried over from it.
          {:ok, result} ->
            state = %{
              state
              | format: result.output_format,
                columns: result.columns,
                stats: result.stats || state.stats
            }

            {:cont, {[{:ok, result} | acc], state}}

          # A ClickHouse exception or an undecodable chunk ends the stream.
          {:error, error} ->
            {:halt, {[{:error, error} | acc], end_conn(state, {:error, error})}}
        end
      end)

    {Enum.reverse(elements), state}
  end

  defp end_conn(state, status) do
    close_conn(state.conn)
    %{state | conn: nil, status: status}
  end

  # Runs on completion, early halt and exceptions alike.
  defp finish_output(state) do
    if claim_end(state.span) do
      close_conn(state.conn)
      emit_end(state.span, :stop, stop_metadata(state))
    end
  end

  defp stop_metadata(%{status: :done, stats: %{rows: rows} = stats}),
    do: %{rows: rows, stats: stats}

  defp stop_metadata(%{status: :done, stats: stats}), do: %{stats: stats}
  defp stop_metadata(%{status: {_, error}}), do: %{error: error}
  defp stop_metadata(%{status: :running}), do: %{halted: true}

  # Resets the call if it is still running (ClickHouse then stops the query)
  # and closes its connection.
  defp close_conn(nil), do: :ok

  defp close_conn(%{transport: transport, input: input}) do
    halt_input(input)
    Transport.close(transport)
  end

  # Pulls a lazy enumerable one element at a time.
  defp suspend(enumerable) do
    &Enumerable.reduce(enumerable, &1, fn element, _acc -> {:suspend, element} end)
  end

  # =============================================================================
  # Telemetry
  # =============================================================================

  # Wraps an eager request in a `[:huginn, :query]` span. `fun` receives the
  # span metadata (used for retry events).
  defp instrument(method, sql, query_id, pool, fun) do
    metadata = %{method: method, sql: sql, query_id: query_id, pool: pool}

    :telemetry.span([:huginn, :query], metadata, fn ->
      result = fun.(metadata)
      {result, Map.merge(metadata, result_metadata(result))}
    end)
  end

  defp result_metadata({:ok, %Result{stats: %{rows: rows} = stats}}),
    do: %{rows: rows, stats: stats}

  defp result_metadata({:ok, %Result{stats: stats}}), do: %{stats: stats}
  defp result_metadata({:error, error}), do: %{error: error}

  # A lazy stream cannot run inside `:telemetry.span/3`, which needs a function
  # that returns once. The same events are emitted by hand instead: `:start`
  # when enumeration begins, and `:stop` or `:exception` when it ends.
  defp start_span(method, sql, query_id, pool) do
    metadata = %{
      method: method,
      sql: sql,
      query_id: query_id,
      pool: pool,
      telemetry_span_context: make_ref()
    }

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:huginn, :query, :start],
      %{system_time: System.system_time(), monotonic_time: start_time},
      metadata
    )

    # `ended` makes sure exactly one of :stop/:exception is emitted: when the
    # stream raises, `Stream.resource/3` still runs `finish_output/1`, with
    # state that cannot tell it apart from an early halt.
    %{metadata: metadata, start_time: start_time, ended: :atomics.new(1, [])}
  end

  defp claim_end(%{ended: ended}), do: :atomics.compare_exchange(ended, 1, 0, 1) == :ok

  defp end_span(span, event, extra) do
    if claim_end(span), do: emit_end(span, event, extra)
  end

  defp emit_end(span, event, extra) do
    end_time = System.monotonic_time()

    :telemetry.execute(
      [:huginn, :query, event],
      %{duration: end_time - span.start_time, monotonic_time: end_time},
      Map.merge(span.metadata, extra)
    )
  end
end
