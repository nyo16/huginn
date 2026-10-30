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

  `stream_query/2` sends its single `QueryInfo` over `ExecuteQueryWithStreamIO`
  and closes the input side right away, which ClickHouse handles exactly like
  `ExecuteQueryWithStreamOutput`. It does so because grpc-elixir only returns a
  cancellable stream handle for request-streaming calls, and the handle is what
  lets an abandoned stream cancel its query.

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
  `query/2` repeats the whole request. Inserts and streams repeat only channel
  acquisition, so nothing that reached the server is ever sent twice.

  ## Streaming and memory

  Streams are lazy and enumerate the RPC exactly once. Stopping early (e.g.
  `Enum.take/2`, `Enum.find/2`, an exception) cancels the RPC, and so does the
  consuming process exiting, so ClickHouse stops the query. Two limits remain:
  they sit in grpc-elixir's gun adapter, not in Huginn.

    * **Receive side.** gun refills the HTTP/2 flow-control window on its own,
      so the server is never slowed down by a slow consumer. The result is
      buffered in memory as fast as the server produces it. Bound large
      exports with `LIMIT` or the `max_result_bytes`/`max_result_rows`
      settings.
    * **Send side.** Each input message is handed to the connection process
      without waiting for the HTTP/2 send window. A source that is faster than
      the network piles up in memory. Feed `insert_stream/3` and `stream_io/3`
      from a source that is not much faster than the server ingests.

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
  alias Huginn.Clickhouse.{Config, Query, Result, Retry, SQL}

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

  `data` is enumerated once, lazily. Elements are packed into messages of
  about `:chunk_size` bytes, so memory stays bounded by the message size
  (see "Streaming and memory" in the module docs for the send-side caveat):

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
      retries acquiring a channel.

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
           {:ok, channel} <- checkout_with_retry(pool, opts, metadata) do
        messages =
          Huginn.Clickhouse.Stream.input_stream(sql, data, query_opts(opts, config, query_id))

        stream_insert(channel, messages, stream_timeout(opts))
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
  consuming process exiting cancels the RPC, and ClickHouse stops the query
  the next time it sends a block or progress update. The receive side is not
  flow-controlled, see "Streaming and memory" in the module docs. Bound large
  exports with `LIMIT` or the `max_result_bytes` setting.

  ## Options

  All options from `query/2`, except that `:timeout` defaults to `:infinity`
  and `:retries` only retries acquiring a channel.

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

    output_stream(:stream_query, sql, query_id, pool, opts, fn channel, config ->
      query_info = Query.build(sql, query_opts(opts, config, query_id))
      open_io(channel, [query_info], stream_timeout(opts))
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

  `input` is enumerated, and fully handed to the connection, when enumeration
  of the result starts and before the first reply is read. Replies that arrive
  meanwhile are buffered. `:format` describes the *input* rows, as in
  `insert_stream/3`.

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

    output_stream(:stream_io, sql, query_id, pool, opts, fn channel, config ->
      messages =
        Huginn.Clickhouse.Stream.input_stream(sql, input, query_opts(opts, config, query_id))

      open_io(channel, messages, stream_timeout(opts))
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

  # Client-streaming `ExecuteQueryWithStreamInput`: open the stream, push every
  # message, half-close, then read the single Result.
  defp stream_insert(channel, messages, timeout) do
    grpc_opts = [timeout: timeout]

    with {:ok, grpc_stream} <-
           open_grpc_stream(fn -> Stub.execute_query_with_stream_input(channel, grpc_opts) end) do
      guard = start_guard(grpc_stream)

      try do
        with {:ok, grpc_stream} <- send_input(grpc_stream, messages) do
          grpc_stream
          |> GRPC.Stub.recv(grpc_opts)
          |> from_reply()
        end
      after
        release(guard)
      end
    end
  end

  # Bidirectional `ExecuteQueryWithStreamIO`: open the stream, push every
  # message, half-close, then hand back the lazy reply stream. Returns the
  # state `output_stream/6` pulls from.
  defp open_io(channel, messages, timeout) do
    grpc_opts = [timeout: timeout]

    with {:ok, grpc_stream} <-
           open_grpc_stream(fn -> Stub.execute_query_with_stream_io(channel, grpc_opts) end) do
      guard = start_guard(grpc_stream)

      try do
        with {:ok, grpc_stream} <- send_input(grpc_stream, messages),
             {:ok, replies} <- recv_stream(grpc_stream, grpc_opts) do
          {:ok, %{grpc: grpc_stream, guard: guard, next: suspend(replies)}}
        end
      catch
        kind, reason ->
          release(guard)
          :erlang.raise(kind, reason, __STACKTRACE__)
      else
        {:ok, _conn} = ok ->
          ok

        {:error, _} = error ->
          release(guard)
          error
      end
    end
  end

  defp open_grpc_stream(open) do
    case open.() do
      %GRPC.Client.Stream{} = grpc_stream -> {:ok, grpc_stream}
      {:error, reason} -> {:error, Huginn.Error.wrap(reason)}
    end
  catch
    :exit, reason -> {:error, exit_error(reason)}
  end

  # A failed recv has consumed the stream's terminal message, so the RPC is
  # over and there is nothing to cancel.
  defp recv_stream(grpc_stream, grpc_opts) do
    case GRPC.Stub.recv(grpc_stream, grpc_opts) do
      {:ok, replies} -> {:ok, replies}
      {:ok, replies, _headers} -> {:ok, replies}
      {:error, reason} -> {:error, Huginn.Error.wrap(reason)}
    end
  end

  defp from_reply({:ok, grpc_result}), do: Result.from_grpc(grpc_result)
  defp from_reply({:ok, grpc_result, _headers}), do: Result.from_grpc(grpc_result)
  defp from_reply({:error, reason}), do: {:error, Huginn.Error.wrap(reason)}

  # Pushes every QueryInfo onto a request stream and half-closes it.
  #
  # If the connection fails mid-send, the stream is cancelled and a
  # `:connection` error is returned. If enumerating the caller's `messages`
  # raises, the stream is cancelled (so the server does not wait for input
  # forever) and the exception propagates. Our own send failures are told apart
  # from the caller's by a tagged throw.
  defp send_input(grpc_stream, messages) do
    {:ok, push_messages(grpc_stream, messages)}
  catch
    :throw, {__MODULE__, :send_failed, reason} ->
      cancel_quietly(grpc_stream)
      {:error, exit_error(reason)}

    kind, reason ->
      cancel_quietly(grpc_stream)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  # One message is buffered so we always know which is last: every non-final
  # message is sent with `next_query_info: true` (ClickHouse keeps reading), and
  # the final one with `next_query_info: false` plus an END_STREAM frame.
  defp push_messages(grpc_stream, messages) do
    case Enum.reduce(messages, {grpc_stream, :none}, &push_pending/2) do
      {grpc_stream, :none} -> guard_send(fn -> GRPC.Stub.end_stream(grpc_stream) end)
      {grpc_stream, last} -> send_message(grpc_stream, last, false, end_stream: true)
    end
  end

  defp push_pending(message, {grpc_stream, :none}), do: {grpc_stream, message}

  defp push_pending(message, {grpc_stream, pending}) do
    {send_message(grpc_stream, pending, true, []), message}
  end

  defp send_message(grpc_stream, message, next_query_info, opts) do
    message = %{message | next_query_info: next_query_info}
    guard_send(fn -> GRPC.Stub.send_request(grpc_stream, message, opts) end)
  end

  defp guard_send(fun) do
    fun.()
  catch
    :exit, reason -> throw({__MODULE__, :send_failed, reason})
  end

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

  # Resets the HTTP/2 stream. ClickHouse then fails the query at its next send
  # or input read. Must only be called on a stream whose terminal message has
  # not been read: once the adapter's per-stream response process has
  # delivered it, it stops itself, and cancelling it again races with its exit.
  defp cancel_quietly(grpc_stream) do
    GRPC.Stub.cancel(grpc_stream)
    :ok
  catch
    # The connection is gone, and the stream with it.
    :exit, _reason -> :ok
  end

  # The gun adapter keeps a stream alive in the shared connection process, not
  # in the caller, so nothing cancels it if the caller is killed mid-stream
  # (`Stream.resource/3` cleanup and `after` blocks do not run on a kill). This
  # monitor process cancels the stream if the caller dies first. It is
  # unlinked so the caller's death does not take it down too, and it holds no
  # data and sends nothing to the caller.
  defp start_guard(grpc_stream) do
    caller = self()

    spawn(fn ->
      ref = Process.monitor(caller)

      receive do
        {:DOWN, ^ref, :process, _pid, _reason} -> cancel_quietly(grpc_stream)
        :release -> :ok
      end
    end)
  end

  defp release(guard), do: send(guard, :release)

  # =============================================================================
  # Lazy output streams (stream_query/2, stream_io/3)
  # =============================================================================

  # `open` receives the channel and pool config and returns
  # `{:ok, %{grpc: stream, guard: pid, next: continuation}}` or an error. It
  # runs when enumeration starts. The returned stream yields one element per
  # Result, ends after the first error, and cancels the RPC unless it was read
  # to the end.
  defp output_stream(method, sql, query_id, pool, opts, open) do
    Stream.resource(
      fn -> start_output(method, sql, query_id, pool, opts, open) end,
      &next_output/1,
      &finish_output/1
    )
  end

  defp start_output(method, sql, query_id, pool, opts, open) do
    span = start_span(method, sql, query_id, pool)

    try do
      with {:ok, config} <- Config.fetch(pool),
           {:ok, channel} <- checkout_with_retry(pool, opts, span.metadata) do
        open.(channel, config)
      end
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

  defp pull_output(%{conn: conn} = state) do
    case conn.next.({:cont, nil}) do
      {:suspended, reply, next} ->
        handle_reply(reply, %{state | conn: %{conn | next: next}})

      {done, _acc} when done in [:done, :halted] ->
        {:halt, %{state | conn: terminate_conn(conn), status: :done}}
    end
  end

  defp handle_reply({:ok, grpc_result}, state) do
    case Result.from_grpc(grpc_result, format: state.format, columns: state.columns) do
      # ClickHouse sets `output_format` and `output_columns` only on the first
      # Result of a stream, so later chunks are decoded with the ones carried
      # over from it.
      {:ok, result} ->
        state = %{
          state
          | format: result.output_format,
            columns: result.columns,
            stats: result.stats || state.stats
        }

        {[{:ok, result}], state}

      # A ClickHouse exception or undecodable chunk. The RPC may still be open;
      # `finish_output/1` cancels it.
      {:error, error} ->
        {[{:error, error}], %{state | status: {:error, error}}}
    end
  end

  # Trailers only appear as elements with `:return_headers`, which we never set.
  defp handle_reply({:trailers, _trailers}, state), do: {[], state}

  # The adapter ends its stream after an error, so the RPC is over.
  defp handle_reply({:error, reason}, state) do
    error = Huginn.Error.wrap(reason)
    {[{:error, error}], %{state | conn: terminate_conn(state.conn), status: {:error, error}}}
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

  # The stream reached its end on its own: just release the guard.
  defp terminate_conn(%{guard: guard}) do
    release(guard)
    nil
  end

  # The stream was abandoned before its end: cancel it, then release the guard.
  defp close_conn(nil), do: :ok

  defp close_conn(%{grpc: grpc_stream, guard: guard}) do
    cancel_quietly(grpc_stream)
    release(guard)
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
