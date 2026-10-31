defmodule Huginn.Clickhouse.Transport do
  @moduledoc false

  # A minimal gRPC client for one streaming call over one dedicated HTTP/2
  # connection, built on Mint in passive mode. It exists for flow control,
  # which grpc-elixir's adapters do not provide in either direction:
  #
  #   * Receive: the socket is only read when the consumer asks for more
  #     (`await/1`). Until then the kernel buffer fills, TCP closes its window
  #     and the server's writes block, so a slow consumer slows the query down
  #     instead of buffering its result. Because the connection carries only
  #     this call, stalling it stalls nothing else.
  #   * Send: data is written only up to the HTTP/2 send window
  #     (`flush/1`). The caller drains one message before encoding the next,
  #     so at most one message is held in memory however fast the source is.
  #
  # The struct is owned by the process that created it (it holds the socket).
  # If that process dies the socket closes and ClickHouse cancels the query.
  #
  # Wire format (https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
  # each message is a 1-byte compressed flag, a 4-byte big-endian length and
  # the protobuf payload; the status arrives as `grpc-status`/`grpc-message`
  # trailers, or in the response headers of a trailers-only response.

  alias Clickhouse.Grpc.Result, as: GrpcResult
  alias Huginn.Clickhouse.Config

  @service "/clickhouse.grpc.ClickHouse/"
  @max_connect_timeout 30_000

  @type t :: %__MODULE__{
          conn: Mint.HTTP2.t(),
          ref: reference(),
          deadline: integer() | :infinity,
          out: binary(),
          eof?: boolean(),
          chunks: [binary()],
          buffered: non_neg_integer(),
          needed: pos_integer(),
          results: :queue.queue(GrpcResult.t()),
          grpc_status: {non_neg_integer(), String.t()} | nil,
          http_status: non_neg_integer() | nil,
          status: :open | :done | {:error, Huginn.Error.t()}
        }

  @enforce_keys [:conn, :ref, :deadline]
  defstruct [
    :conn,
    :ref,
    :deadline,
    :grpc_status,
    :http_status,
    out: <<>>,
    eof?: false,
    chunks: [],
    buffered: 0,
    needed: 5,
    results: :queue.new(),
    status: :open
  ]

  @doc """
  Connects to the pool's server and opens a call to `method` (e.g.
  `"ExecuteQueryWithStreamIO"`). Nothing but the HTTP/2 preface and the
  request headers is sent, so a failure here is always safe to retry.
  """
  @spec open(Config.t(), String.t(), timeout()) :: {:ok, t()} | {:error, Huginn.Error.t()}
  def open(%Config{} = config, method, timeout) do
    {scheme, host, port, ssl_opts} = Config.endpoint(config)
    deadline = deadline(timeout)

    connect_opts = [
      mode: :passive,
      log: false,
      client_settings: [enable_push: false],
      transport_opts: [timeout: connect_timeout(deadline), keepalive: true] ++ ssl_opts
    ]

    with {:ok, conn} <- connect(scheme, host, port, connect_opts) do
      case Mint.HTTP2.request(conn, "POST", @service <> method, headers(timeout), :stream) do
        {:ok, conn, ref} ->
          {:ok, %__MODULE__{conn: conn, ref: ref, deadline: deadline}}

        {:error, conn, reason} ->
          Mint.HTTP2.close(conn)
          {:error, connection_error("could not start the gRPC call", reason)}
      end
    end
  end

  @doc """
  Queues one protobuf message for sending. Only one message may be queued at a
  time: drain it with `flush/1`/`await/1` first, which is what bounds memory.
  """
  @spec put_message(t(), struct()) :: t()
  def put_message(%__MODULE__{out: <<>>, eof?: false} = t, message) do
    payload = Protobuf.encode_to_iodata(message)
    frame = IO.iodata_to_binary([<<0, IO.iodata_length(payload)::32>> | payload])
    %{t | out: frame}
  end

  @doc "Half-closes the request once the queued data has been sent."
  @spec finish_input(t()) :: t()
  def finish_input(%__MODULE__{} = t), do: flush(%{t | eof?: true})

  @doc "True while queued data (or the end-of-input marker) is still unsent."
  @spec sending?(t()) :: boolean()
  def sending?(%__MODULE__{status: :open, out: out, eof?: eof?}), do: out != <<>> or eof?
  def sending?(%__MODULE__{}), do: false

  @doc """
  Sends as much of the queued data as the HTTP/2 send window allows, without
  blocking. Once the call has ended (the server finished or failed it), queued
  data is dropped: there is nobody left to read it.
  """
  @spec flush(t()) :: t()
  def flush(%__MODULE__{status: :open, out: <<>>, eof?: true} = t) do
    case Mint.HTTP2.stream_request_body(t.conn, t.ref, :eof) do
      {:ok, conn} -> %{t | conn: conn, eof?: false}
      {:error, conn, reason} -> fail(%{t | conn: conn}, send_error(reason))
    end
  end

  def flush(%__MODULE__{status: :open, out: <<>>} = t), do: t

  def flush(%__MODULE__{status: :open, out: out} = t) do
    case min(window(t, {:request, t.ref}), window(t, :connection)) do
      size when size <= 0 ->
        t

      size ->
        size = min(size, byte_size(out))
        chunk = binary_part(out, 0, size)

        case Mint.HTTP2.stream_request_body(t.conn, t.ref, chunk) do
          {:ok, conn} ->
            flush(%{t | conn: conn, out: binary_part(out, size, byte_size(out) - size)})

          {:error, conn, reason} ->
            fail(%{t | conn: conn}, send_error(reason))
        end
    end
  end

  def flush(%__MODULE__{} = t), do: %{t | out: <<>>, eof?: false}

  @doc """
  Blocks until the server sends something (data, window updates, the status),
  the deadline passes or the connection fails, then processes it. Decoded
  results are queued for `take/1`.
  """
  @spec await(t()) :: t()
  def await(%__MODULE__{status: :open} = t) do
    case remaining(t.deadline) do
      0 ->
        fail(t, timeout_error())

      timeout ->
        case Mint.HTTP2.recv(t.conn, 0, timeout) do
          {:ok, conn, responses} ->
            handle_responses(%{t | conn: conn}, responses)

          {:error, conn, %Mint.TransportError{reason: :timeout}, responses} ->
            t = handle_responses(%{t | conn: conn}, responses)
            if t.status == :open, do: fail(t, timeout_error()), else: t

          {:error, conn, reason, responses} ->
            t = handle_responses(%{t | conn: conn}, responses)

            if t.status == :open,
              do: fail(t, connection_error("the connection failed", reason)),
              else: t
        end
    end
  end

  def await(%__MODULE__{} = t), do: t

  @doc "Removes and returns the decoded results received so far."
  @spec take(t()) :: {[GrpcResult.t()], t()}
  def take(%__MODULE__{results: results} = t),
    do: {:queue.to_list(results), %{t | results: :queue.new()}}

  @doc "`:open` while the call runs, then `:done` or `{:error, error}`."
  @spec status(t()) :: :open | :done | {:error, Huginn.Error.t()}
  def status(%__MODULE__{status: status}), do: status

  @doc """
  Ends the call and closes the connection. A call that is still running is
  reset with `CANCEL` first, so ClickHouse stops the query.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{conn: conn, ref: ref, status: status}) do
    conn =
      case status do
        :open ->
          case Mint.HTTP2.cancel_request(conn, ref) do
            {:ok, conn} -> conn
            {:error, conn, _reason} -> conn
          end

        _ ->
          conn
      end

    Mint.HTTP2.close(conn)
    :ok
  catch
    # Cleanup must not fail: Mint throws if the socket is already gone while it
    # sends GOAWAY. The socket is closed either way.
    :throw, _reason -> :ok
  end

  # -- Connecting ---------------------------------------------------------------

  defp connect(scheme, host, port, opts) do
    case Mint.HTTP2.connect(scheme, host, port, opts) do
      {:ok, conn} ->
        {:ok, conn}

      {:error, reason} ->
        {:error, connection_error("could not connect to #{host}:#{port}", reason)}
    end
  end

  defp headers(timeout) do
    [
      {"content-type", "application/grpc+proto"},
      {"te", "trailers"},
      {"user-agent", "huginn"}
    ] ++ timeout_header(timeout)
  end

  # gRPC timeouts are at most 8 digits plus a unit.
  defp timeout_header(:infinity), do: []
  defp timeout_header(ms) when ms <= 99_999_999, do: [{"grpc-timeout", "#{ms}m"}]
  defp timeout_header(ms), do: [{"grpc-timeout", "#{min(div(ms, 1000), 99_999_999)}S"}]

  defp deadline(:infinity), do: :infinity
  defp deadline(ms) when is_integer(ms) and ms >= 0, do: System.monotonic_time(:millisecond) + ms

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp connect_timeout(deadline) do
    case remaining(deadline) do
      :infinity -> @max_connect_timeout
      ms -> min(max(ms, 1), @max_connect_timeout)
    end
  end

  defp window(t, which), do: Mint.HTTP2.get_window_size(t.conn, which)

  # -- Receiving ----------------------------------------------------------------

  defp handle_responses(t, responses), do: Enum.reduce(responses, t, &handle_response/2)

  # Everything after the call ended (e.g. a late reset) is irrelevant.
  defp handle_response(_response, %{status: status} = t) when status != :open, do: t

  defp handle_response({:status, ref, status}, %{ref: ref} = t), do: %{t | http_status: status}

  defp handle_response({:headers, ref, headers}, %{ref: ref} = t) do
    case List.keyfind(headers, "grpc-status", 0) do
      {_, code} ->
        message =
          case List.keyfind(headers, "grpc-message", 0) do
            {_, message} -> URI.decode(message)
            nil -> ""
          end

        %{t | grpc_status: {parse_status(code), message}}

      nil ->
        t
    end
  end

  defp handle_response({:data, ref, data}, %{ref: ref} = t) do
    t = %{t | chunks: [data | t.chunks], buffered: t.buffered + byte_size(data)}
    if t.buffered >= t.needed, do: decode_messages(t), else: t
  end

  defp handle_response({:done, ref}, %{ref: ref} = t) do
    case {t.grpc_status, t.http_status} do
      {{0, _}, _} ->
        %{t | status: :done}

      {{code, message}, _} ->
        %{t | status: {:error, Huginn.Error.wrap(%GRPC.RPCError{status: code, message: message})}}

      {nil, status} ->
        %{
          t
          | status:
              {:error,
               %Huginn.Error{
                 reason: :grpc,
                 message: "the server ended the call without a gRPC status (HTTP #{status})",
                 details: {:http_status, status}
               }}
        }
    end
  end

  defp handle_response({:error, ref, reason}, %{ref: ref} = t),
    do: %{t | status: {:error, connection_error("the server reset the call", reason)}}

  # PING acks, responses for other refs, etc.
  defp handle_response(_response, t), do: t

  # A message can be far larger than one socket read (ClickHouse sends about
  # one block per Result). Received data is kept as a list of chunks and only
  # joined once `needed` bytes (the next complete message, per its length
  # prefix) have arrived, so receiving a message costs O(its size) rather than
  # re-copying a growing buffer on every read.
  defp decode_messages(t) do
    buffer = t.chunks |> Enum.reverse() |> IO.iodata_to_binary()
    decode_buffer(%{t | chunks: [], buffered: 0}, buffer)
  end

  defp decode_buffer(t, <<0, size::32, payload::binary-size(size), rest::binary>>) do
    case decode_result(payload) do
      {:ok, result} -> decode_buffer(%{t | results: :queue.in(result, t.results)}, rest)
      {:error, error} -> fail(t, error)
    end
  end

  # We never advertise `grpc-accept-encoding`, so a compressed message is a
  # protocol violation.
  defp decode_buffer(t, <<1, _::binary>>) do
    fail(t, %Huginn.Error{
      reason: :decode,
      message: "the server sent a compressed gRPC message",
      details: :compressed_message
    })
  end

  defp decode_buffer(t, <<>>), do: %{t | needed: 5}

  # An incomplete message: keep it (copied, so it does not pin the joined
  # buffer of the messages already decoded) and wait for the rest.
  defp decode_buffer(t, rest) do
    needed =
      case rest do
        <<_flag, size::32, _::binary>> -> size + 5
        _ -> 5
      end

    %{t | chunks: [:binary.copy(rest)], buffered: byte_size(rest), needed: needed}
  end

  defp decode_result(payload) do
    {:ok, GrpcResult.decode(payload)}
  rescue
    error in Protobuf.DecodeError ->
      {:error,
       %Huginn.Error{
         reason: :decode,
         message: "could not decode a Result message: #{Exception.message(error)}",
         details: error
       }}
  end

  defp parse_status(code) do
    case Integer.parse(code) do
      {status, ""} -> status
      _ -> 2
    end
  end

  # -- Errors -------------------------------------------------------------------

  # A failure on our side ends the call; reset it so the server stops too.
  defp fail(t, error) do
    conn =
      case Mint.HTTP2.cancel_request(t.conn, t.ref) do
        {:ok, conn} -> conn
        {:error, conn, _reason} -> conn
      end

    %{t | conn: conn, out: <<>>, eof?: false, status: {:error, error}}
  end

  defp send_error(reason), do: connection_error("could not send to the server", reason)

  defp timeout_error do
    %Huginn.Error{reason: :timeout, message: "the call deadline expired", details: :timeout}
  end

  defp connection_error(prefix, reason) do
    %Huginn.Error{
      reason: :connection,
      message: "#{prefix}: #{describe(reason)}",
      details: reason
    }
  end

  defp describe(%{__exception__: true} = error), do: Exception.message(error)
  defp describe(reason), do: inspect(reason)
end
