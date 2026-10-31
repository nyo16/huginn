defmodule Huginn.Error do
  @moduledoc """
  The single error type returned (and raised) by Huginn.

  Every public function returns `{:error, %Huginn.Error{}}` on failure, and the
  bang/streaming variants raise it. Match on `:reason` to branch:

  | `:reason` | Meaning | `:code` |
  | --------- | ------- | ------- |
  | `:server` | ClickHouse rejected or failed the query | ClickHouse error code |
  | `:connection` | No usable connection (pool not ready, refused, unavailable) | gRPC status, if any |
  | `:timeout` | The request deadline expired | gRPC status, if any |
  | `:grpc` | Any other gRPC-level failure | gRPC status |
  | `:decode` | The server output could not be parsed in the requested format | `nil` |
  | `:unexpected_reply` | The server replied with something that is not a `Result` | `nil` |
  | `:not_configured` | No pool is configured under the requested name | `nil` |

  `:details` carries the original term (the ClickHouse exception, the
  `GRPC.RPCError`, the decoder error, ...) for logging and debugging.
  """

  @type reason ::
          :server
          | :connection
          | :timeout
          | :grpc
          | :decode
          | :unexpected_reply
          | :not_configured

  @type t :: %__MODULE__{
          reason: reason(),
          message: String.t(),
          code: integer() | nil,
          details: term()
        }

  defexception [:reason, :message, :code, :details]

  # gRPC status codes, see https://grpc.io/docs/guides/status-codes/
  @grpc_deadline_exceeded 4
  @grpc_unavailable 14

  @connection_reasons [:not_connected, :no_channels, :closed, :unavailable, :econnrefused]

  @doc """
  Normalizes any failure term produced by the transport or the server into a
  `Huginn.Error`. An existing `Huginn.Error` is returned unchanged.
  """
  @spec wrap(term()) :: t()
  def wrap(%__MODULE__{} = error), do: error

  def wrap(%Clickhouse.Grpc.Exception{} = exception) do
    %__MODULE__{
      reason: :server,
      code: exception.code,
      message:
        "ClickHouse error #{exception.code} (#{exception.name}): #{exception.display_text}",
      details: exception
    }
  end

  def wrap(%GRPC.RPCError{status: @grpc_deadline_exceeded} = error),
    do: grpc(:timeout, error)

  def wrap(%GRPC.RPCError{status: @grpc_unavailable} = error),
    do: grpc(:connection, error)

  def wrap(%GRPC.RPCError{} = error), do: grpc(:grpc, error)

  def wrap(:timeout),
    do: %__MODULE__{reason: :timeout, message: "request timed out", details: :timeout}

  def wrap(reason) when reason in @connection_reasons do
    %__MODULE__{
      reason: :connection,
      message: "no usable connection: #{inspect(reason)}",
      details: reason
    }
  end

  def wrap({:error, reason}), do: wrap(reason)

  def wrap(other) do
    %__MODULE__{reason: :grpc, message: "request failed: #{inspect(other)}", details: other}
  end

  defp grpc(reason, %GRPC.RPCError{status: status, message: message} = error) do
    %__MODULE__{
      reason: reason,
      code: status,
      message: "gRPC status #{status}: #{message}",
      details: error
    }
  end
end
