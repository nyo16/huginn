defmodule Huginn.Clickhouse.Retry do
  @moduledoc """
  Retries transport-level gRPC failures with exponential backoff.

  Only *transient* failures are retried — connection errors and a small set of
  retryable gRPC statuses (`UNAVAILABLE`, `DEADLINE_EXCEEDED`). ClickHouse query
  errors (a returned `Exception` with a non-zero code) are **not** transient and
  are returned to the caller unchanged.
  """

  require Logger

  @transient_grpc_statuses [
    # DEADLINE_EXCEEDED
    4,
    # UNAVAILABLE
    14
  ]

  # `:not_connected` is what GrpcConnectionPool.get_channel/1 returns when no
  # channel is ready — the most common transient failure on this path, and the
  # one that previously fell through to non-transient.
  @transient_reasons [
    :not_connected,
    :no_channels,
    :timeout,
    :closed,
    :unavailable,
    :econnrefused
  ]

  @doc """
  Runs `fun` and retries on transient errors.

  `fun` is a 0-arity function returning `{:ok, term}` or `{:error, reason}`.

  ## Options

    * `:retries` - maximum number of *additional* attempts after the first
      (default: `0`, i.e. no retries — preserves non-retrying behavior).
    * `:retry_backoff` - base backoff in milliseconds; the delay for attempt `n`
      is `retry_backoff * 2 ** (n - 1)` (default: `100`).

  """
  @spec with_retry((-> {:ok, term()} | {:error, term()}), keyword()) ::
          {:ok, term()} | {:error, term()}
  def with_retry(fun, opts \\ []) when is_function(fun, 0) do
    retries = Keyword.get(opts, :retries, 0)
    backoff = Keyword.get(opts, :retry_backoff, 100)
    attempt(fun, retries, backoff, 1)
  end

  @doc """
  Returns `true` when `reason` represents a transient transport failure.
  """
  @spec transient?(term()) :: boolean()
  def transient?(%{__struct__: GRPC.RPCError, status: status}),
    do: status in @transient_grpc_statuses

  def transient?(reason) when reason in @transient_reasons, do: true
  def transient?({:error, reason}), do: transient?(reason)
  def transient?(_reason), do: false

  defp attempt(fun, retries_left, backoff, attempt_no) do
    case fun.() do
      {:error, reason} when retries_left > 0 ->
        if transient?(reason) do
          delay = backoff * Integer.pow(2, attempt_no - 1)

          Logger.debug(fn ->
            "Huginn: transient error #{inspect(reason)}, retrying in #{delay}ms " <>
              "(#{retries_left} attempt(s) left)"
          end)

          Process.sleep(delay)
          attempt(fun, retries_left - 1, backoff, attempt_no + 1)
        else
          {:error, reason}
        end

      other ->
        other
    end
  end
end
