defmodule Huginn.Clickhouse.Retry do
  @moduledoc """
  Retries connection failures with capped, jittered exponential backoff.

  Only `%Huginn.Error{reason: :connection}` is retried: no usable channel, a
  dead connection process, or gRPC `UNAVAILABLE`. Everything else is returned
  unchanged on the first failure:

    * ClickHouse errors (`:server`) are deterministic, so retrying cannot help.
    * Deadlines (`:timeout`) are not retried either. The server may still be
      executing (or may already have committed) the request, so repeating it
      could run a query twice or duplicate an insert.

  `Huginn.Clickhouse.Client` decides *what* is retried: `query/2` repeats the
  whole request, while inserts and streams only repeat channel acquisition,
  i.e. nothing that has reached the server.

  Each retry emits a `[:huginn, :query, :retry]` telemetry event, see
  `Huginn.Clickhouse.Telemetry`.
  """

  require Logger

  @default_backoff 100
  @default_max_backoff 5_000

  @doc """
  Runs `fun` and retries it while it fails with a transient error.

  `fun` is a 0-arity function returning `{:ok, term}` or
  `{:error, %Huginn.Error{}}`. `metadata` is merged into the metadata of the
  `[:huginn, :query, :retry]` event emitted before each retry.

  ## Options

    * `:retries` - maximum number of *additional* attempts after the first
      (default: `0`, i.e. no retries).
    * `:retry_backoff` - base delay in milliseconds (default: `#{@default_backoff}`).
    * `:retry_max_backoff` - upper bound for a single delay in milliseconds
      (default: `#{@default_max_backoff}`).

  See `delay/2` for how the delay before each retry is computed.
  """
  @spec with_retry((-> {:ok, term()} | {:error, term()}), keyword(), map()) ::
          {:ok, term()} | {:error, term()}
  def with_retry(fun, opts \\ [], metadata \\ %{}) when is_function(fun, 0) do
    attempt(fun, Keyword.get(opts, :retries, 0), opts, metadata, 1)
  end

  @doc """
  Returns `true` when `error` is worth retrying, i.e. it is a
  `%Huginn.Error{reason: :connection}`.
  """
  @spec transient?(term()) :: boolean()
  def transient?(%Huginn.Error{reason: :connection}), do: true
  def transient?(_error), do: false

  @doc """
  Returns the delay in milliseconds to wait after failed attempt `attempt`
  (1-based).

  The delay is drawn uniformly from `1..cap` ("full jitter"), where
  `cap = min(retry_backoff * 2 ** (attempt - 1), retry_max_backoff)`. Jitter
  keeps many clients that failed together from retrying in lockstep. Returns
  `0` when the cap is `0`.
  """
  @spec delay(pos_integer(), keyword()) :: non_neg_integer()
  def delay(attempt, opts \\ []) when is_integer(attempt) and attempt > 0 do
    base = Keyword.get(opts, :retry_backoff, @default_backoff)
    max_backoff = Keyword.get(opts, :retry_max_backoff, @default_max_backoff)

    case min(base * Integer.pow(2, attempt - 1), max_backoff) do
      cap when cap > 0 -> :rand.uniform(cap)
      _cap -> 0
    end
  end

  defp attempt(fun, retries_left, opts, metadata, attempt_no) do
    case fun.() do
      {:error, error} when retries_left > 0 ->
        if transient?(error) do
          delay = delay(attempt_no, opts)

          :telemetry.execute(
            [:huginn, :query, :retry],
            %{attempt: attempt_no, delay: delay},
            Map.put(metadata, :error, error)
          )

          Logger.debug(fn ->
            "Huginn: #{Exception.message(error)}, retrying in #{delay}ms " <>
              "(#{retries_left} attempt(s) left)"
          end)

          Process.sleep(delay)
          attempt(fun, retries_left - 1, opts, metadata, attempt_no + 1)
        else
          {:error, error}
        end

      other ->
        other
    end
  end
end
