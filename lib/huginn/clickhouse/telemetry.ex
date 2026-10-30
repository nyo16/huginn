defmodule Huginn.Clickhouse.Telemetry do
  @moduledoc """
  Telemetry events emitted by Huginn, and an optional default logger.

  Every request issued through `Huginn.Clickhouse.Client` is wrapped in a
  [`:telemetry`](https://hexdocs.pm/telemetry) span:

  | Event | Measurements | Metadata |
  | ----- | ------------ | -------- |
  | `[:huginn, :query, :start]` | `:system_time`, `:monotonic_time` | `:method`, `:sql`, `:query_id`, `:pool`, `:telemetry_span_context` |
  | `[:huginn, :query, :stop]` | `:duration`, `:monotonic_time` | start metadata + `:stats` and `:rows`, or `:error`, or `:halted` |
  | `[:huginn, :query, :exception]` | `:duration`, `:monotonic_time` | start metadata + `:kind`, `:reason`, `:stacktrace` |
  | `[:huginn, :query, :retry]` | `:attempt`, `:delay` | `:method`, `:sql`, `:query_id`, `:pool`, `:error` |

  `:method` is one of `:query`, `:insert`, `:insert_stream`, `:stream_query`,
  `:stream_io`. `:query_id` is the ID the query runs under on the server. It
  is generated up front unless you pass `:query_id`. Durations are in
  `:native` time units (use `System.convert_time_unit/3`).

  `:stop` metadata depends on how the request ended:

    * success - `:stats` is the server's final statistics (or `nil`) and
      `:rows` is `stats.rows`, the number of result rows (only when the
      server sent statistics)
    * failure - `:error` is the `Huginn.Error`
    * a stream the consumer stopped early - `halted: true`

  `stream_query/2` and `stream_io/3` are lazy: their `:start` is emitted when
  enumeration begins, and `:stop` (or `:exception`) when it ends, so
  `:duration` covers the whole enumeration.

  `[:huginn, :query, :retry]` fires before each retry of a `:connection`
  failure. `:attempt` is the attempt that just failed (1-based), `:delay` is
  the backoff in milliseconds.

  ## Default logger

  Attach a ready-made handler that logs each completed request:

      Huginn.attach_default_logger()
      # or with a level
      Huginn.attach_default_logger(:debug)

  Detach it with `detach_default_logger/0`.
  """

  require Logger

  @handler_id "huginn-default-logger"
  @events [
    [:huginn, :query, :stop],
    [:huginn, :query, :exception]
  ]

  @doc """
  Attaches a default `Logger` handler for Huginn query events.

  `level` is the log level used for successful queries (default: `:info`);
  failures and exceptions are always logged at `:error`. Returns `:ok`, or
  `{:error, :already_exists}` if already attached.
  """
  @spec attach_default_logger(Logger.level()) :: :ok | {:error, :already_exists}
  def attach_default_logger(level \\ :info) do
    :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, %{level: level})
  end

  @doc """
  Detaches the default logger attached by `attach_default_logger/1`.
  """
  @spec detach_default_logger() :: :ok | {:error, :not_found}
  def detach_default_logger do
    :telemetry.detach(@handler_id)
  end

  @doc false
  def handle_event([:huginn, :query, :stop], %{duration: duration}, metadata, %{level: level}) do
    ms = System.convert_time_unit(duration, :native, :millisecond)

    case metadata do
      %{error: error} ->
        Logger.error(fn ->
          "Huginn #{metadata.method} failed in #{ms}ms: #{format_error(error)} — #{trim(metadata.sql)}"
        end)

      %{halted: true} ->
        Logger.log(level, fn ->
          "Huginn #{metadata.method} stopped early after #{ms}ms — #{trim(metadata.sql)}"
        end)

      _ ->
        Logger.log(level, fn ->
          "Huginn #{metadata.method} #{rows(metadata)}in #{ms}ms — #{trim(metadata.sql)}"
        end)
    end
  end

  def handle_event([:huginn, :query, :exception], %{duration: duration}, metadata, _config) do
    ms = System.convert_time_unit(duration, :native, :millisecond)

    Logger.error(fn ->
      "Huginn #{metadata.method} raised after #{ms}ms: " <>
        "#{Exception.format(metadata.kind, metadata.reason)} — #{trim(metadata.sql)}"
    end)
  end

  defp format_error(%Huginn.Error{} = error), do: Exception.message(error)
  defp format_error(error), do: inspect(error)

  defp rows(%{rows: count}) when is_integer(count), do: "(#{count} rows) "
  defp rows(_metadata), do: ""

  defp trim(nil), do: ""

  defp trim(sql) do
    sql = String.replace(sql, ~r/\s+/, " ") |> String.trim()
    if String.length(sql) > 120, do: String.slice(sql, 0, 117) <> "...", else: sql
  end
end
