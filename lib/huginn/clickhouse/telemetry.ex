defmodule Huginn.Clickhouse.Telemetry do
  @moduledoc """
  Telemetry events emitted by Huginn, and an optional default logger.

  Each request issued through `Huginn.Clickhouse.Client` (`query/2`, `insert/3`,
  `insert_stream/3`) is wrapped in a [`:telemetry`](https://hexdocs.pm/telemetry)
  span:

  | Event | Measurements | Metadata |
  | ----- | ------------ | -------- |
  | `[:huginn, :query, :start]` | `:system_time`, `:monotonic_time` | `:method`, `:sql`, `:query_id`, `:pool` |
  | `[:huginn, :query, :stop]` | `:duration`, `:monotonic_time` | above + `:rows`, `:stats` (or `:error`) |
  | `[:huginn, :query, :exception]` | `:duration`, `:monotonic_time` | above + `:kind`, `:reason`, `:stacktrace` |

  `:method` is one of `:query`, `:insert`, `:insert_stream`. Durations are in
  `:native` time units (use `System.convert_time_unit/3`).

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
      %{error: reason} ->
        Logger.error(fn ->
          "Huginn #{metadata.method} failed in #{ms}ms: #{inspect(reason)} — #{trim(metadata.sql)}"
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

  defp rows(%{rows: count}) when is_integer(count), do: "(#{count} rows) "
  defp rows(_metadata), do: ""

  defp trim(nil), do: ""

  defp trim(sql) do
    sql = String.replace(sql, ~r/\s+/, " ") |> String.trim()
    if String.length(sql) > 120, do: String.slice(sql, 0, 117) <> "...", else: sql
  end
end
