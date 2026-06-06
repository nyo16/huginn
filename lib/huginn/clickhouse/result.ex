defmodule Huginn.Clickhouse.Result do
  @moduledoc """
  Parses and transforms ClickHouse gRPC Result messages.
  """

  alias Clickhouse.Grpc.{Exception, LogEntry, NameAndType, Progress, Stats}
  alias Clickhouse.Grpc.Result, as: GrpcResult

  @type column :: %{name: String.t(), type: String.t()}

  @type t :: %__MODULE__{
          query_id: String.t(),
          rows: list(list(String.t())),
          columns: list(column()),
          output_format: String.t(),
          stats: stats() | nil,
          progress: progress() | nil,
          logs: list(log_entry()),
          time_zone: String.t()
        }

  @type stats :: %{
          rows: non_neg_integer(),
          blocks: non_neg_integer(),
          allocated_bytes: non_neg_integer(),
          rows_before_limit: non_neg_integer()
        }

  @type progress :: %{
          read_rows: non_neg_integer(),
          read_bytes: non_neg_integer(),
          total_rows_to_read: non_neg_integer(),
          written_rows: non_neg_integer(),
          written_bytes: non_neg_integer()
        }

  @type log_entry :: %{
          time: non_neg_integer(),
          level: atom(),
          source: String.t(),
          text: String.t()
        }

  defstruct [
    :query_id,
    :output_format,
    :stats,
    :progress,
    :time_zone,
    rows: [],
    columns: [],
    logs: []
  ]

  @doc """
  Converts a gRPC Result message to a Huginn.Clickhouse.Result struct.
  """
  @spec from_grpc(struct()) :: {:ok, t()} | {:error, struct()}
  def from_grpc(%GrpcResult{exception: %Exception{code: code} = exception})
      when code != 0 do
    {:error, exception}
  end

  def from_grpc(%GrpcResult{} = grpc_result) do
    columns = parse_columns(grpc_result.output_columns)

    result = %__MODULE__{
      query_id: grpc_result.query_id,
      output_format: grpc_result.output_format,
      time_zone: grpc_result.time_zone,
      columns: columns,
      rows: parse_output(grpc_result.output, grpc_result.output_format, columns),
      stats: parse_stats(grpc_result.stats),
      progress: parse_progress(grpc_result.progress),
      logs: parse_logs(grpc_result.logs)
    }

    {:ok, result}
  end

  @doc """
  Converts rows to a list of maps using column names as keys.
  """
  @spec to_maps(t()) :: list(map())
  def to_maps(%__MODULE__{rows: rows, columns: columns}) do
    column_names = Enum.map(columns, & &1.name)

    Enum.map(rows, fn row ->
      column_names
      |> Enum.zip(row)
      |> Map.new()
    end)
  end

  @doc """
  Parses raw output bytes based on format.

  `columns` is the list of `t:column/0` maps parsed from the gRPC result. It is
  only consulted for `JSONEachRow`, where JSON object key order is not
  guaranteed to match the column order — values are extracted in column order so
  rows line up with `columns`. When `columns` is empty (the default, and the
  case for the 2-arity form), `JSONEachRow` falls back to the order returned by
  `Jason`.
  """
  @spec parse_output(binary() | nil, String.t()) :: list(list(String.t()))
  @spec parse_output(binary() | nil, String.t(), list(column())) :: list(list(String.t()))
  def parse_output(output, format, columns \\ [])
  def parse_output(<<>>, _format, _columns), do: []
  def parse_output(nil, _format, _columns), do: []

  def parse_output(output, format, _columns) when format in ["TabSeparated", "TSV"] do
    output
    |> String.trim_trailing("\n")
    |> String.split("\n")
    |> Enum.map(&String.split(&1, "\t"))
  end

  def parse_output(output, format, _columns) when format in ["CSV", "CSVWithNames"] do
    output
    |> String.trim_trailing("\n")
    |> String.split("\n")
    |> Enum.map(&parse_csv_row/1)
  end

  def parse_output(output, "JSONEachRow", columns) do
    column_names = Enum.map(columns, & &1.name)

    output
    |> String.trim_trailing("\n")
    |> String.split("\n")
    |> Enum.map(&Jason.decode!/1)
    |> Enum.map(fn decoded ->
      case column_names do
        [] -> Map.values(decoded)
        names -> Enum.map(names, &Map.get(decoded, &1))
      end
    end)
  end

  def parse_output(output, _format, _columns) do
    # Return raw binary as single row for unknown formats
    [[output]]
  end

  @doc """
  Merges multiple results from streaming responses.
  """
  @spec merge(list(t())) :: t()
  def merge([]), do: %__MODULE__{}
  def merge([single]), do: single

  def merge(results) do
    first = List.first(results)
    last = List.last(results)

    %__MODULE__{
      query_id: first.query_id,
      output_format: first.output_format,
      time_zone: first.time_zone,
      columns: first.columns,
      rows: Enum.flat_map(results, & &1.rows),
      stats: last.stats,
      progress: last.progress,
      logs: Enum.flat_map(results, & &1.logs)
    }
  end

  defp parse_columns(nil), do: []
  defp parse_columns([]), do: []

  defp parse_columns(columns) do
    Enum.map(columns, fn %NameAndType{name: name, type: type} ->
      %{name: name, type: type}
    end)
  end

  defp parse_stats(nil), do: nil

  defp parse_stats(%Stats{} = stats) do
    %{
      rows: stats.rows,
      blocks: stats.blocks,
      allocated_bytes: stats.allocated_bytes,
      rows_before_limit: stats.rows_before_limit
    }
  end

  defp parse_progress(nil), do: nil

  defp parse_progress(%Progress{} = progress) do
    %{
      read_rows: progress.read_rows,
      read_bytes: progress.read_bytes,
      total_rows_to_read: progress.total_rows_to_read,
      written_rows: progress.written_rows,
      written_bytes: progress.written_bytes
    }
  end

  defp parse_logs(nil), do: []
  defp parse_logs([]), do: []

  defp parse_logs(logs) do
    Enum.map(logs, fn %LogEntry{} = log ->
      %{
        time: log.time,
        level: log_level_to_atom(log.level),
        source: log.source,
        text: log.text
      }
    end)
  end

  defp log_level_to_atom(:LOG_NONE), do: :none
  defp log_level_to_atom(:LOG_FATAL), do: :fatal
  defp log_level_to_atom(:LOG_CRITICAL), do: :critical
  defp log_level_to_atom(:LOG_ERROR), do: :error
  defp log_level_to_atom(:LOG_WARNING), do: :warning
  defp log_level_to_atom(:LOG_NOTICE), do: :notice
  defp log_level_to_atom(:LOG_INFORMATION), do: :info
  defp log_level_to_atom(:LOG_DEBUG), do: :debug
  defp log_level_to_atom(:LOG_TRACE), do: :trace
  defp log_level_to_atom(_), do: :unknown

  # Parses a single CSV line, honoring double-quoted fields. Commas, quotes and
  # newlines inside a quoted field are preserved, and a doubled quote ("") inside
  # a quoted field decodes to a single quote. Mirrors the writer in
  # `Huginn.Clickhouse.Stream.encode_csv_field/1`.
  defp parse_csv_row(row), do: parse_csv_fields(row, :start, "", [])

  # Outside a field, deciding whether it's quoted.
  defp parse_csv_fields("", :start, _acc, fields), do: Enum.reverse(["" | fields])

  defp parse_csv_fields(<<?", rest::binary>>, :start, _acc, fields),
    do: parse_csv_fields(rest, :quoted, "", fields)

  defp parse_csv_fields(rest, :start, _acc, fields),
    do: parse_csv_fields(rest, :unquoted, "", fields)

  # Inside a quoted field.
  defp parse_csv_fields(<<?", ?", rest::binary>>, :quoted, acc, fields),
    do: parse_csv_fields(rest, :quoted, acc <> "\"", fields)

  defp parse_csv_fields(<<?", rest::binary>>, :quoted, acc, fields),
    do: parse_csv_fields(rest, :after_quote, acc, fields)

  defp parse_csv_fields(<<char::utf8, rest::binary>>, :quoted, acc, fields),
    do: parse_csv_fields(rest, :quoted, acc <> <<char::utf8>>, fields)

  defp parse_csv_fields("", :quoted, acc, fields), do: Enum.reverse([acc | fields])

  # Just closed a quoted field; expect a comma or end of line.
  defp parse_csv_fields(<<?,, rest::binary>>, :after_quote, acc, fields),
    do: parse_csv_fields(rest, :start, "", [acc | fields])

  defp parse_csv_fields("", :after_quote, acc, fields), do: Enum.reverse([acc | fields])

  defp parse_csv_fields(<<_char::utf8, rest::binary>>, :after_quote, acc, fields),
    do: parse_csv_fields(rest, :after_quote, acc, fields)

  # Inside an unquoted field.
  defp parse_csv_fields(<<?,, rest::binary>>, :unquoted, acc, fields),
    do: parse_csv_fields(rest, :start, "", [acc | fields])

  defp parse_csv_fields(<<char::utf8, rest::binary>>, :unquoted, acc, fields),
    do: parse_csv_fields(rest, :unquoted, acc <> <<char::utf8>>, fields)

  defp parse_csv_fields("", :unquoted, acc, fields), do: Enum.reverse([acc | fields])
end
