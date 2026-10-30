defmodule Huginn.Clickhouse.Result do
  @moduledoc """
  Parses and transforms ClickHouse gRPC Result messages.

  ## Row values

  `rows` holds one list per row, values in column order. How values are typed
  depends on the output format (see `parse_output/3`):

    * `TabSeparated`/`TSV` and `CSV`/`CSVWithNames` - every field is a binary,
      with ClickHouse escaping undone. `NULL` decodes to `nil`. Bytes are kept
      as-is, so `String` columns that hold non-UTF-8 data decode to raw
      binaries.
    * `JSONEachRow`, `JSONCompactEachRow` (and their `Strings` variants) - JSON
      types (`nil`, numbers, booleans, binaries, lists, maps).
    * Any other format - the whole output is returned as a single row holding
      the raw bytes: `[[output]]`.
  """

  alias Clickhouse.Grpc.{Exception, LogEntry, NameAndType, Progress, Stats}
  alias Clickhouse.Grpc.Result, as: GrpcResult

  @type column :: %{name: String.t(), type: String.t()}

  @typedoc "A decoded field. See the module docs for which types each format yields."
  @type value :: binary() | nil | number() | boolean() | list() | map()

  @type row :: list(value())

  @type t :: %__MODULE__{
          query_id: String.t(),
          rows: list(row()),
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

  @tsv_formats ["TabSeparated", "TSV"]
  @csv_formats ["CSV", "CSVWithNames"]
  @json_object_formats ["JSONEachRow", "JSONStringsEachRow"]
  @json_array_formats ["JSONCompactEachRow", "JSONCompactStringsEachRow"]

  @doc """
  Converts a gRPC Result message to a `Huginn.Clickhouse.Result` struct.

  `defaults` lets a streaming caller inherit `:format` and `:columns` from an
  earlier message in the same response.

  ClickHouse populates `output_format` and `output_columns` only on the *first*
  `Result` of a stream; later chunks leave both empty. Parsing such a chunk in
  isolation would fall through to the unknown-format clause of `parse_output/3`
  and collapse the whole chunk into a single opaque row, so streaming callers
  thread the values established by the first chunk.

  Returns `{:error, %Huginn.Error{}}` when the message carries a ClickHouse
  exception (`reason: :server`), is not a `Result` at all
  (`reason: :unexpected_reply`), or its output cannot be decoded in the declared
  format (`reason: :decode`).
  """
  @spec from_grpc(term(), keyword()) :: {:ok, t()} | {:error, Huginn.Error.t()}
  def from_grpc(grpc_result, defaults \\ [])

  def from_grpc(%GrpcResult{exception: %Exception{code: code} = exception}, _defaults)
      when code != 0 do
    {:error, Huginn.Error.wrap(exception)}
  end

  def from_grpc(%GrpcResult{} = grpc_result, defaults) do
    format = presence(grpc_result.output_format) || Keyword.get(defaults, :format)

    columns =
      case parse_columns(grpc_result.output_columns) do
        [] -> Keyword.get(defaults, :columns, [])
        parsed -> parsed
      end

    with {:ok, rows} <- parse_output(grpc_result.output, format, columns) do
      {:ok,
       %__MODULE__{
         query_id: grpc_result.query_id,
         output_format: format,
         time_zone: grpc_result.time_zone,
         columns: columns,
         rows: rows,
         stats: parse_stats(grpc_result.stats),
         progress: parse_progress(grpc_result.progress),
         logs: parse_logs(grpc_result.logs)
       }}
    end
  end

  # A trailers-only reply (grpc-status 0 carrying no message) decodes to `[]`
  # rather than a Result, so surface it as an error instead of raising.
  def from_grpc(other, _defaults) do
    {:error,
     %Huginn.Error{
       reason: :unexpected_reply,
       message: "expected a ClickHouse Result message, got: #{inspect(other, limit: 5)}",
       details: other
     }}
  end

  @doc """
  Converts rows to a list of maps using column names as keys.

  ## Examples

      iex> result = %Huginn.Clickhouse.Result{
      ...>   columns: [%{name: "name", type: "String"}, %{name: "age", type: "UInt8"}],
      ...>   rows: [["alice", "25"]]
      ...> }
      iex> Huginn.Clickhouse.Result.to_maps(result)
      [%{"name" => "alice", "age" => "25"}]

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

  Supported formats:

    * `TabSeparated`/`TSV` - rows end at `\\n`, fields at `\\t`. ClickHouse
      escape sequences are undone exactly as the server's own reader does:
      `\\b \\f \\r \\n \\t \\0 \\a \\e \\v` decode to control bytes, `\\xHH` to
      the byte `0xHH`, a backslash before `\\ ' " / =`, a backtick or a control
      byte decodes to that byte, and any other `\\c` stays as the two bytes
      `\\c`. A field that is exactly `\\N` is `NULL` and decodes to `nil`.
    * `CSV`/`CSVWithNames` - RFC 4180 style over the whole buffer: quoted fields
      may contain `,`, `\\n`, `\\r\\n` and doubled quotes; rows end at `\\n` or
      `\\r\\n`. An unquoted `\\N` decodes to `nil` (a quoted `"\\N"` is the
      two-byte string). `CSVWithNames` returns the header line as the first row.
    * `JSONEachRow`/`JSONStringsEachRow` - one JSON object per line. With
      `columns`, values are extracted in column order (JSON key order is not
      guaranteed to match it) and a missing key yields `nil`; without columns
      the values come in `Map.values/1` order.
    * `JSONCompactEachRow`/`JSONCompactStringsEachRow` - one JSON array per line.
    * anything else - `[[output]]`, the raw bytes as a single row.

  Decoding works on bytes, so non-UTF-8 `String` data never crashes the
  parser. Malformed output returns `{:error, %Huginn.Error{reason: :decode}}`.

  ## Examples

      iex> Huginn.Clickhouse.Result.parse_output("a\\\\tb\\t\\\\N\\n", "TabSeparated")
      {:ok, [["a\\tb", nil]]}

      iex> Huginn.Clickhouse.Result.parse_output(~s("x,""y"" z",\\\\N\\r\\n), "CSV")
      {:ok, [[~s(x,"y" z), nil]]}

  """
  @spec parse_output(binary() | nil, String.t() | nil, list(column())) ::
          {:ok, list(row())} | {:error, Huginn.Error.t()}
  def parse_output(output, format, columns \\ [])
  def parse_output(<<>>, _format, _columns), do: {:ok, []}
  def parse_output(nil, _format, _columns), do: {:ok, []}

  def parse_output(output, format, _columns) when format in @tsv_formats do
    output |> tsv(output, 0, [], [], []) |> decode_result(output, format)
  end

  def parse_output(output, format, _columns) when format in @csv_formats do
    output |> csv_start([], []) |> decode_result(output, format)
  end

  def parse_output(output, format, columns) when format in @json_object_formats do
    column_names = Enum.map(columns, & &1.name)

    parse_json_lines(output, format, fn
      decoded when is_map(decoded) and column_names == [] -> {:ok, Map.values(decoded)}
      decoded when is_map(decoded) -> {:ok, Enum.map(column_names, &Map.get(decoded, &1))}
      _other -> :error
    end)
  end

  def parse_output(output, format, _columns) when format in @json_array_formats do
    parse_json_lines(output, format, fn
      decoded when is_list(decoded) -> {:ok, decoded}
      _other -> :error
    end)
  end

  def parse_output(output, _format, _columns) do
    # Return raw binary as single row for unknown formats
    {:ok, [[output]]}
  end

  defp decode_result({:ok, rows}, _output, _format), do: {:ok, rows}

  defp decode_result({:error, message, rest}, output, format) do
    offset = byte_size(output) - byte_size(rest)

    {:error,
     %Huginn.Error{
       reason: :decode,
       message: "cannot decode #{format} output at byte #{offset}: #{message}",
       details: %{format: format, offset: offset}
     }}
  end

  # JSON strings escape newlines, so every raw `\n` separates two documents.
  defp parse_json_lines(output, format, to_row) do
    output
    |> :binary.split("\n", [:global])
    |> Enum.reduce_while({:ok, []}, fn
      "", acc ->
        {:cont, acc}

      line, {:ok, rows} ->
        with {:ok, decoded} <- Jason.decode(line),
             {:ok, row} <- to_row.(decoded) do
          {:cont, {:ok, [row | rows]}}
        else
          {:error, error} -> {:halt, json_error(format, line, error)}
          :error -> {:halt, json_error(format, line, :unexpected_shape)}
        end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  defp json_error(format, line, details) do
    detail =
      case details do
        %Jason.DecodeError{} = error -> Jason.DecodeError.message(error)
        :unexpected_shape -> "unexpected JSON value for #{format}"
      end

    {:error,
     %Huginn.Error{
       reason: :decode,
       message:
         "cannot decode #{format} output: #{detail} (line: #{inspect(line, limit: 5, printable_limit: 200)})",
       details: details
     }}
  end

  # TabSeparated decoder, mirroring ClickHouse's readEscapedStringInto
  # (src/IO/ReadHelpers.cpp) plus Nullable's `\N` handling.
  #
  # tsv(rest, seg, len, acc, fields, rows): `seg` starts at the current run of
  # plain bytes and `len` counts them, so a field without escapes becomes a
  # single `binary_part/3` instead of byte-by-byte appends. `acc` holds the
  # iodata decoded so far for the current field (before `seg`); `fields` and
  # `rows` are accumulated in reverse.
  defp tsv(<<?\t, rest::binary>>, seg, len, acc, fields, rows),
    do: tsv(rest, rest, 0, [], [field_binary(acc, seg, len) | fields], rows)

  defp tsv(<<?\n, rest::binary>>, seg, len, acc, fields, rows),
    do: tsv(rest, rest, 0, [], [], [Enum.reverse([field_binary(acc, seg, len) | fields]) | rows])

  # A field that is exactly `\N` is NULL. Mid-field `\N` decodes to nothing
  # (handled by the generic escape clause), as in ClickHouse.
  defp tsv(<<?\\, ?N, ?\t, rest::binary>>, _seg, 0, [], fields, rows),
    do: tsv(rest, rest, 0, [], [nil | fields], rows)

  defp tsv(<<?\\, ?N, ?\n, rest::binary>>, _seg, 0, [], fields, rows),
    do: tsv(rest, rest, 0, [], [], [Enum.reverse([nil | fields]) | rows])

  defp tsv(<<?\\, ?N>>, _seg, 0, [], fields, rows),
    do: {:ok, Enum.reverse([Enum.reverse([nil | fields]) | rows])}

  defp tsv(<<?\\, rest::binary>> = escape, seg, len, acc, fields, rows) do
    case unescape(rest) do
      {decoded, rest} ->
        tsv(rest, rest, 0, [acc, binary_part(seg, 0, len), decoded], fields, rows)

      :error ->
        {:error, "invalid escape sequence", escape}
    end
  end

  defp tsv(<<_byte, rest::binary>>, seg, len, acc, fields, rows),
    do: tsv(rest, seg, len + 1, acc, fields, rows)

  # The output ends with `\n`, so normally nothing is pending here.
  defp tsv(<<>>, _seg, 0, [], [], rows), do: {:ok, Enum.reverse(rows)}

  defp tsv(<<>>, seg, len, acc, fields, rows),
    do: {:ok, Enum.reverse([Enum.reverse([field_binary(acc, seg, len) | fields]) | rows])}

  # The current field: the pending plain run, prefixed by any decoded iodata.
  defp field_binary([], seg, len), do: binary_part(seg, 0, len)
  defp field_binary(acc, seg, len), do: IO.iodata_to_binary([acc, binary_part(seg, 0, len)])

  # parseComplexEscapeSequence + parseEscapeSequence (src/IO/ReadHelpers.{h,cpp}).
  # Returns the decoded iodata and the bytes after the sequence.
  defp unescape(<<?x, hi, lo, rest::binary>>) do
    case {hex(hi), hex(lo)} do
      {h, l} when is_integer(h) and is_integer(l) -> {h * 16 + l, rest}
      _ -> :error
    end
  end

  defp unescape(<<?x, _rest::binary>>), do: :error
  defp unescape(<<?N, rest::binary>>), do: {[], rest}
  defp unescape(<<?a, rest::binary>>), do: {?\a, rest}
  defp unescape(<<?b, rest::binary>>), do: {?\b, rest}
  defp unescape(<<?e, rest::binary>>), do: {?\e, rest}
  defp unescape(<<?f, rest::binary>>), do: {?\f, rest}
  defp unescape(<<?n, rest::binary>>), do: {?\n, rest}
  defp unescape(<<?r, rest::binary>>), do: {?\r, rest}
  defp unescape(<<?t, rest::binary>>), do: {?\t, rest}
  defp unescape(<<?v, rest::binary>>), do: {?\v, rest}
  defp unescape(<<?0, rest::binary>>), do: {0, rest}

  # These, and control bytes (a backslash before a raw tab or newline), decode to
  # the byte itself.
  defp unescape(<<char, rest::binary>>) when char in [?\\, ?', ?", ?`, ?/, ?=] or char <= 31,
    do: {char, rest}

  # ClickHouse keeps the backslash for any other character (`\%` stays `\%`), so
  # LIKE patterns survive.
  defp unescape(<<char, rest::binary>>), do: {[?\\, char], rest}
  defp unescape(<<>>), do: :error

  defp hex(c) when c in ?0..?9, do: c - ?0
  defp hex(c) when c in ?a..?f, do: c - ?a + 10
  defp hex(c) when c in ?A..?F, do: c - ?A + 10
  defp hex(_c), do: nil

  # CSV decoder: quote-aware over the whole buffer, so quoted fields can contain
  # newlines. ClickHouse writes strings quoted (doubling `"`), numbers bare, and
  # NULL as a bare `\N`.
  defp csv_start(<<?", rest::binary>>, fields, rows),
    do: csv_quoted(rest, rest, 0, [], fields, rows)

  defp csv_start(rest, fields, rows), do: csv_unquoted(rest, rest, 0, fields, rows)

  defp csv_unquoted(<<?,, rest::binary>>, seg, len, fields, rows),
    do: csv_start(rest, [csv_bare(seg, len) | fields], rows)

  defp csv_unquoted(<<?\r, ?\n, rest::binary>>, seg, len, fields, rows),
    do: csv_row_end(rest, [csv_bare(seg, len) | fields], rows)

  defp csv_unquoted(<<?\n, rest::binary>>, seg, len, fields, rows),
    do: csv_row_end(rest, [csv_bare(seg, len) | fields], rows)

  # Nothing pending after the final row terminator.
  defp csv_unquoted(<<>>, _seg, 0, [], rows), do: {:ok, Enum.reverse(rows)}

  defp csv_unquoted(<<>>, seg, len, fields, rows),
    do: {:ok, Enum.reverse([Enum.reverse([csv_bare(seg, len) | fields]) | rows])}

  defp csv_unquoted(<<_byte, rest::binary>>, seg, len, fields, rows),
    do: csv_unquoted(rest, seg, len + 1, fields, rows)

  defp csv_bare(seg, len) do
    case binary_part(seg, 0, len) do
      "\\N" -> nil
      field -> field
    end
  end

  # A doubled quote is one literal quote: keep the run including the first `"`.
  defp csv_quoted(<<?", ?", rest::binary>>, seg, len, acc, fields, rows),
    do: csv_quoted(rest, rest, 0, [acc, binary_part(seg, 0, len + 1)], fields, rows)

  defp csv_quoted(<<?", rest::binary>>, seg, len, acc, fields, rows),
    do: csv_after_quote(rest, [field_binary(acc, seg, len) | fields], rows)

  defp csv_quoted(<<_byte, rest::binary>>, seg, len, acc, fields, rows),
    do: csv_quoted(rest, seg, len + 1, acc, fields, rows)

  defp csv_quoted(<<>>, _seg, _len, _acc, _fields, _rows),
    do: {:error, "unterminated quoted field", <<>>}

  defp csv_after_quote(<<?,, rest::binary>>, fields, rows), do: csv_start(rest, fields, rows)

  defp csv_after_quote(<<?\r, ?\n, rest::binary>>, fields, rows),
    do: csv_row_end(rest, fields, rows)

  defp csv_after_quote(<<?\n, rest::binary>>, fields, rows), do: csv_row_end(rest, fields, rows)

  defp csv_after_quote(<<>>, fields, rows),
    do: {:ok, Enum.reverse([Enum.reverse(fields) | rows])}

  defp csv_after_quote(rest, _fields, _rows),
    do: {:error, "unexpected data after a closing quote", rest}

  defp csv_row_end(rest, fields, rows), do: csv_start(rest, [], [Enum.reverse(fields) | rows])

  defp presence(nil), do: nil
  defp presence(""), do: nil
  defp presence(value), do: value

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
  defp log_level_to_atom(:LOG_TEST), do: :test
  defp log_level_to_atom(_), do: :unknown
end
