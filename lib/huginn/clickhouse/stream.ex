defmodule Huginn.Clickhouse.Stream do
  @moduledoc """
  Builds the `QueryInfo` message stream for `ExecuteQueryWithStreamInput`.

  `input_stream/3` encodes rows (or passes pre-encoded binaries through),
  packs them into messages of roughly `:chunk_size` bytes and turns them into
  the lazy message stream that `Huginn.Clickhouse.Client.insert_stream/3` sends.

  ## Row encoding

  The `:format` option selects how structured rows are encoded. It must match
  the `FORMAT` clause of the `INSERT` statement, which is what ClickHouse uses
  to parse the data.

    * `TabSeparated`/`TSV` (and the `WithNames`/`WithNamesAndTypes` variants) -
      a row is a list of fields joined by `\\t` and ended by `\\n`. Fields are
      escaped like ClickHouse's own writer: `\\`, `'`, `\\t`, `\\n`, `\\r`,
      `\\0`, `\\b` and `\\f` become backslash sequences. `nil` becomes `\\N`
      (`NULL`).
    * `CSV` (and `CSVWithNames`/`CSVWithNamesAndTypes`) - binaries are always
      double-quoted with `"` doubled, so empty strings, the literal string
      `\\N`, surrounding whitespace, commas and newlines all survive. Integers
      and floats are written bare. `nil` becomes a bare `\\N` (`NULL`).
    * `JSONEachRow` - a row is a map, encoded as one JSON object per line. List
      rows raise `ArgumentError`; use `JSONCompactEachRow` for them.
    * `JSONCompactEachRow` - a row is a list, encoded as one JSON array per line.

  In the TSV and CSV formats, non-binary scalars (integers, floats, booleans,
  atoms, anything implementing `String.Chars`) are converted with
  `to_string/1`. Nested lists and maps are not supported there; encode them
  yourself or use a JSON format.

  Elements that are binaries are passed through verbatim whatever the
  `:format`, so pre-encoded data (for example `File.stream!/2` chunks) keeps
  the caller's own framing. Each binary element must end on a row boundary
  that the format understands.
  """

  alias Huginn.Clickhouse.Query

  @default_chunk_size 1_048_576

  @tsv_formats [
    "TabSeparated",
    "TSV",
    "TabSeparatedWithNames",
    "TSVWithNames",
    "TabSeparatedWithNamesAndTypes",
    "TSVWithNamesAndTypes"
  ]
  @csv_formats ["CSV", "CSVWithNames", "CSVWithNamesAndTypes"]

  @doc """
  Builds a lazy stream of `QueryInfo` messages for a streaming insert.

  The first message carries the SQL query plus the first data chunk; every
  later message carries only data. The source is enumerated exactly once and
  lazily, so non-restartable sources (`File.stream!/2`, a `Stream.map/2` with
  side effects) are safe and memory stays bounded by `:chunk_size`. An empty
  source still yields one message (the query with empty data).

  ## Options

    * `:chunk_size` - target number of bytes of input data per message
      (default: #{@default_chunk_size}). Encoded rows are accumulated until
      the buffer reaches this size, then sent as one message. An element that
      is by itself at least `:chunk_size` bytes is sent alone in its own
      message; elements are never split.
    * `:format` - how structured rows are encoded (default: `"TabSeparated"`);
      see the module docs. It does not change the SQL - the `FORMAT` clause of
      the `INSERT` must name the same format.
    * All options from `Query.build/2`

  `next_query_info` is set by `Huginn.Clickhouse.Client.insert_stream/3`, which
  knows which message is last.

  ## Examples

      # Structured rows, encoded by this module
      rows = [["a", 1], ["b\\tc", nil]]
      input_stream("INSERT INTO t FORMAT TabSeparated", rows)

      # Maps as JSON objects
      input_stream("INSERT INTO t FORMAT JSONEachRow", [%{"id" => 1}], format: "JSONEachRow")

      # Pre-encoded chunks, passed through untouched and packed into ~1 MiB messages
      File.stream!("data.csv", 65_536)
      |> then(&input_stream("INSERT INTO t FORMAT CSV", &1))

  """
  @spec input_stream(String.t(), Enumerable.t(), keyword()) :: Enumerable.t(struct())
  def input_stream(sql, data_enum, opts \\ []) do
    format = Keyword.get(opts, :format, "TabSeparated")
    chunk_size = Keyword.get(opts, :chunk_size, @default_chunk_size)

    unless is_integer(chunk_size) and chunk_size > 0 do
      raise ArgumentError, ":chunk_size must be a positive integer, got: #{inspect(chunk_size)}"
    end

    data_enum
    |> chunk_bytes(format, chunk_size)
    |> Stream.with_index()
    |> Stream.map(fn
      {data, 0} -> Query.build_insert(sql, data, opts)
      {data, _} -> Query.build_continuation(data, has_more: true)
    end)
  end

  # Packs encoded elements into binaries of at least `chunk_size` bytes (except
  # the last). The accumulator is `{iodata, byte_count, emitted?}`; the buffer
  # is converted to a binary once per message.
  defp chunk_bytes(data_enum, format, chunk_size) do
    Stream.transform(
      data_enum,
      fn -> {[], 0, false} end,
      fn element, {buffer, size, emitted?} ->
        encoded = encode_element(element, format)
        encoded_size = IO.iodata_length(encoded)

        cond do
          encoded_size >= chunk_size ->
            # Oversized: flush what is buffered, then send the element alone.
            chunks = if size > 0, do: [IO.iodata_to_binary(buffer)], else: []
            {chunks ++ [IO.iodata_to_binary(encoded)], {[], 0, true}}

          size + encoded_size >= chunk_size ->
            {[IO.iodata_to_binary([buffer, encoded])], {[], 0, true}}

          true ->
            {[], {[buffer, encoded], size + encoded_size, emitted?}}
        end
      end,
      fn
        # Nothing was ever sent: still send the query, with empty data.
        {_buffer, 0, false} = acc -> {[""], acc}
        {_buffer, 0, true} = acc -> {[], acc}
        {buffer, _size, _emitted?} = acc -> {[IO.iodata_to_binary(buffer)], acc}
      end,
      fn _acc -> :ok end
    )
  end

  defp encode_element(element, _format) when is_binary(element), do: element

  # Every structured row ends with a newline, otherwise concatenating adjacent
  # messages would merge rows (ClickHouse joins `input_data` across QueryInfos).
  defp encode_element(row, format) when format in @tsv_formats and is_list(row),
    do: [join(row, ?\t, &encode_tsv_field/1), ?\n]

  defp encode_element(row, format) when format in @csv_formats and is_list(row),
    do: [join(row, ?,, &encode_csv_field/1), ?\n]

  defp encode_element(row, "JSONEachRow") when is_map(row),
    do: [Jason.encode_to_iodata!(row), ?\n]

  defp encode_element(row, "JSONEachRow") when is_list(row) do
    raise ArgumentError,
          "JSONEachRow rows must be maps (encoded as JSON objects), got a list: " <>
            "#{inspect(row, limit: 5)}. Use format: \"JSONCompactEachRow\" " <>
            "(with FORMAT JSONCompactEachRow in the SQL) for list rows"
  end

  defp encode_element(row, "JSONCompactEachRow") when is_list(row),
    do: [Jason.encode_to_iodata!(row), ?\n]

  defp encode_element(row, format) do
    raise ArgumentError,
          "cannot encode #{inspect(row, limit: 5)} as #{inspect(format)}: supported :format " <>
            "values are TabSeparated/TSV, CSV (and their WithNames variants) and " <>
            "JSONCompactEachRow for list rows, JSONEachRow for map rows. Pass " <>
            "pre-encoded binaries for any other format"
  end

  defp join([], _separator, _encode), do: []
  defp join([field], _separator, encode), do: [encode.(field)]

  defp join([field | rest], separator, encode),
    do: [encode.(field), separator | join(rest, separator, encode)]

  defp encode_tsv_field(nil), do: "\\N"
  defp encode_tsv_field(field) when is_binary(field), do: escape_tsv(field, field, 0, [])
  defp encode_tsv_field(field), do: field |> scalar_to_string() |> encode_tsv_field()

  # Mirrors ClickHouse's writeAnyEscapedString<'\''> (src/IO/WriteHelpers.h),
  # which TabSeparated uses. `seg`/`len` track the current run of bytes that
  # need no escaping so it is copied with one `binary_part/3`.
  defp escape_tsv(<<char, rest::binary>>, seg, len, acc)
       when char in [?\\, ?', ?\t, ?\n, ?\r, ?\0, ?\b, ?\f],
       do: escape_tsv(rest, rest, 0, [acc, binary_part(seg, 0, len), tsv_escape(char)])

  defp escape_tsv(<<_byte, rest::binary>>, seg, len, acc), do: escape_tsv(rest, seg, len + 1, acc)
  defp escape_tsv(<<>>, seg, _len, []), do: seg
  defp escape_tsv(<<>>, seg, _len, acc), do: [acc, seg]

  defp tsv_escape(?\\), do: "\\\\"
  defp tsv_escape(?'), do: "\\'"
  defp tsv_escape(?\t), do: "\\t"
  defp tsv_escape(?\n), do: "\\n"
  defp tsv_escape(?\r), do: "\\r"
  defp tsv_escape(?\0), do: "\\0"
  defp tsv_escape(?\b), do: "\\b"
  defp tsv_escape(?\f), do: "\\f"

  defp encode_csv_field(nil), do: "\\N"
  defp encode_csv_field(field) when is_integer(field) or is_float(field), do: to_string(field)

  defp encode_csv_field(field) when is_binary(field),
    do: [?", :binary.replace(field, "\"", "\"\"", [:global]), ?"]

  defp encode_csv_field(field), do: field |> scalar_to_string() |> encode_csv_field()

  defp scalar_to_string(field) when is_list(field) or is_map(field) do
    raise ArgumentError,
          "nested lists and maps cannot be encoded as TSV/CSV fields, got: " <>
            "#{inspect(field, limit: 5)}. Encode the value yourself or use a JSON format"
  end

  defp scalar_to_string(field), do: to_string(field)
end
