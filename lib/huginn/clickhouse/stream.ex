defmodule Huginn.Clickhouse.Stream do
  @moduledoc """
  Streaming utilities for ClickHouse gRPC operations.

  Provides helpers for:
  - Building input streams for `ExecuteQueryWithStreamInput`
  - Processing output streams from `ExecuteQueryWithStreamOutput`
  - Bidirectional streaming with `ExecuteQueryWithStreamIO`
  """

  alias Huginn.Clickhouse.{Query, Result}

  @doc """
  Builds a lazy stream of `QueryInfo` messages for a streaming insert.

  The first message carries the SQL query plus the first data chunk; every
  later message carries only data. The source is enumerated exactly once, so
  non-restartable sources (`File.stream!/2`, a `Stream.map/2` with side
  effects) are safe.

  ## Options

    * `:chunk_size` - Elements per chunk (default: 1000)
    * `:format` - Input data format (default: "TabSeparated")
    * All options from `Query.build/2`

  Elements that are already encoded binaries are concatenated verbatim, so the
  caller's own framing is preserved. Structured rows are encoded according to
  `:format`.

  `next_query_info` is set by `Huginn.Clickhouse.Client.insert_stream/3`, which
  knows which message is last.

  ## Examples

      # Structured rows, encoded by this module
      rows = [["a", "1"], ["b", "2"], ["c", "3"]]
      input_stream("INSERT INTO t FORMAT TabSeparated", rows)

      # Pre-encoded chunks, passed through untouched
      File.stream!("data.csv", [], 65_536)
      |> then(&input_stream("INSERT INTO t FORMAT CSV", &1, chunk_size: 1))

  """
  @spec input_stream(String.t(), Enumerable.t(), keyword()) :: Enumerable.t(struct())
  def input_stream(sql, data_enum, opts \\ []) do
    format = Keyword.get(opts, :format, "TabSeparated")
    chunk_size = Keyword.get(opts, :chunk_size, 1000)

    data_enum
    |> Stream.chunk_every(chunk_size)
    |> Stream.map(&encode_chunk(&1, format))
    |> Stream.with_index()
    |> Stream.map(fn
      {data, 0} -> Query.build_insert(sql, data, opts)
      {data, _} -> Query.build_continuation(data, has_more: true)
    end)
  end

  @doc """
  Collects all results from an output stream into a single Result.
  """
  @spec collect_output(Enumerable.t()) :: {:ok, Result.t()} | {:error, term()}
  def collect_output(stream) do
    results =
      stream
      |> Enum.reduce_while([], fn
        {:ok, grpc_result}, acc ->
          case Result.from_grpc(grpc_result) do
            {:ok, result} -> {:cont, [result | acc]}
            {:error, _} = error -> {:halt, error}
          end

        {:error, _} = error, _acc ->
          {:halt, error}
      end)

    case results do
      {:error, _} = error -> error
      results when is_list(results) -> {:ok, Result.merge(Enum.reverse(results))}
    end
  end

  @doc """
  Transforms an output stream, applying a function to each Result chunk.
  """
  @spec transform_output(Enumerable.t(), (Result.t() -> term())) :: Enumerable.t()
  def transform_output(stream, fun) do
    Stream.map(stream, fn
      {:ok, grpc_result} ->
        case Result.from_grpc(grpc_result) do
          {:ok, result} -> {:ok, fun.(result)}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end)
  end

  @doc """
  Creates a stream that yields parsed Result structs from gRPC results.
  """
  @spec parse_stream(Enumerable.t()) :: Enumerable.t({:ok, Result.t()} | {:error, term()})
  def parse_stream(stream) do
    Stream.map(stream, fn
      {:ok, grpc_result} -> Result.from_grpc(grpc_result)
      {:error, _} = error -> error
    end)
  end

  @doc """
  Streams rows from an output stream, yielding individual rows.

  Useful for processing large result sets row by row.
  """
  @spec row_stream(Enumerable.t()) :: Enumerable.t(list())
  def row_stream(stream) do
    stream
    |> parse_stream()
    |> Stream.flat_map(fn
      {:ok, %Result{rows: rows}} -> rows
      {:error, _} -> []
    end)
  end

  @doc """
  Streams rows as maps from an output stream.
  """
  @spec map_stream(Enumerable.t()) :: Enumerable.t(map())
  def map_stream(stream) do
    stream
    |> parse_stream()
    |> Stream.flat_map(fn
      {:ok, result} -> Result.to_maps(result)
      {:error, _} -> []
    end)
  end

  # Already-encoded binaries are concatenated verbatim so the caller's framing
  # survives. Structured rows get a trailing newline on every row, otherwise
  # concatenating adjacent chunks would merge the last row of one chunk into
  # the first row of the next (ClickHouse joins `input_data` across QueryInfos).
  defp encode_chunk([head | _] = chunk, _format) when is_binary(head) do
    IO.iodata_to_binary(chunk)
  end

  defp encode_chunk(chunk, format) do
    Enum.map_join(chunk, fn row -> encode_row(row, format) <> "\n" end)
  end

  defp encode_row(row, format) when format in ["TabSeparated", "TSV"] do
    Enum.join(row, "\t")
  end

  defp encode_row(row, format) when format in ["CSV", "CSVWithNames"] do
    Enum.map_join(row, ",", &encode_csv_field/1)
  end

  defp encode_row(row, "JSONEachRow"), do: Jason.encode!(row)

  defp encode_row(row, _format), do: Enum.join(row, "\t")

  defp encode_csv_field(field) when is_binary(field) do
    if String.contains?(field, [",", "\"", "\n"]) do
      "\"" <> String.replace(field, "\"", "\"\"") <> "\""
    else
      field
    end
  end

  defp encode_csv_field(field), do: to_string(field)
end
