defmodule Huginn.Clickhouse.Stream do
  @moduledoc """
  Streaming utilities for ClickHouse gRPC operations.

  Provides helpers for:
  - Building input streams for `ExecuteQueryWithStreamInput`
  - Processing output streams from `ExecuteQueryWithStreamOutput`
  - Bidirectional streaming with `ExecuteQueryWithStreamIO`
  """

  alias Huginn.Clickhouse.{Query, Result}
  alias Clickhouse.Grpc.QueryInfo

  @doc """
  Creates an input stream from an enumerable of data chunks.

  The first QueryInfo contains the SQL query, subsequent ones contain data.

  ## Options

    * `:chunk_size` - Number of rows per chunk (for lists)
    * `:format` - Input data format (default: "TabSeparated")
    * All options from `Query.build/2`

  ## Examples

      # Stream from a list of rows
      rows = [["a", "1"], ["b", "2"], ["c", "3"]]
      stream = Stream.input_stream("INSERT INTO t VALUES", rows, format: "TabSeparated")

      # Stream from a file
      File.stream!("data.csv")
      |> Stream.input_stream("INSERT INTO t FORMAT CSV", format: "CSV")

  """
  @spec input_stream(String.t(), Enumerable.t(), keyword()) :: Enumerable.t(QueryInfo.t())
  def input_stream(sql, data_enum, opts \\ []) do
    format = Keyword.get(opts, :format, "TabSeparated")
    chunk_size = Keyword.get(opts, :chunk_size, 1000)

    Stream.concat(
      # First message: query with initial data
      Stream.resource(
        fn -> {data_enum, true} end,
        fn
          {enum, true} ->
            case Enum.take(enum, chunk_size) do
              [] ->
                {:halt, nil}

              chunk ->
                data = encode_chunk(chunk, format)
                query_info = Query.build_insert(sql, data, opts)
                remaining = Enum.drop(enum, chunk_size)
                has_more = Enum.any?(remaining)
                query_info = %{query_info | next_query_info: has_more}
                {[query_info], {remaining, false}}
            end

          {enum, false} ->
            case Enum.take(enum, chunk_size) do
              [] ->
                {:halt, nil}

              chunk ->
                data = encode_chunk(chunk, format)
                remaining = Enum.drop(enum, chunk_size)
                has_more = Enum.any?(remaining)
                query_info = Query.build_continuation(data, has_more: has_more)
                {[query_info], {remaining, false}}
            end
        end,
        fn _ -> :ok end
      ),
      []
    )
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

  defp encode_chunk(chunk, "TabSeparated") when is_list(chunk) do
    chunk
    |> Enum.map(fn row -> Enum.join(row, "\t") end)
    |> Enum.join("\n")
  end

  defp encode_chunk(chunk, "CSV") when is_list(chunk) do
    chunk
    |> Enum.map(fn row ->
      row
      |> Enum.map(&encode_csv_field/1)
      |> Enum.join(",")
    end)
    |> Enum.join("\n")
  end

  defp encode_chunk(chunk, "JSONEachRow") when is_list(chunk) do
    chunk
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")
  end

  defp encode_chunk(chunk, _format) when is_binary(chunk), do: chunk
  defp encode_chunk(chunk, _format) when is_list(chunk), do: Enum.join(chunk, "\n")

  defp encode_csv_field(field) when is_binary(field) do
    if String.contains?(field, [",", "\"", "\n"]) do
      "\"" <> String.replace(field, "\"", "\"\"") <> "\""
    else
      field
    end
  end

  defp encode_csv_field(field), do: to_string(field)
end
