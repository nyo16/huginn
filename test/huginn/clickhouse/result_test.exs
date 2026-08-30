defmodule Huginn.Clickhouse.ResultTest do
  use ExUnit.Case, async: true

  alias Clickhouse.Grpc.{LogEntry, LogsLevel, NameAndType}
  alias Clickhouse.Grpc.Result, as: GrpcResult
  alias Huginn.Clickhouse.{Result, Stream}

  describe "parse_output/2 CSV" do
    test "parses simple unquoted fields" do
      assert Result.parse_output("a,b,c\n1,2,3", "CSV") == [["a", "b", "c"], ["1", "2", "3"]]
    end

    test "keeps commas inside quoted fields" do
      assert Result.parse_output(~s("a,b",c), "CSV") == [["a,b", "c"]]
    end

    test "decodes doubled quotes inside a quoted field" do
      assert Result.parse_output(~s("she said ""hi""",x), "CSV") == [[~s(she said "hi"), "x"]]
    end

    test "handles empty fields and empty quoted fields" do
      assert Result.parse_output(~s(,"",a), "CSV") == [["", "", "a"]]
    end

    test "round-trips with the CSV writer" do
      row = ["plain", "has,comma", ~s(has"quote), "trailing\\"]
      encoded = encode_csv_row(row)
      assert Result.parse_output(encoded, "CSV") == [row]
    end
  end

  describe "parse_output/3 JSONEachRow" do
    test "extracts values in column order regardless of JSON key order" do
      columns = [%{name: "id", type: "UInt8"}, %{name: "name", type: "String"}]
      # Keys deliberately out of column order.
      output = ~s({"name":"alice","id":1}\n{"name":"bob","id":2})

      assert Result.parse_output(output, "JSONEachRow", columns) == [
               [1, "alice"],
               [2, "bob"]
             ]
    end

    test "falls back to JSON order when no columns given (2-arity)" do
      output = ~s({"a":1,"b":2})
      assert Result.parse_output(output, "JSONEachRow") == [[1, 2]]
    end

    test "fills nil for a column missing from the JSON object" do
      columns = [%{name: "id", type: "UInt8"}, %{name: "name", type: "String"}]
      assert Result.parse_output(~s({"id":1}), "JSONEachRow", columns) == [[1, nil]]
    end
  end

  describe "from_grpc/2 streaming chunks" do
    test "inherits format and columns from an earlier chunk" do
      # ClickHouse sets output_format/output_columns only on the first Result of
      # a stream, so a later chunk arrives with both empty. Without inheritance
      # the entire chunk collapses into one opaque row.
      later_chunk = %GrpcResult{output: "1\ta\n2\tb\n", output_format: "", output_columns: []}
      columns = [%{name: "id", type: "UInt8"}, %{name: "name", type: "String"}]

      assert {:ok, result} =
               Result.from_grpc(later_chunk, format: "TabSeparated", columns: columns)

      assert result.rows == [["1", "a"], ["2", "b"]]
      assert result.columns == columns
      assert result.output_format == "TabSeparated"
    end

    test "a chunk's own format and columns win over the inherited ones" do
      first_chunk = %GrpcResult{
        output: "a,b\n",
        output_format: "CSV",
        output_columns: [
          %NameAndType{name: "x", type: "String"},
          %NameAndType{name: "y", type: "String"}
        ]
      }

      assert {:ok, result} = Result.from_grpc(first_chunk, format: "TabSeparated", columns: [])
      assert result.output_format == "CSV"
      assert result.rows == [["a", "b"]]
      assert result.columns == [%{name: "x", type: "String"}, %{name: "y", type: "String"}]
    end

    test "an unformatted chunk with nothing to inherit stays one opaque row" do
      chunk = %GrpcResult{output: "1\ta\n2\tb\n", output_format: ""}
      assert {:ok, result} = Result.from_grpc(chunk)
      assert result.rows == [["1\ta\n2\tb\n"]]
    end

    test "a trailers-only reply is an error, not a raise" do
      assert {:error, {:unexpected_reply, []}} = Result.from_grpc([])
    end
  end

  describe "log levels" do
    test "every LogsLevel member decodes to a named level" do
      # Guards a regenerated proto: a new enum member would otherwise decode
      # silently to :unknown (upstream added LOG_TEST = 9).
      for {name, _value} <- LogsLevel.mapping() do
        grpc_result = %GrpcResult{
          output: "",
          output_format: "TabSeparated",
          logs: [%LogEntry{level: name, time: 0, source: "src", text: "msg"}]
        }

        assert {:ok, %{logs: [%{level: level}]}} = Result.from_grpc(grpc_result)

        refute level == :unknown,
               "LogsLevel member #{inspect(name)} has no clause in log_level_to_atom/1"
      end
    end
  end

  # Mirrors the private writer in Huginn.Clickhouse.Stream by exercising it via
  # the public input_stream/3 (TabSeparated path is trivial; reuse CSV encoder).
  defp encode_csv_row(row) do
    [info] =
      Stream.input_stream("INSERT INTO t FORMAT CSV", [row], format: "CSV")
      |> Enum.to_list()

    info.input_data
  end
end
