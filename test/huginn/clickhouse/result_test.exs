defmodule Huginn.Clickhouse.ResultTest do
  use ExUnit.Case, async: true

  alias Clickhouse.Grpc.{Exception, LogEntry, LogsLevel, NameAndType}
  alias Clickhouse.Grpc.Result, as: GrpcResult
  alias Huginn.Clickhouse.Result

  doctest Result

  describe "parse_output/3 empty output" do
    test "empty and nil output decode to no rows in any format" do
      for format <- ["TabSeparated", "CSV", "JSONEachRow", "JSONCompactEachRow", "Unknown"] do
        assert Result.parse_output("", format) == {:ok, []}
        assert Result.parse_output(nil, format) == {:ok, []}
      end
    end
  end

  describe "parse_output/3 TabSeparated" do
    test "splits rows and fields" do
      assert Result.parse_output("a\tb\tc\n1\t2\t3\n", "TabSeparated") ==
               {:ok, [["a", "b", "c"], ["1", "2", "3"]]}
    end

    test "TSV is an alias" do
      assert Result.parse_output("a\tb\n", "TSV") == {:ok, [["a", "b"]]}
    end

    test "a last row without a trailing newline is kept" do
      assert Result.parse_output("a\tb\nc\td", "TabSeparated") == {:ok, [["a", "b"], ["c", "d"]]}
    end

    test "keeps empty fields and empty rows" do
      assert Result.parse_output("\t\n\n", "TabSeparated") == {:ok, [["", ""], [""]]}
    end

    test "decodes the escape sequences ClickHouse writes" do
      output = Enum.join([~S(a\tb), ~S(c\nd), ~S(e\\f), ~S(\'q\'), ~S(\0\b\f\r)], "\t") <> "\n"

      assert Result.parse_output(output, "TabSeparated") ==
               {:ok, [["a\tb", "c\nd", "e\\f", "'q'", <<0, ?\b, ?\f, ?\r>>]]}
    end

    test "decodes \\xHH and the remaining single-character escapes" do
      output = Enum.join([~S(\x41\xff), ~S(\a\e\v), ~S(\"\`\/\=)], "\t") <> "\n"

      assert Result.parse_output(output, "TabSeparated") ==
               {:ok, [[<<?A, 255>>, "\a\e\v", ~s("`/=)]]}
    end

    test "keeps the backslash before characters ClickHouse does not unescape" do
      assert Result.parse_output(~S(100\%) <> "\n", "TabSeparated") == {:ok, [[~S(100\%)]]}
    end

    test "a backslash before a raw tab or newline yields that byte inside the field" do
      assert Result.parse_output("a\\\tb\\\nc\n", "TabSeparated") == {:ok, [["a\tb\nc"]]}
    end

    test "a field that is exactly \\N is nil" do
      assert Result.parse_output("\\N\ta\t\\N\n\\N\n", "TabSeparated") ==
               {:ok, [[nil, "a", nil], [nil]]}

      assert Result.parse_output("\\N", "TabSeparated") == {:ok, [[nil]]}
    end

    test "an escaped backslash followed by N is the literal string, not nil" do
      assert Result.parse_output("\\\\N\n", "TabSeparated") == {:ok, [["\\N"]]}
    end

    test "\\N inside a field decodes to nothing, as in ClickHouse" do
      assert Result.parse_output("a\\Nb\n", "TabSeparated") == {:ok, [["ab"]]}
    end

    test "non-UTF-8 bytes pass through" do
      assert Result.parse_output(<<255, 0, ?\t, 0xC3, ?\n>>, "TabSeparated") ==
               {:ok, [[<<255, 0>>, <<0xC3>>]]}
    end

    test "a trailing lone backslash is a decode error" do
      assert {:error, %Huginn.Error{reason: :decode, details: %{offset: 1}}} =
               Result.parse_output("a\\", "TabSeparated")
    end

    test "a malformed \\x escape is a decode error" do
      assert {:error, %Huginn.Error{reason: :decode}} =
               Result.parse_output("\\xZZ\n", "TabSeparated")

      assert {:error, %Huginn.Error{reason: :decode}} =
               Result.parse_output("\\x4", "TabSeparated")
    end
  end

  describe "parse_output/3 CSV" do
    test "parses simple unquoted fields" do
      assert Result.parse_output("a,b,c\n1,2,3", "CSV") ==
               {:ok, [["a", "b", "c"], ["1", "2", "3"]]}
    end

    test "keeps commas inside quoted fields" do
      assert Result.parse_output(~s("a,b",c\n), "CSV") == {:ok, [["a,b", "c"]]}
    end

    test "decodes doubled quotes inside a quoted field" do
      assert Result.parse_output(~s("she said ""hi""",x), "CSV") ==
               {:ok, [[~s(she said "hi"), "x"]]}
    end

    test "handles empty fields and empty quoted fields" do
      assert Result.parse_output(~s(,"",a\n), "CSV") == {:ok, [["", "", "a"]]}
    end

    test "quoted fields may contain LF and CRLF" do
      output = ~s("a\nb",c\n"d\r\ne",f\n)
      assert Result.parse_output(output, "CSV") == {:ok, [["a\nb", "c"], ["d\r\ne", "f"]]}
    end

    test "accepts CRLF row endings" do
      assert Result.parse_output(~s("a",1\r\n"b",2\r\n), "CSV") == {:ok, [["a", "1"], ["b", "2"]]}
    end

    test "unquoted \\N is nil, quoted \\N is the string" do
      assert Result.parse_output(~S(\N,"\N",x) <> "\n", "CSV") == {:ok, [[nil, "\\N", "x"]]}
    end

    test "non-UTF-8 bytes pass through" do
      assert Result.parse_output(<<?", 255, 0, ?", ?,, 0xC3, ?\n>>, "CSV") ==
               {:ok, [[<<255, 0>>, <<0xC3>>]]}
    end

    test "CSVWithNames returns the header as the first row" do
      assert Result.parse_output(~s("x","y"\n1,"a"\n), "CSVWithNames") ==
               {:ok, [["x", "y"], ["1", "a"]]}
    end

    test "an unterminated quoted field is a decode error" do
      assert {:error, %Huginn.Error{reason: :decode, message: message}} =
               Result.parse_output(~s("abc\n), "CSV")

      assert message =~ "unterminated"
    end

    test "data after a closing quote is a decode error" do
      assert {:error, %Huginn.Error{reason: :decode, details: %{format: "CSV", offset: 3}}} =
               Result.parse_output(~s("a"b\n), "CSV")
    end
  end

  describe "parse_output/3 JSONEachRow" do
    test "extracts values in column order regardless of JSON key order" do
      columns = [%{name: "id", type: "UInt8"}, %{name: "name", type: "String"}]
      # Keys deliberately out of column order.
      output = ~s({"name":"alice","id":1}\n{"name":"bob","id":2}\n)

      assert Result.parse_output(output, "JSONEachRow", columns) ==
               {:ok, [[1, "alice"], [2, "bob"]]}
    end

    test "falls back to map order when no columns given (2-arity)" do
      assert Result.parse_output(~s({"a":1,"b":2}), "JSONEachRow") == {:ok, [[1, 2]]}
    end

    test "fills nil for a column missing from the JSON object" do
      columns = [%{name: "id", type: "UInt8"}, %{name: "name", type: "String"}]
      assert Result.parse_output(~s({"id":1}), "JSONEachRow", columns) == {:ok, [[1, nil]]}
    end

    test "invalid JSON is a decode error, not a raise" do
      assert {:error, %Huginn.Error{reason: :decode, details: %Jason.DecodeError{}}} =
               Result.parse_output(~s({"a":1}\n{"a":\n), "JSONEachRow")
    end

    test "a line that is not an object is a decode error" do
      assert {:error, %Huginn.Error{reason: :decode}} =
               Result.parse_output("[1,2]\n", "JSONEachRow")
    end
  end

  describe "parse_output/3 JSONCompactEachRow" do
    test "each line is a row" do
      assert Result.parse_output(~s([1,"a",null]\n[2,"b",true]\n), "JSONCompactEachRow") ==
               {:ok, [[1, "a", nil], [2, "b", true]]}
    end

    test "a line that is not an array is a decode error" do
      assert {:error, %Huginn.Error{reason: :decode}} =
               Result.parse_output(~s({"a":1}\n), "JSONCompactEachRow")
    end
  end

  describe "parse_output/3 unknown formats" do
    test "returns the raw output as a single row" do
      assert Result.parse_output("anything\tat\nall", "Pretty") == {:ok, [["anything\tat\nall"]]}
    end
  end

  describe "from_grpc/2" do
    test "a ClickHouse exception becomes a :server error carrying its code" do
      exception = %Exception{code: 62, name: "SYNTAX_ERROR", display_text: "Syntax error"}

      assert {:error, %Huginn.Error{reason: :server, code: 62, details: ^exception}} =
               Result.from_grpc(%GrpcResult{exception: exception})
    end

    test "an exception with code 0 is not an error" do
      assert {:ok, %Result{rows: [["1"]]}} =
               Result.from_grpc(%GrpcResult{
                 exception: %Exception{code: 0},
                 output: "1\n",
                 output_format: "TabSeparated"
               })
    end

    test "a trailers-only reply is an :unexpected_reply error, not a raise" do
      assert {:error, %Huginn.Error{reason: :unexpected_reply, details: []}} =
               Result.from_grpc([])
    end

    test "undecodable output is a :decode error" do
      assert {:error, %Huginn.Error{reason: :decode}} =
               Result.from_grpc(%GrpcResult{output: "{oops\n", output_format: "JSONEachRow"})
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
  end

  describe "to_maps/1" do
    test "converts rows to maps keyed by column name" do
      result = %Result{
        columns: [%{name: "name", type: "String"}, %{name: "age", type: "UInt8"}],
        rows: [["alice", "25"], ["bob", nil]]
      }

      assert Result.to_maps(result) == [
               %{"name" => "alice", "age" => "25"},
               %{"name" => "bob", "age" => nil}
             ]
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
end
