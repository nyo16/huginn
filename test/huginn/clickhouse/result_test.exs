defmodule Huginn.Clickhouse.ResultTest do
  use ExUnit.Case, async: true

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

  # Mirrors the private writer in Huginn.Clickhouse.Stream by exercising it via
  # the public input_stream/3 (TabSeparated path is trivial; reuse CSV encoder).
  defp encode_csv_row(row) do
    [info] =
      Stream.input_stream("INSERT INTO t FORMAT CSV", [row], format: "CSV")
      |> Enum.to_list()

    info.input_data
  end
end
