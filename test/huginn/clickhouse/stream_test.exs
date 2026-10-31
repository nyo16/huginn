defmodule Huginn.Clickhouse.StreamTest do
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.{Result, Stream}

  @sql "INSERT INTO t FORMAT TabSeparated"

  # Encodes rows into a single message (chunk_size larger than any test data)
  # and returns its input_data.
  defp encode(rows, format) do
    [info] =
      @sql
      |> Stream.input_stream(rows, format: format, chunk_size: 1_000_000_000)
      |> Enum.to_list()

    info.input_data
  end

  defp data_sizes(elements, chunk_size) do
    @sql
    |> Stream.input_stream(elements, chunk_size: chunk_size)
    |> Enum.map(&byte_size(&1.input_data))
  end

  defp round_trip(rows, format) do
    Result.parse_output(encode(rows, format), format)
  end

  @adversarial [
    "",
    "plain",
    "tab\there",
    "new\nline",
    "crlf\r\nend",
    "cr\ronly",
    "back\\slash",
    "trailing\\",
    "\\",
    "\\N",
    "N",
    "\\t literal",
    ~s(double "quotes"),
    ~s("),
    ~s(""),
    "single 'quotes'",
    "comma,separated",
    " leading and trailing spaces ",
    <<0>>,
    "\b\f\a\e\v",
    <<255, 0>>,
    <<0xC3>>,
    "unicode: żółć 日本 🎉",
    nil
  ]

  describe "TSV and CSV round-trip" do
    for format <- ["TabSeparated", "CSV"] do
      test "#{format}: every adversarial value survives in every column position" do
        rows =
          for value <- @adversarial do
            [value, "x", value]
          end

        assert round_trip(rows, unquote(format)) == {:ok, rows}
      end

      test "#{format}: a single-column row of each adversarial value survives" do
        for value <- @adversarial do
          assert round_trip([[value]], unquote(format)) == {:ok, [[value]]},
                 "value #{inspect(value)} did not round-trip"
        end
      end

      test "#{format}: deterministic random rows round-trip" do
        :rand.seed(:exsss, {1, 2, 3})
        alphabet = ~c"\t\n\r\\\",'N ab" ++ [0, 8, 12, 255, 0xC3]

        rows =
          for _ <- 1..500 do
            for _ <- 1..:rand.uniform(4) do
              if :rand.uniform(10) == 1 do
                nil
              else
                for _ <- 1..(:rand.uniform(13) - 1)//1, into: <<>>, do: <<Enum.random(alphabet)>>
              end
            end
          end

        assert round_trip(rows, unquote(format)) == {:ok, rows}
      end
    end
  end

  describe "TabSeparated encoding" do
    test "escapes like ClickHouse and writes NULL as \\N" do
      assert encode([["a\tb", "c\nd\r", "e\\'f", <<0, ?\b, ?\f>>, nil]], "TabSeparated") ==
               Enum.join([~S(a\tb), ~S(c\nd\r), ~S(e\\\'f), ~S(\0\b\f), ~S(\N)], "\t") <> "\n"
    end

    test "non-binary scalars go through to_string/1" do
      assert encode([[1, 2.5, true, :atom]], "TSV") == "1\t2.5\ttrue\tatom\n"
    end

    test "nested lists are rejected" do
      assert_raise ArgumentError, ~r/nested lists and maps/, fn -> encode([[[1, 2]]], "TSV") end
    end
  end

  describe "CSV encoding" do
    test "always quotes binaries, doubles quotes, writes numbers and NULL bare" do
      assert encode([["a", "", ~s(x"y), 1, 2.5, nil]], "CSV") ==
               ~s("a","","x""y",1,2.5,\\N\n)
    end
  end

  describe "JSON encoding" do
    test "JSONEachRow encodes maps as objects" do
      assert encode([%{"id" => 1}, %{"id" => 2}], "JSONEachRow") == ~s({"id":1}\n{"id":2}\n)
    end

    test "JSONEachRow rejects list rows and points to JSONCompactEachRow" do
      assert_raise ArgumentError, ~r/JSONCompactEachRow/, fn ->
        encode([[1, 2]], "JSONEachRow")
      end
    end

    test "JSONCompactEachRow encodes lists as arrays and round-trips" do
      rows = [[1, "a\tb", nil], [2, "", true]]
      assert encode(rows, "JSONCompactEachRow") == ~s([1,"a\\tb",null]\n[2,"",true]\n)
      assert round_trip(rows, "JSONCompactEachRow") == {:ok, rows}
    end

    test "an unsupported format for structured rows raises" do
      assert_raise ArgumentError, ~r/cannot encode/, fn -> encode([["a"]], "Parquet") end
    end
  end

  describe "input_stream/3 messages" do
    test "the first message carries the query, later ones are continuations" do
      messages =
        @sql
        |> Stream.input_stream([["a"], ["b"], ["c"]], chunk_size: 1)
        |> Enum.to_list()

      assert [first | rest] = messages
      assert first.query == @sql
      assert first.input_data == "a\n"
      assert Enum.map(rest, & &1.input_data) == ["b\n", "c\n"]
      assert Enum.all?(rest, &(&1.query == "" and &1.next_query_info))
    end

    test "an empty source still sends the query with empty data" do
      assert [%{query: @sql, input_data: ""}] = @sql |> Stream.input_stream([]) |> Enum.to_list()
    end

    test "binaries are passed through verbatim whatever the format" do
      chunks = ["raw\t\\x\n", "more\n"]

      assert [%{input_data: "raw\t\\x\nmore\n"}] =
               @sql |> Stream.input_stream(chunks, format: "CSV") |> Enum.to_list()
    end

    test "rejects a non-positive chunk_size" do
      assert_raise ArgumentError, ~r/chunk_size/, fn ->
        Stream.input_stream(@sql, [], chunk_size: 0)
      end
    end
  end

  describe "input_stream/3 chunking by bytes" do
    test "accumulates elements until the chunk reaches chunk_size" do
      elements = List.duplicate("12345", 7)
      assert data_sizes(elements, 10) == [10, 10, 10, 5]
      assert data_sizes(elements, 12) == [15, 15, 5]
    end

    test "rows are counted by their encoded size" do
      # "ab\tcd\n" is 6 bytes; the escaped "a\\b\n" is 5.
      assert data_sizes([["ab", "cd"], ["ab", "cd"], ["a\\b"]], 11) == [12, 5]
    end

    test "an oversized element is sent alone and never split" do
      big = String.duplicate("x", 100)

      messages = Stream.input_stream(@sql, ["aa", "bb", big, "cc"], chunk_size: 10)
      assert Enum.map(messages, & &1.input_data) == ["aabb", big, "cc"]
    end

    test "an oversized first element flushes nothing before it" do
      big = String.duplicate("x", 20)
      assert data_sizes([big, "a"], 10) == [20, 1]
    end

    test "the source is enumerated once and lazily" do
      parent = self()

      source =
        1..1_000_000
        |> Elixir.Stream.each(&send(parent, {:pulled, &1}))
        |> Elixir.Stream.map(&[Integer.to_string(&1)])

      messages = Stream.input_stream(@sql, source, chunk_size: 4)

      # Each row "N\n" is 2 bytes for 1..9, so two rows fill one message.
      assert [m1, m2] = Enum.take(messages, 2)
      assert {m1.input_data, m2.input_data} == {"1\n2\n", "3\n4\n"}

      for n <- 1..4, do: assert_received({:pulled, ^n})
      refute_received {:pulled, 5}
    end
  end
end
