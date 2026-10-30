defmodule Huginn.Clickhouse.CodecIntegrationTest do
  @moduledoc """
  Round-trips adversarial String / Nullable(String) values through a live
  ClickHouse: encoded by `Huginn.Clickhouse.Stream`, parsed by the server,
  written back by the server and decoded by `Huginn.Clickhouse.Result`.

  Excluded by default; needs `docker compose up -d --wait` and
  `mix test --include integration`.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Huginn.Clickhouse.Pool

  @pool :huginn_codec_pool

  @values [
    "",
    "plain",
    "tab\there",
    "new\nline",
    "crlf\r\nend",
    "cr\ronly",
    "back\\slash",
    "trailing\\",
    "\\N",
    "\\t literal",
    ~s(double "quotes" and ""),
    "single 'quotes'",
    "comma,separated",
    " padded ",
    <<0>>,
    "\b\f\a\e\v",
    <<255, 0>>,
    "unicode: żółć 日本 🎉"
  ]

  setup_all do
    start_supervised!(
      {Pool,
       host: "localhost",
       port: 9100,
       auth: {:password, "default", ""},
       pool_name: @pool,
       pool_size: 1}
    )

    :ok = Pool.await_ready(@pool, 10_000)
    :ok
  end

  # Every value appears in the non-nullable column; the nullable column holds
  # the same value or, on every third row, NULL.
  defp rows do
    @values
    |> Enum.with_index()
    |> Enum.map(fn {value, id} -> [id, value, if(rem(id, 3) == 0, do: nil, else: value)] end)
  end

  defp with_table(fun) do
    table = "huginn_codec_#{System.unique_integer([:positive])}"

    {:ok, _} =
      Huginn.query(
        "CREATE TABLE #{table} (id UInt32, s String, n Nullable(String)) ENGINE = Memory",
        pool: @pool
      )

    try do
      fun.(table)
    after
      Huginn.query("DROP TABLE IF EXISTS #{table}", pool: @pool)
    end
  end

  for format <- ["TabSeparated", "CSV"] do
    test "#{format}: insert_stream then query returns the exact values, NULL as nil" do
      format = unquote(format)

      with_table(fn table ->
        # A small chunk_size forces many continuation messages.
        assert {:ok, _} =
                 Huginn.insert_stream("INSERT INTO #{table} FORMAT #{format}", rows(),
                   format: format,
                   chunk_size: 64,
                   pool: @pool
                 )

        expected = Enum.map(rows(), fn [id, s, n] -> [Integer.to_string(id), s, n] end)

        for read_format <- ["TabSeparated", "CSV"] do
          assert {:ok, result} =
                   Huginn.query("SELECT id, s, n FROM #{table} ORDER BY id",
                     format: read_format,
                     pool: @pool
                   )

          assert result.rows == expected,
                 "inserted as #{format}, read as #{read_format}"
        end
      end)
    end
  end

  test "ClickHouse's own TabSeparated escaping decodes correctly" do
    assert {:ok, result} =
             Huginn.query(~S(SELECT 'a\tb', NULL, 'x\\y\ny', '\\N'),
               format: "TabSeparated",
               pool: @pool
             )

    assert result.rows == [["a\tb", nil, "x\\y\ny", "\\N"]]
  end

  test "ClickHouse's own CSV quoting decodes correctly" do
    assert {:ok, result} =
             Huginn.query(~S(SELECT 'a,"b"', NULL, 'x\r\ny', '\\N', 42),
               format: "CSV",
               pool: @pool
             )

    assert result.rows == [[~s(a,"b"), nil, "x\r\ny", "\\N", "42"]]
  end
end
