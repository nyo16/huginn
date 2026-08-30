defmodule Huginn.Clickhouse.IntegrationTest do
  @moduledoc """
  End-to-end tests against a live ClickHouse over gRPC.

  Excluded by default. Start ClickHouse first (`docker-compose up -d`) and run:

      mix test --include integration

  These cover the request/response and (previously broken) streaming paths:
  `query/2`, `insert/3`, `insert_stream/3`, `stream_query/2`, `stream_maps/2`,
  and `stream_io/1`.
  """
  use ExUnit.Case, async: false

  alias Huginn.Clickhouse.{Config, Query}

  @moduletag :integration

  @table "huginn_it_#{System.system_time(:millisecond)}"

  setup_all do
    config = Config.new(host: "localhost", port: 9100, auth: {:password, "default", ""})
    pool_config = Config.to_pool_config(config)

    start_supervised!({GrpcConnectionPool, pool_config})

    if function_exported?(GrpcConnectionPool, :await_ready, 2) do
      GrpcConnectionPool.await_ready(:clickhouse_pool, 10_000)
    end

    {:ok, _} =
      Huginn.query("""
      CREATE TABLE IF NOT EXISTS #{@table} (id UInt32, name String)
      ENGINE = MergeTree ORDER BY id
      """)

    on_exit(fn -> Huginn.query("DROP TABLE IF EXISTS #{@table}") end)
    :ok
  end

  setup do
    {:ok, _} = Huginn.query("TRUNCATE TABLE #{@table}")
    :ok
  end

  test "query/2 returns rows" do
    assert {:ok, result} = Huginn.query("SELECT 1 AS one, 'a' AS letter", format: "TabSeparated")
    assert result.rows == [["1", "a"]]
  end

  test "ping/1" do
    assert :ok = Huginn.ping()
  end

  test "insert/3 then read back" do
    assert {:ok, _} =
             Huginn.insert("INSERT INTO #{@table} FORMAT TabSeparated", "1\tneo\n2\ttrinity")

    assert {:ok, result} =
             Huginn.query("SELECT id, name FROM #{@table} ORDER BY id", format: "TabSeparated")

    assert result.rows == [["1", "neo"], ["2", "trinity"]]
  end

  test "insert_stream/3 streams multiple chunks (client-streaming)" do
    chunks = ["1\tone\n", "2\ttwo\n", "3\tthree\n"]

    assert {:ok, _} =
             Huginn.insert_stream("INSERT INTO #{@table} FORMAT TabSeparated", chunks)

    assert {:ok, result} = Huginn.query("SELECT count() FROM #{@table}", format: "TabSeparated")
    assert result.rows == [["3"]]
  end

  test "stream_query/2 and stream_maps/2 (server-streaming)" do
    rows = Enum.map_join(1..50, "\n", fn i -> "#{i}\tn#{i}" end)
    assert {:ok, _} = Huginn.insert("INSERT INTO #{@table} FORMAT TabSeparated", rows)

    count =
      "SELECT id, name FROM #{@table}"
      |> Huginn.stream_rows(format: "TabSeparated")
      |> Enum.count()

    assert count == 50

    maps =
      "SELECT id, name FROM #{@table} ORDER BY id LIMIT 1"
      |> Huginn.stream_maps(format: "JSONEachRow")
      |> Enum.to_list()

    assert [%{"id" => _, "name" => _}] = maps
  end

  test "stream_query/2 does not lose rows across multiple Result chunks" do
    # 200k rows is large enough that ClickHouse splits the response into several
    # Result messages. Only the first carries `output_format`/`output_columns`,
    # so decoding each chunk in isolation collapsed every later chunk into a
    # single opaque row and silently dropped ~2/3 of the rows.
    n = 200_000

    rows =
      "SELECT number FROM system.numbers LIMIT #{n}"
      |> Huginn.stream_rows(format: "TabSeparated")
      |> Enum.count()

    assert rows == n
  end

  test "stream_query/2 surfaces a connection failure instead of an empty stream" do
    items =
      "SELECT 1"
      |> Huginn.stream_query(pool: :huginn_no_such_pool)
      |> Enum.to_list()

    assert [{:error, _}] = items
  end

  test "stream_io/1 bidirectional send/recv" do
    {output, send} = Huginn.stream_io()

    send.(Query.build("SELECT 1 AS n", format: "TabSeparated"))

    assert [{:ok, result}] = Enum.take(output, 1)
    assert result.rows == [["1"]]
  end
end
