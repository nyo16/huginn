defmodule Huginn.Clickhouse.IntegrationTest do
  @moduledoc """
  End-to-end tests against a live ClickHouse over gRPC.

  Excluded by default. Start ClickHouse first (`docker compose up -d --wait`)
  and run:

      mix test --include integration

  """
  use ExUnit.Case, async: false

  alias Huginn.Clickhouse.Pool

  @moduletag :integration

  @pool_opts [host: "localhost", port: 9100, auth: {:password, "default", ""}]

  setup_all do
    # Registers the pool under the default name, so calls need no `:pool`.
    start_supervised!({Pool, @pool_opts})
    :ok = Huginn.await_ready()
  end

  # Each test gets its own table, dropped by a per-test `on_exit`, which runs
  # while the module's pool is still up.
  setup do
    table = "huginn_it_#{System.unique_integer([:positive])}"

    {:ok, _} =
      Huginn.query(
        "CREATE TABLE #{table} (id UInt32, name String) ENGINE = MergeTree ORDER BY id"
      )

    on_exit(fn -> {:ok, _} = Huginn.query("DROP TABLE IF EXISTS #{table}") end)
    {:ok, table: table}
  end

  defp unique_query_id, do: "huginn-it-#{System.unique_integer([:positive])}"

  # Polls `system.processes` until the query is gone. KILL QUERY and stream
  # cancellation are asynchronous on the server.
  defp assert_query_finishes(query_id, attempts \\ 100) do
    sql = "SELECT count() FROM system.processes WHERE query_id = '#{query_id}'"

    case Huginn.query!(sql).rows do
      [["0"]] ->
        :ok

      _ when attempts > 0 ->
        Process.sleep(50)
        assert_query_finishes(query_id, attempts - 1)

      rows ->
        flunk("query #{query_id} is still running: #{inspect(rows)}")
    end
  end

  defp assert_query_running(query_id, attempts \\ 100) do
    sql = "SELECT count() FROM system.processes WHERE query_id = '#{query_id}'"

    case Huginn.query!(sql).rows do
      [["1"]] ->
        :ok

      _ when attempts > 0 ->
        Process.sleep(50)
        assert_query_running(query_id, attempts - 1)

      _ ->
        flunk("query #{query_id} never started")
    end
  end

  @doc false
  def forward_event(event, _measurements, %{query_id: id} = meta, %{pid: pid, query_id: id}),
    do: send(pid, {:event, event, meta})

  def forward_event(_event, _measurements, _metadata, _config), do: :ok

  describe "query/2" do
    test "returns rows" do
      assert {:ok, result} = Huginn.query("SELECT 1 AS one, 'a' AS letter")
      assert result.rows == [["1", "a"]]
    end

    test "ping/1" do
      assert :ok = Huginn.ping()
    end

    test "runs in the pool's configured database, unless the call overrides it" do
      {:ok, _} = Huginn.query("CREATE DATABASE IF NOT EXISTS huginn_it_db")
      on_exit(fn -> {:ok, _} = Huginn.query("DROP DATABASE IF EXISTS huginn_it_db") end)

      db_pool_opts = @pool_opts ++ [pool_name: :huginn_it_db_pool, database: "huginn_it_db"]
      start_supervised!({Pool, db_pool_opts})
      :ok = Huginn.await_ready(:huginn_it_db_pool, 10_000)

      assert Huginn.query!("SELECT currentDatabase()", pool: :huginn_it_db_pool).rows ==
               [["huginn_it_db"]]

      assert Huginn.query!("SELECT currentDatabase()").rows == [["default"]]

      assert Huginn.query!("SELECT currentDatabase()",
               pool: :huginn_it_db_pool,
               database: "default"
             ).rows == [["default"]]
    end

    test "returns a :server error with the ClickHouse code" do
      assert {:error, %Huginn.Error{reason: :server, code: 62} = error} = Huginn.query("SELEC 1")
      assert Exception.message(error) =~ "Syntax error"
    end

    test "query!/2 raises the server error" do
      assert_raise Huginn.Error, ~r/Syntax error/, fn -> Huginn.query!("SELEC 1") end
    end

    test "returns a :timeout error once the deadline passes" do
      assert {:error, %Huginn.Error{reason: :timeout}} =
               Huginn.query("SELECT sleep(2)", timeout: 200)
    end
  end

  describe "inserts" do
    test "insert/3 then read back", %{table: table} do
      assert {:ok, _} =
               Huginn.insert("INSERT INTO #{table} FORMAT TabSeparated", "1\tneo\n2\ttrinity\n")

      assert Huginn.query!("SELECT id, name FROM #{table} ORDER BY id").rows ==
               [["1", "neo"], ["2", "trinity"]]
    end

    test "insert_stream/3 sends one message per chunk", %{table: table} do
      sql = "INSERT INTO #{table} FORMAT TabSeparated"
      rows = [[1, "one"], [2, "tab\there"], [3, "three"]]

      # Every row is at least 1 byte, so each one fills a message of its own:
      # the continuation path really runs.
      messages = Huginn.Clickhouse.Stream.input_stream(sql, rows, chunk_size: 1)
      assert Enum.count(messages) == 3

      assert {:ok, _} = Huginn.insert_stream(sql, rows, chunk_size: 1)

      assert Huginn.query!("SELECT id, name FROM #{table} ORDER BY id").rows ==
               [["1", "one"], ["2", "tab\there"], ["3", "three"]]
    end

    test "insert_stream/3 commits nothing when the source raises", %{table: table} do
      data =
        Stream.map(1..3, fn
          3 -> raise "source failed"
          i -> [i, "row#{i}"]
        end)

      assert_raise RuntimeError, "source failed", fn ->
        Huginn.insert_stream("INSERT INTO #{table} FORMAT TabSeparated", data, chunk_size: 1)
      end

      assert Huginn.query!("SELECT count() FROM #{table}").rows == [["0"]]
    end
  end

  describe "streaming results" do
    test "stream_rows/2 and stream_maps/2", %{table: table} do
      rows = Enum.map_join(1..50, fn i -> "#{i}\tn#{i}\n" end)
      assert {:ok, _} = Huginn.insert("INSERT INTO #{table} FORMAT TabSeparated", rows)

      assert "SELECT id, name FROM #{table}" |> Huginn.stream_rows() |> Enum.count() == 50

      assert [%{"id" => 1, "name" => "n1"}] =
               "SELECT id, name FROM #{table} ORDER BY id LIMIT 1"
               |> Huginn.stream_maps(format: "JSONEachRow")
               |> Enum.to_list()
    end

    test "does not lose rows across multiple Result chunks" do
      # 200k rows is large enough that ClickHouse splits the response into
      # several Result messages. Only the first carries `output_format` and
      # `output_columns`, so the later ones must be decoded with those.
      n = 200_000

      assert "SELECT number FROM system.numbers LIMIT #{n}"
             |> Huginn.stream_rows()
             |> Enum.count() == n
    end

    test "yields a server error as the last element" do
      assert [{:error, %Huginn.Error{reason: :server, code: 62}}] =
               "SELEC 1" |> Huginn.stream_query() |> Enum.to_list()
    end

    test "stopping early cancels the query on the server" do
      query_id = unique_query_id()

      :telemetry.attach(
        query_id,
        [:huginn, :query, :stop],
        &__MODULE__.forward_event/4,
        %{pid: self(), query_id: query_id}
      )

      on_exit(fn -> :telemetry.detach(query_id) end)

      # An endless query: only cancellation can end it.
      assert [{:ok, _}] =
               "SELECT number FROM system.numbers"
               |> Huginn.stream_query(query_id: query_id)
               |> Enum.take(1)

      assert_received {:event, [:huginn, :query, :stop], %{halted: true}}
      assert_query_finishes(query_id)
    end

    test "a consumer that dies mid-stream cancels the query" do
      query_id = unique_query_id()
      test_pid = self()

      consumer =
        spawn(fn ->
          "SELECT number FROM system.numbers"
          |> Huginn.stream_query(query_id: query_id)
          |> Enum.each(fn _ ->
            send(test_pid, :consuming)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :consuming, 5_000
      Process.exit(consumer, :kill)

      assert_query_finishes(query_id)
    end
  end

  describe "stream_io/3" do
    test "streams input into an INSERT ... SELECT FROM input()", %{table: table} do
      sql =
        "INSERT INTO #{table} SELECT x, toString(x * 2) " <>
          "FROM input('x UInt32') FORMAT TabSeparated"

      replies =
        sql
        |> Huginn.stream_io(Stream.map(1..3, &[&1]), chunk_size: 1)
        |> Enum.to_list()

      assert replies != []
      assert Enum.all?(replies, &match?({:ok, %Huginn.Clickhouse.Result{}}, &1))

      assert Huginn.query!("SELECT id, name FROM #{table} ORDER BY id").rows ==
               [["1", "2"], ["2", "4"], ["3", "6"]]
    end

    test "yields a server error as the last element" do
      assert [{:error, %Huginn.Error{reason: :server}}] =
               "INSERT INTO huginn_it_no_such_table FORMAT TabSeparated"
               |> Huginn.stream_io([[1]])
               |> Enum.to_list()
    end
  end

  describe "cancel/2" do
    test "kills a running query" do
      query_id = unique_query_id()

      # Endless until killed; max_execution_time bounds it if cancel/2 fails.
      task =
        Task.async(fn ->
          Huginn.query("SELECT count() FROM system.numbers",
            query_id: query_id,
            settings: %{"max_execution_time" => "30"}
          )
        end)

      assert_query_running(query_id)
      assert :ok = Huginn.cancel(query_id)

      Task.await(task, 10_000)
      assert_query_finishes(query_id)
    end
  end
end
