defmodule Huginn.Clickhouse.ClientTest do
  @moduledoc """
  Client behavior that needs no ClickHouse server: configuration errors,
  connection failures, error raising and retries.
  """
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.Pool

  # Port 1 refuses connections, so the pool starts but never gets a channel.
  defp start_unreachable_pool do
    pool = :"client_test_#{System.unique_integer([:positive])}"
    start_supervised!({Pool, host: "127.0.0.1", port: 1, pool_size: 1, pool_name: pool})
    pool
  end

  defp unique_pool, do: :"client_test_unknown_#{System.unique_integer([:positive])}"

  defp unique_query_id, do: "client-test-#{System.unique_integer([:positive])}"

  # Forwards events for one query id to the test process. Handlers are global
  # and this module runs concurrently with others, hence the filter.
  defp forward_events(events, query_id) do
    handler = "client-test-#{query_id}"

    :telemetry.attach_many(handler, events, &__MODULE__.forward_event/4, %{
      pid: self(),
      query_id: query_id
    })

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  @doc false
  def forward_event(event, measurements, %{query_id: id} = metadata, %{pid: pid, query_id: id}) do
    send(pid, {:event, event, measurements, metadata})
  end

  def forward_event(_event, _measurements, _metadata, _config), do: :ok

  describe "unknown pool" do
    setup do
      {:ok, pool: unique_pool()}
    end

    test "query/2 returns :not_configured", %{pool: pool} do
      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Huginn.query("SELECT 1", pool: pool)
    end

    test "insert/3 returns :not_configured", %{pool: pool} do
      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Huginn.insert("INSERT INTO t FORMAT TabSeparated", "1\n", pool: pool)
    end

    test "insert_stream/3 returns :not_configured", %{pool: pool} do
      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Huginn.insert_stream("INSERT INTO t FORMAT TabSeparated", [["1"]], pool: pool)
    end

    test "insert_stream/3 does not enumerate the data", %{pool: pool} do
      data = Stream.map([["1"]], fn _ -> flunk("data was enumerated") end)

      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Huginn.insert_stream("INSERT INTO t FORMAT TabSeparated", data, pool: pool)
    end

    test "stream_query/2 yields :not_configured as its only element", %{pool: pool} do
      assert [{:error, %Huginn.Error{reason: :not_configured}}] =
               "SELECT 1" |> Huginn.stream_query(pool: pool) |> Enum.to_list()
    end

    test "stream_io/3 yields :not_configured as its only element", %{pool: pool} do
      assert [{:error, %Huginn.Error{reason: :not_configured}}] =
               "SELECT * FROM input('x UInt32') FORMAT TabSeparated"
               |> Huginn.stream_io([[1]], pool: pool)
               |> Enum.to_list()
    end

    test "ping/1, cancel/2 and running_queries/1 return :not_configured", %{pool: pool} do
      assert {:error, %Huginn.Error{reason: :not_configured}} = Huginn.ping(pool: pool)
      assert {:error, %Huginn.Error{reason: :not_configured}} = Huginn.cancel("id", pool: pool)

      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Huginn.running_queries(pool: pool)
    end

    test "query!/2 raises Huginn.Error", %{pool: pool} do
      error = assert_raise Huginn.Error, fn -> Huginn.query!("SELECT 1", pool: pool) end
      assert error.reason == :not_configured
    end

    test "stream_rows/2 and stream_maps/2 raise instead of yielding nothing", %{pool: pool} do
      assert_raise Huginn.Error, fn ->
        "SELECT 1" |> Huginn.stream_rows(pool: pool) |> Enum.to_list()
      end

      assert_raise Huginn.Error, fn ->
        "SELECT 1" |> Huginn.stream_maps(pool: pool) |> Enum.to_list()
      end
    end

    test "await_ready/2 returns :not_configured", %{pool: pool} do
      assert {:error, %Huginn.Error{reason: :not_configured}} = Huginn.await_ready(pool, 10)
    end
  end

  describe "unreachable pool" do
    setup do
      {:ok, pool: start_unreachable_pool()}
    end

    test "query/2 returns a :connection error", %{pool: pool} do
      assert {:error, %Huginn.Error{reason: :connection}} = Huginn.query("SELECT 1", pool: pool)
    end

    test "stream_query/2 yields a :connection error", %{pool: pool} do
      assert [{:error, %Huginn.Error{reason: :connection}}] =
               "SELECT 1" |> Huginn.stream_query(pool: pool) |> Enum.to_list()
    end

    test "await_ready/2 gives up with a :connection error", %{pool: pool} do
      assert {:error, %Huginn.Error{reason: :connection}} = Huginn.await_ready(pool, 50)
    end

    test "query/2 makes exactly 1 + :retries attempts", %{pool: pool} do
      query_id = unique_query_id()
      forward_events([[:huginn, :query, :retry], [:huginn, :query, :stop]], query_id)

      assert {:error, %Huginn.Error{reason: :connection}} =
               Huginn.query("SELECT 1",
                 pool: pool,
                 query_id: query_id,
                 retries: 2,
                 retry_backoff: 1
               )

      assert_received {:event, [:huginn, :query, :retry], %{attempt: 1}, %{method: :query}}
      assert_received {:event, [:huginn, :query, :retry], %{attempt: 2}, %{method: :query}}
      refute_received {:event, [:huginn, :query, :retry], _, _}

      # One span covers all attempts.
      assert_received {:event, [:huginn, :query, :stop], _, %{error: %{reason: :connection}}}
      refute_received {:event, [:huginn, :query, :stop], _, _}
    end

    test "insert/3 retries only channel checkout, never the request", %{pool: pool} do
      query_id = unique_query_id()
      forward_events([[:huginn, :query, :retry]], query_id)
      rpc_handler = "client-test-rpc-#{query_id}"
      test_pid = self()

      # grpc emits this whenever a request is actually issued on a channel.
      :telemetry.attach(
        rpc_handler,
        [:grpc, :client, :rpc, :start],
        &__MODULE__.forward_rpc/4,
        test_pid
      )

      on_exit(fn -> :telemetry.detach(rpc_handler) end)

      assert {:error, %Huginn.Error{reason: :connection}} =
               Huginn.insert("INSERT INTO t FORMAT TabSeparated", "1\n",
                 pool: pool,
                 query_id: query_id,
                 retries: 2,
                 retry_backoff: 1
               )

      assert_received {:event, [:huginn, :query, :retry], %{attempt: 1}, %{method: :insert}}
      assert_received {:event, [:huginn, :query, :retry], %{attempt: 2}, %{method: :insert}}
      refute_received {:event, [:huginn, :query, :retry], _, _}
      refute_received :grpc_rpc_started
    end

    test "insert_stream/3 does not enumerate the data when no channel is available",
         %{pool: pool} do
      data = Stream.map([["1"]], fn _ -> flunk("data was enumerated") end)

      assert {:error, %Huginn.Error{reason: :connection}} =
               Huginn.insert_stream("INSERT INTO t FORMAT TabSeparated", data,
                 pool: pool,
                 retries: 1,
                 retry_backoff: 1
               )
    end
  end

  @doc false
  def forward_rpc(_event, _measurements, _metadata, pid), do: send(pid, :grpc_rpc_started)

  describe "child_spec/1" do
    test "identifies the pool by name" do
      assert %{id: {Pool, :client_test_spec}, type: :supervisor} =
               Huginn.child_spec(host: "localhost", pool_name: :client_test_spec)
    end
  end
end
