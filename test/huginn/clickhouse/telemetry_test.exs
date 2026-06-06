defmodule Huginn.Clickhouse.TelemetryTest do
  use ExUnit.Case, async: false

  alias Huginn.Clickhouse.Result

  setup do
    handler = "test-#{System.unique_integer([:positive])}"

    events = [
      [:huginn, :query, :start],
      [:huginn, :query, :stop],
      [:huginn, :query, :exception]
    ]

    test_pid = self()

    :telemetry.attach_many(
      handler,
      events,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    :ok
  end

  # Re-implements the private Client.instrument/5 contract so we can assert the
  # emitted events without a live ClickHouse server.
  defp instrument(method, sql, query_id, pool, fun) do
    metadata = %{method: method, sql: sql, query_id: query_id, pool: pool}

    :telemetry.span([:huginn, :query], metadata, fn ->
      result = fun.()

      extra =
        case result do
          {:ok, %Result{} = r} -> %{rows: length(r.rows), stats: r.stats}
          {:error, reason} -> %{error: reason}
          _ -> %{}
        end

      {result, Map.merge(metadata, extra)}
    end)
  end

  test "emits start and stop with metadata on success" do
    result = %Result{rows: [["1"]]}

    assert {:ok, ^result} =
             instrument(:query, "SELECT 1", "qid", :clickhouse_pool, fn -> {:ok, result} end)

    assert_received {:telemetry, [:huginn, :query, :start], %{system_time: _}, start_meta}
    assert start_meta.method == :query
    assert start_meta.sql == "SELECT 1"
    assert start_meta.query_id == "qid"
    assert start_meta.pool == :clickhouse_pool

    assert_received {:telemetry, [:huginn, :query, :stop], %{duration: _}, stop_meta}
    assert stop_meta.rows == 1
  end

  test "emits error metadata on failure" do
    assert {:error, :boom} =
             instrument(:insert, "INSERT", nil, :p, fn -> {:error, :boom} end)

    assert_received {:telemetry, [:huginn, :query, :stop], _measurements, %{error: :boom}}
  end

  test "default logger attaches and detaches" do
    assert :ok = Huginn.attach_default_logger(:debug)
    assert {:error, :already_exists} = Huginn.attach_default_logger()
    assert :ok = Huginn.detach_default_logger()
  end
end
