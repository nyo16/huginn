defmodule Huginn.Clickhouse.TelemetryTest do
  # The default logger is attached under a fixed, global handler id.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Huginn.Clickhouse.Telemetry

  @events [
    [:huginn, :query, :start],
    [:huginn, :query, :stop],
    [:huginn, :query, :exception]
  ]

  setup do
    query_id = "telemetry-test-#{System.unique_integer([:positive])}"
    handler = "test-#{query_id}"

    # A module function capture (not an anonymous fn) keeps :telemetry from
    # logging its "local function" performance notice.
    :telemetry.attach_many(handler, @events, &__MODULE__.forward_event/4, %{
      pid: self(),
      query_id: query_id
    })

    on_exit(fn -> :telemetry.detach(handler) end)
    {:ok, query_id: query_id}
  end

  # Only events for this test's query id are forwarded, so requests made by
  # other test modules never leak in.
  @doc false
  def forward_event(event, measurements, %{query_id: id} = metadata, %{pid: pid, query_id: id}) do
    send(pid, {:telemetry, event, measurements, metadata})
  end

  def forward_event(_event, _measurements, _metadata, _config), do: :ok

  describe "request events" do
    test "query/2 emits start and stop with error metadata", %{query_id: id} do
      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Huginn.query("SELECT 1", pool: :unknown, query_id: id)

      assert_received {:telemetry, [:huginn, :query, :start], %{system_time: _}, start_meta}
      assert %{method: :query, sql: "SELECT 1", query_id: ^id, pool: :unknown} = start_meta

      assert_received {:telemetry, [:huginn, :query, :stop], %{duration: duration}, stop_meta}
      assert is_integer(duration) and duration >= 0
      assert %{method: :query, error: %Huginn.Error{reason: :not_configured}} = stop_meta

      refute_received {:telemetry, [:huginn, :query, :exception], _, _}
    end

    test "insert_stream/3 reports the generated query id it runs under" do
      # No :query_id is given, so the event is matched on its pool instead.
      pool = :"telemetry_insert_stream_#{System.unique_integer([:positive])}"
      handler = "insert-stream-#{pool}"

      :telemetry.attach_many(handler, @events, &__MODULE__.forward_by_pool/4, %{
        pid: self(),
        pool: pool
      })

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Huginn.insert_stream("INSERT INTO t FORMAT TabSeparated", [["a"]], pool: pool)

      assert_received {:by_pool, [:huginn, :query, :start],
                       %{method: :insert_stream, query_id: query_id}}

      assert query_id =~ ~r/\A[0-9a-f]{32}\z/

      assert_received {:by_pool, [:huginn, :query, :stop],
                       %{query_id: ^query_id, error: %Huginn.Error{reason: :not_configured}}}
    end

    test "stream_query/2 emits nothing until enumerated, then start and stop", %{query_id: id} do
      stream = Huginn.stream_query("SELECT 1", pool: :unknown, query_id: id)
      refute_received {:telemetry, _, _, _}

      assert [{:error, %Huginn.Error{reason: :not_configured}}] = Enum.to_list(stream)

      assert_received {:telemetry, [:huginn, :query, :start], _, %{method: :stream_query}}

      assert_received {:telemetry, [:huginn, :query, :stop], %{duration: _},
                       %{method: :stream_query, error: %Huginn.Error{reason: :not_configured}}}
    end

    test "stream_rows/2 reports the failure it raises", %{query_id: id} do
      assert_raise Huginn.Error, fn ->
        "SELECT 1" |> Huginn.stream_rows(pool: :unknown, query_id: id) |> Enum.to_list()
      end

      assert_received {:telemetry, [:huginn, :query, :stop], _,
                       %{method: :stream_query, error: %Huginn.Error{reason: :not_configured}}}
    end
  end

  @doc false
  def forward_by_pool(event, _measurements, %{pool: pool} = metadata, %{pid: pid, pool: pool}) do
    send(pid, {:by_pool, event, metadata})
  end

  def forward_by_pool(_event, _measurements, _metadata, _config), do: :ok

  describe "default logger" do
    test "attaches and detaches" do
      assert :ok = Huginn.attach_default_logger(:debug)
      assert {:error, :already_exists} = Huginn.attach_default_logger()
      assert :ok = Huginn.detach_default_logger()
    end

    test "logs successful requests with row counts at the configured level" do
      metadata = %{method: :query, sql: "SELECT\n  number\nFROM numbers(3)", rows: 3}
      duration = System.convert_time_unit(12, :millisecond, :native)

      log =
        capture_log(fn ->
          Telemetry.handle_event(
            [:huginn, :query, :stop],
            %{duration: duration},
            metadata,
            %{level: :info}
          )
        end)

      assert log =~ "[info]"
      assert log =~ "Huginn query (3 rows) in 12ms — SELECT number FROM numbers(3)"
    end

    test "logs failures at error level with the error message" do
      error = %Huginn.Error{reason: :server, code: 62, message: "ClickHouse error 62: Bad"}

      log =
        capture_log(fn ->
          Telemetry.handle_event(
            [:huginn, :query, :stop],
            %{duration: 0},
            %{method: :insert, sql: "SELEC 1", error: error},
            %{level: :info}
          )
        end)

      assert log =~ "[error]"
      assert log =~ "Huginn insert failed in 0ms: ClickHouse error 62: Bad — SELEC 1"
    end

    test "logs streams that were stopped early" do
      log =
        capture_log(fn ->
          Telemetry.handle_event(
            [:huginn, :query, :stop],
            %{duration: 0},
            %{method: :stream_query, sql: "SELECT 1", halted: true},
            %{level: :info}
          )
        end)

      assert log =~ "Huginn stream_query stopped early after 0ms — SELECT 1"
    end

    test "logs exceptions and truncates long SQL" do
      sql = "SELECT " <> String.duplicate("x, ", 100) <> "1"

      log =
        capture_log(fn ->
          Telemetry.handle_event(
            [:huginn, :query, :exception],
            %{duration: 0},
            %{method: :stream_io, sql: sql, kind: :error, reason: %RuntimeError{message: "boom"}},
            %{level: :info}
          )
        end)

      assert log =~ "[error]"
      assert log =~ "Huginn stream_io raised after 0ms: ** (RuntimeError) boom"
      assert log =~ ~r/— SELECT x, .{100,}\.\.\.$/m
    end
  end
end
