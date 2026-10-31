defmodule Huginn.Clickhouse.RetryTest do
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.Retry

  @connection %Huginn.Error{reason: :connection, message: "no usable connection"}
  @timeout %Huginn.Error{reason: :timeout, message: "request timed out"}
  @server %Huginn.Error{reason: :server, code: 62, message: "syntax error"}

  defp counter, do: :counters.new(1, [])
  defp bump(c), do: :counters.add(c, 1, 1)
  defp count(c), do: :counters.get(c, 1)

  defp failing(c, error) do
    fn ->
      bump(c)
      {:error, error}
    end
  end

  describe "with_retry/3" do
    test "returns success on the first attempt without retrying" do
      c = counter()

      fun = fn ->
        bump(c)
        {:ok, :done}
      end

      assert Retry.with_retry(fun, retries: 3, retry_backoff: 1) == {:ok, :done}
      assert count(c) == 1
    end

    test "retries connection errors then succeeds" do
      c = counter()

      fun = fn ->
        bump(c)
        if count(c) < 3, do: {:error, @connection}, else: {:ok, :done}
      end

      assert Retry.with_retry(fun, retries: 5, retry_backoff: 1) == {:ok, :done}
      assert count(c) == 3
    end

    test "exhausts retries and returns the last error" do
      c = counter()

      assert Retry.with_retry(failing(c, @connection), retries: 2, retry_backoff: 1) ==
               {:error, @connection}

      # initial attempt + 2 retries
      assert count(c) == 3
    end

    test "does not retry timeouts" do
      c = counter()

      assert Retry.with_retry(failing(c, @timeout), retries: 5, retry_backoff: 1) ==
               {:error, @timeout}

      assert count(c) == 1
    end

    test "does not retry server errors" do
      c = counter()

      assert Retry.with_retry(failing(c, @server), retries: 5, retry_backoff: 1) ==
               {:error, @server}

      assert count(c) == 1
    end

    test "does not retry when retries is 0 (default)" do
      c = counter()

      assert Retry.with_retry(failing(c, @connection)) == {:error, @connection}
      assert count(c) == 1
    end

    test "emits a retry event per retry with the given metadata" do
      ref = make_ref()
      handler = "retry-test-#{inspect(ref)}"

      :telemetry.attach(
        handler,
        [:huginn, :query, :retry],
        &__MODULE__.forward_event/4,
        %{pid: self(), ref: ref}
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      Retry.with_retry(failing(counter(), @connection), [retries: 2, retry_backoff: 1], %{
        query_id: ref
      })

      assert_received {^ref, %{attempt: 1, delay: d1}, %{query_id: ^ref, error: @connection}}
      assert_received {^ref, %{attempt: 2, delay: d2}, %{query_id: ^ref, error: @connection}}
      refute_received {^ref, _, _}
      assert d1 == 1
      assert d2 in 1..2
    end
  end

  describe "transient?/1" do
    test "only connection errors are transient" do
      assert Retry.transient?(@connection)
      refute Retry.transient?(@timeout)
      refute Retry.transient?(@server)
      refute Retry.transient?(%Huginn.Error{reason: :grpc, code: 3})
    end

    test "raw terms are not transient" do
      refute Retry.transient?(:not_connected)
      refute Retry.transient?(%GRPC.RPCError{status: 14, message: "unavailable"})
    end

    test "wrapped transport failures classify as expected" do
      assert Retry.transient?(Huginn.Error.wrap(:not_connected))
      assert Retry.transient?(Huginn.Error.wrap(%GRPC.RPCError{status: 14, message: "down"}))
      refute Retry.transient?(Huginn.Error.wrap(%GRPC.RPCError{status: 4, message: "deadline"}))
    end
  end

  describe "delay/2" do
    test "stays within the exponential ceiling" do
      for attempt <- 1..6, _ <- 1..50 do
        assert Retry.delay(attempt, retry_backoff: 10) in 1..(10 * Integer.pow(2, attempt - 1))
      end
    end

    test "is capped by retry_max_backoff" do
      for _ <- 1..200 do
        assert Retry.delay(30, retry_backoff: 100, retry_max_backoff: 250) in 1..250
      end
    end

    test "defaults to a 5s cap" do
      for _ <- 1..200, do: assert(Retry.delay(30) in 1..5_000)
    end

    test "is 0 when the backoff is 0" do
      assert Retry.delay(3, retry_backoff: 0) == 0
    end
  end

  # Handlers are global and tests run concurrently, so only events tagged with
  # this test's ref are forwarded.
  @doc false
  def forward_event(_event, measurements, %{query_id: ref} = metadata, %{pid: pid, ref: ref}) do
    send(pid, {ref, measurements, metadata})
  end

  def forward_event(_event, _measurements, _metadata, _config), do: :ok
end
