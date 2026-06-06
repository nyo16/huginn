defmodule Huginn.Clickhouse.RetryTest do
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.Retry

  defp counter, do: :counters.new(1, [])
  defp bump(c), do: :counters.add(c, 1, 1)
  defp count(c), do: :counters.get(c, 1)

  describe "with_retry/2" do
    test "returns success on the first attempt without retrying" do
      c = counter()

      result =
        Retry.with_retry(
          fn ->
            bump(c)
            {:ok, :done}
          end,
          retries: 3,
          retry_backoff: 1
        )

      assert result == {:ok, :done}
      assert count(c) == 1
    end

    test "retries transient errors then succeeds" do
      c = counter()

      result =
        Retry.with_retry(
          fn ->
            bump(c)
            if count(c) < 3, do: {:error, :unavailable}, else: {:ok, :done}
          end,
          retries: 5,
          retry_backoff: 1
        )

      assert result == {:ok, :done}
      assert count(c) == 3
    end

    test "exhausts retries and returns the last error" do
      c = counter()

      result =
        Retry.with_retry(
          fn ->
            bump(c)
            {:error, :timeout}
          end,
          retries: 2,
          retry_backoff: 1
        )

      assert result == {:error, :timeout}
      # initial attempt + 2 retries
      assert count(c) == 3
    end

    test "does not retry non-transient errors" do
      c = counter()

      result =
        Retry.with_retry(
          fn ->
            bump(c)
            {:error, :syntax_error}
          end,
          retries: 5,
          retry_backoff: 1
        )

      assert result == {:error, :syntax_error}
      assert count(c) == 1
    end

    test "does not retry when retries is 0 (default)" do
      c = counter()

      result =
        Retry.with_retry(fn ->
          bump(c)
          {:error, :unavailable}
        end)

      assert result == {:error, :unavailable}
      assert count(c) == 1
    end
  end

  describe "transient?/1" do
    test "treats connection reasons as transient" do
      assert Retry.transient?(:timeout)
      assert Retry.transient?(:unavailable)
      assert Retry.transient?(:no_channels)
      assert Retry.transient?({:error, :closed})
    end

    test "treats retryable gRPC statuses as transient" do
      assert Retry.transient?(%GRPC.RPCError{status: 14, message: "unavailable"})
      assert Retry.transient?(%GRPC.RPCError{status: 4, message: "deadline"})
    end

    test "treats other errors as non-transient" do
      refute Retry.transient?(:syntax_error)
      refute Retry.transient?(%GRPC.RPCError{status: 3, message: "invalid argument"})
    end
  end
end
