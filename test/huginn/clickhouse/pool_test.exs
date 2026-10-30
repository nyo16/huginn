defmodule Huginn.Clickhouse.PoolTest do
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.{Config, Pool}

  # Pools point at 127.0.0.1:1, where nothing listens: the pool supervisor
  # starts regardless (workers connect asynchronously after `init/1`) and the
  # failing reconnects only log.
  @moduletag :capture_log

  defp pool_opts(name, auth) do
    [host: "127.0.0.1", port: 1, pool_size: 1, pool_name: name, auth: auth]
  end

  test "registers the config so Config.fetch/1 returns it" do
    start_supervised!({Pool, pool_opts(:pool_test_register, {:password, "user", "pass"})})

    assert {:ok, %Config{host: "127.0.0.1", port: 1, auth: {:password, "user", "pass"}}} =
             Config.fetch(:pool_test_register)
  end

  test "accepts a map of options" do
    start_supervised!({Pool, Map.new(pool_opts(:pool_test_map, nil))})

    assert {:ok, %Config{pool_name: :pool_test_map}} = Config.fetch(:pool_test_map)
  end

  test "pools with different credentials coexist" do
    start_supervised!({Pool, pool_opts(:pool_test_a, {:password, "alice", "a-secret"})})
    start_supervised!({Pool, pool_opts(:pool_test_b, {:jwt, "b-token"})})

    assert {:ok, %Config{auth: {:password, "alice", "a-secret"}}} = Config.fetch(:pool_test_a)
    assert {:ok, %Config{auth: {:jwt, "b-token"}}} = Config.fetch(:pool_test_b)
  end

  test "a duplicate pool name does not overwrite the running pool's config" do
    start_supervised!({Pool, pool_opts(:pool_test_dup, {:jwt, "first"})})

    assert {:error, {:already_started, _pid}} =
             Pool.start_link(pool_opts(:pool_test_dup, {:jwt, "second"}))

    assert {:ok, %Config{auth: {:jwt, "first"}}} = Config.fetch(:pool_test_dup)
  end

  test "child_spec/1 ids are unique per pool name" do
    assert %{id: {Pool, :pool_test_spec}, type: :supervisor} =
             Pool.child_spec(pool_opts(:pool_test_spec, nil))
  end

  describe "await_ready/2" do
    test "returns :not_configured for an unknown pool" do
      assert {:error, %Huginn.Error{reason: :not_configured}} =
               Pool.await_ready(:pool_test_unknown, 10)
    end

    test "returns a :connection error when no channel connects in time" do
      start_supervised!({Pool, pool_opts(:pool_test_unreachable, nil)})

      assert {:error, %Huginn.Error{reason: :connection}} =
               Pool.await_ready(:pool_test_unreachable, 100)
    end
  end
end
