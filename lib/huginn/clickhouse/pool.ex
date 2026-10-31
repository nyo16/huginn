defmodule Huginn.Clickhouse.Pool do
  @moduledoc """
  Supervises one ClickHouse gRPC connection pool.

  Starting a pool registers its `Huginn.Clickhouse.Config` under the config's
  `:pool_name`, so every query routed to that pool (via the `:pool` option)
  uses that pool's database and credentials, then starts the underlying
  `GrpcConnectionPool`. The usual entry point is `Huginn.child_spec/1`, which
  delegates here.

  ## Single pool

  Add the pool to your application's supervision tree:

      children = [
        {Huginn, host: "localhost", port: 9100, auth: {:password, "default", ""}}
      ]

  Alternatively set `config :huginn, :clickhouse, host: ...` and Huginn's own
  application starts the pool for you.

  ## Multiple pools

  Pools are identified by `:pool_name`, so any number can coexist under one
  supervisor — for example against different hosts or with different
  credentials:

      children = [
        {Huginn,
         pool_name: :analytics,
         host: "analytics.internal",
         database: "analytics",
         auth: {:password, "reader", System.fetch_env!("ANALYTICS_PASSWORD")}},
        {Huginn,
         pool_name: :events,
         host: "events.internal",
         database: "events",
         auth: {:jwt, System.fetch_env!("EVENTS_JWT")},
         ssl: true}
      ]

      Huginn.query("SELECT count() FROM visits", pool: :analytics)
      Huginn.query("SELECT count() FROM clicks", pool: :events)

  Starting a second pool under a name that is already running fails with
  `{:error, {:already_started, pid}}` and leaves the running pool's config
  untouched.

  A pool starts even when the server is unreachable; its workers keep
  reconnecting in the background. Use `await_ready/2` to block until at least
  one connection is up.
  """

  use Supervisor

  alias Huginn.Clickhouse.Config

  @doc """
  Returns a supervisor child spec for a pool.

  Accepts the options of `Huginn.Clickhouse.Config.new/1` (keyword list or map)
  or a `Huginn.Clickhouse.Config` struct. The child id is
  `{Huginn.Clickhouse.Pool, pool_name}`, so pools with different names can be
  started under the same supervisor.
  """
  @spec child_spec(keyword() | map() | Config.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    config = to_config(opts)

    %{
      id: {__MODULE__, config.pool_name},
      start: {__MODULE__, :start_link, [config]},
      type: :supervisor
    }
  end

  @doc """
  Starts a pool supervisor. See `child_spec/1` for the accepted options.
  """
  @spec start_link(keyword() | map() | Config.t()) :: Supervisor.on_start()
  def start_link(opts) do
    config = to_config(opts)
    # Naming the supervisor makes a duplicate pool name fail before `init/1`,
    # i.e. before it could overwrite the running pool's registered config.
    Supervisor.start_link(__MODULE__, config, name: supervisor_name(config.pool_name))
  end

  @doc """
  Blocks until the pool has at least one connected channel.

  Returns `{:error, %Huginn.Error{reason: :not_configured}}` when no pool is
  registered under `pool_name`, and `{:error, %Huginn.Error{reason: :connection}}`
  when no connection came up within `timeout` milliseconds.
  """
  @spec await_ready(atom(), pos_integer()) :: :ok | {:error, Huginn.Error.t()}
  def await_ready(pool_name \\ Config.default_pool(), timeout \\ 10_000) do
    with {:ok, _config} <- Config.fetch(pool_name) do
      do_await_ready(pool_name, timeout)
    end
  end

  defp do_await_ready(pool_name, timeout) do
    case GrpcConnectionPool.await_ready(pool_name, timeout) do
      :ok -> :ok
      {:error, :timeout} -> not_ready(pool_name, timeout)
    end
  rescue
    # The pool's ETS table is missing: the config is registered but the pool
    # process is gone (stopped, or crashed past its restart intensity).
    ArgumentError -> not_running(pool_name)
  end

  defp not_ready(pool_name, timeout) do
    {:error,
     %Huginn.Error{
       reason: :connection,
       message: "pool #{inspect(pool_name)} has no connected channel after #{timeout}ms",
       details: :timeout
     }}
  end

  defp not_running(pool_name) do
    {:error,
     %Huginn.Error{
       reason: :connection,
       message: "pool #{inspect(pool_name)} is not running",
       details: :not_running
     }}
  end

  @impl Supervisor
  def init(%Config{} = config) do
    # Resolve the pool config first: it raises on an unverifiable `ssl: true`,
    # and that must happen before the config becomes visible to queries.
    pool_config = Config.to_pool_config(config)
    :ok = Config.register(config)

    Supervisor.init([{GrpcConnectionPool, pool_config}], strategy: :one_for_one)
  end

  defp to_config(%Config{} = config), do: config
  defp to_config(opts), do: Config.new(opts)

  defp supervisor_name(pool_name), do: :"#{inspect(__MODULE__)}.#{pool_name}"
end
