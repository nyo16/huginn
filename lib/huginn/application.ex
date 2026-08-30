defmodule Huginn.Application do
  @moduledoc false

  use Application

  alias Huginn.Clickhouse.Config

  @impl true
  def start(_type, _args) do
    children = build_children()

    opts = [strategy: :one_for_one, name: Huginn.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp build_children do
    # The `:grpc` application owns client supervision as of grpc 1.0
    # (it starts GRPC.Client.Registry and the GRPC.Client.Supervisor
    # DynamicSupervisor in its own tree), so Huginn only supervises the
    # optional ClickHouse pool.
    maybe_add_clickhouse_pool([])
  end

  defp maybe_add_clickhouse_pool(children) do
    case Application.get_env(:huginn, :clickhouse) do
      nil ->
        children

      clickhouse_config when is_list(clickhouse_config) ->
        config = Config.new(clickhouse_config)
        pool_config = Config.to_pool_config(config)
        children ++ [{GrpcConnectionPool, pool_config}]
    end
  end
end
