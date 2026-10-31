defmodule Huginn.Application do
  @moduledoc false

  use Application

  alias Huginn.Clickhouse.Pool

  @impl true
  def start(_type, _args) do
    opts = [strategy: :one_for_one, name: Huginn.Supervisor]
    Supervisor.start_link(children(), opts)
  end

  # The `:grpc` application owns client supervision as of grpc 1.0 (it starts
  # GRPC.Client.Registry and the GRPC.Client.Supervisor DynamicSupervisor in its
  # own tree), so Huginn only supervises the optional app-env-configured pool.
  # Further pools are started by the host application via `Huginn.child_spec/1`.
  defp children do
    case Application.get_env(:huginn, :clickhouse) do
      config when is_list(config) or is_map(config) -> [{Pool, config}]
      _ -> []
    end
  end
end
