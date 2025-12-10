defmodule Huginn.Clickhouse.Config do
  @moduledoc """
  Configuration for ClickHouse gRPC connections.

  Supports both password and JWT authentication methods.

  ## Examples

      # Password authentication
      config = Huginn.Clickhouse.Config.new(
        host: "localhost",
        port: 9100,
        database: "default",
        auth: {:password, "default", "secret"}
      )

      # JWT authentication
      config = Huginn.Clickhouse.Config.new(
        host: "localhost",
        port: 9100,
        database: "default",
        auth: {:jwt, "eyJ..."}
      )

  """

  @type auth :: {:password, String.t(), String.t()} | {:jwt, String.t()}

  @type t :: %__MODULE__{
          host: String.t(),
          port: non_neg_integer(),
          database: String.t(),
          auth: auth | nil,
          pool_size: non_neg_integer(),
          pool_name: atom(),
          ssl: boolean(),
          compression: String.t() | nil
        }

  defstruct [
    :host,
    :port,
    :database,
    :auth,
    pool_size: 5,
    pool_name: :clickhouse_pool,
    ssl: false,
    compression: nil
  ]

  @doc """
  Creates a new config struct from keyword options.

  ## Options

    * `:host` - ClickHouse server host (required)
    * `:port` - gRPC port (default: 9100)
    * `:database` - Default database (default: "default")
    * `:auth` - Authentication tuple: `{:password, user, pass}` or `{:jwt, token}`
    * `:pool_size` - Connection pool size (default: 5)
    * `:pool_name` - Pool name for registration (default: :clickhouse_pool)
    * `:ssl` - Enable SSL/TLS (default: false)
    * `:compression` - Output compression type: "gzip", "lz4", "zstd", etc.

  """
  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      host: Keyword.fetch!(opts, :host),
      port: Keyword.get(opts, :port, 9100),
      database: Keyword.get(opts, :database, "default"),
      auth: Keyword.get(opts, :auth),
      pool_size: Keyword.get(opts, :pool_size, 5),
      pool_name: Keyword.get(opts, :pool_name, :clickhouse_pool),
      ssl: Keyword.get(opts, :ssl, false),
      compression: Keyword.get(opts, :compression)
    }
  end

  @doc """
  Loads configuration from application environment.

  Expects config under `:huginn, :clickhouse` key.

  ## Example config

      config :huginn, :clickhouse,
        host: "localhost",
        port: 9100,
        database: "default",
        auth: {:password, "default", ""}

  """
  @spec from_env() :: t()
  def from_env do
    opts = Application.get_env(:huginn, :clickhouse, [])

    if Keyword.has_key?(opts, :host) do
      new(opts)
    else
      # Return a default config if not configured
      %__MODULE__{
        host: "localhost",
        port: 9100,
        database: "default",
        pool_name: :clickhouse_pool,
        pool_size: 5,
        ssl: false
      }
    end
  end

  @doc """
  Converts config to GrpcConnectionPool keyword list format.
  """
  @spec to_pool_config(t()) :: keyword()
  def to_pool_config(%__MODULE__{} = config) do
    endpoint_opts =
      if config.ssl do
        [
          type: :production,
          host: config.host,
          port: config.port,
          ssl: []
        ]
      else
        [
          type: :local,
          host: config.host,
          port: config.port
        ]
      end

    pool_opts = [
      size: config.pool_size,
      name: config.pool_name
    ]

    [
      endpoint: endpoint_opts,
      pool: pool_opts
    ]
  end

  @doc """
  Extracts authentication credentials for query building.
  """
  @spec auth_opts(t()) :: keyword()
  def auth_opts(%__MODULE__{auth: nil}), do: []
  def auth_opts(%__MODULE__{auth: {:password, user, pass}}), do: [user_name: user, password: pass]
  def auth_opts(%__MODULE__{auth: {:jwt, token}}), do: [jwt: token]
end
