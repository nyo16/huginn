defmodule Huginn.Clickhouse.Config do
  @moduledoc """
  Configuration for a ClickHouse gRPC connection pool.

  Supports both password and JWT authentication methods. A config is built with
  `new/1` and started as a pool with `Huginn.child_spec/1` (or
  `Huginn.Clickhouse.Pool`), which registers it under its `:pool_name` so that
  queries routed to that pool use its database and credentials.

  The `:auth` field is excluded from `inspect/2` output so credentials never
  end up in logs or crash reports.

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
          port: pos_integer(),
          database: String.t(),
          auth: auth | nil,
          pool_size: pos_integer(),
          pool_name: atom(),
          ssl: boolean() | keyword()
        }

  @derive {Inspect, except: [:auth]}
  defstruct [
    :host,
    port: 9100,
    database: "default",
    auth: nil,
    pool_size: 5,
    pool_name: :clickhouse_pool,
    ssl: false
  ]

  @default_pool :clickhouse_pool
  @allowed_keys [:host, :port, :database, :auth, :pool_size, :pool_name, :ssl]

  @doc """
  Creates a new config struct from a keyword list or a map.

  Raises `ArgumentError` when an option is missing, unknown or invalid. Error
  messages name the offending option but never include option values, so
  credentials cannot leak through a configuration error.

  ## Options

    * `:host` - ClickHouse server host (required)
    * `:port` - gRPC port (default: 9100)
    * `:database` - Default database for queries on this pool (default: "default")
    * `:auth` - Authentication tuple: `{:password, user, pass}` or `{:jwt, token}`
    * `:pool_size` - Connection pool size (default: 5)
    * `:pool_name` - Pool name for registration (default: :clickhouse_pool)
    * `:ssl` - `false` (default) for plaintext h2c, `true` to enable TLS with
      peer verification against the system CA store, or a keyword list of
      `:ssl` options (e.g. `[cacertfile: "/ca.pem"]`) for custom CAs or client
      certificates. A keyword list without `:verify` gets `verify: :verify_peer`,
      the system CA store (unless `:cacerts`/`:cacertfile` is given) and HTTPS
      hostname checking merged underneath it; your keys always win. Pass
      `verify: :verify_none` explicitly to opt out of verification.

  ## Examples

      Huginn.Clickhouse.Config.new(host: "localhost")
      Huginn.Clickhouse.Config.new(%{host: "localhost", database: "analytics"})

  """
  @spec new(keyword() | map()) :: t()
  def new(opts) when is_map(opts), do: opts |> Map.to_list() |> new()

  def new(opts) when is_list(opts) do
    validate_keys!(opts)

    %__MODULE__{
      host: fetch_host!(opts),
      port: positive_integer!(opts, :port, 9100),
      database: database!(opts),
      auth: auth!(Keyword.get(opts, :auth)),
      pool_size: positive_integer!(opts, :pool_size, 5),
      pool_name: pool_name!(opts),
      ssl: ssl!(Keyword.get(opts, :ssl, false))
    }
  end

  def new(_opts) do
    raise ArgumentError, "Huginn.Clickhouse.Config.new/1 expects a keyword list or a map"
  end

  @doc """
  Returns the config registered for `pool_name` by `Huginn.Clickhouse.Pool`.
  """
  @spec fetch(atom()) :: {:ok, t()} | {:error, Huginn.Error.t()}
  def fetch(pool_name) when is_atom(pool_name) do
    case :persistent_term.get({__MODULE__, pool_name}, nil) do
      %__MODULE__{} = config ->
        {:ok, config}

      nil ->
        {:error,
         %Huginn.Error{
           reason: :not_configured,
           message:
             "no ClickHouse pool is configured under #{inspect(pool_name)}; " <>
               "start one with Huginn.child_spec/1 or set config :huginn, :clickhouse",
           details: pool_name
         }}
    end
  end

  @doc """
  Returns the name of the default pool: the `:pool_name` of the
  `:huginn, :clickhouse` application config, or `:clickhouse_pool`.
  """
  @spec default_pool() :: atom()
  def default_pool do
    case Application.get_env(:huginn, :clickhouse) do
      opts when is_list(opts) -> Keyword.get(opts, :pool_name, @default_pool)
      opts when is_map(opts) -> Map.get(opts, :pool_name, @default_pool)
      _ -> @default_pool
    end
  end

  # Registers `config` under its pool name. `:persistent_term` makes the lookup
  # on every query a constant-time read with no copying; writes only happen
  # when a pool (re)starts.
  @doc false
  @spec register(t()) :: :ok
  def register(%__MODULE__{pool_name: pool_name} = config) do
    :persistent_term.put({__MODULE__, pool_name}, config)
  end

  @doc """
  Converts config to GrpcConnectionPool keyword list format.

  Raises `ArgumentError` when `ssl: true` cannot produce a peer-verifying TLS
  configuration on the running OTP release.
  """
  @spec to_pool_config(t()) :: keyword()
  def to_pool_config(%__MODULE__{} = config) do
    endpoint_opts =
      case ssl_opts(config.ssl) do
        nil ->
          [type: :local, host: config.host, port: config.port]

        opts ->
          [type: :production, host: config.host, port: config.port, ssl: opts]
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

  @doc false
  # The scheme and TLS options for connections Huginn opens itself (streaming
  # calls, see `Huginn.Clickhouse.Transport`). Same TLS policy as the pool.
  @spec endpoint(t()) :: {:http | :https, String.t(), pos_integer(), keyword()}
  def endpoint(%__MODULE__{} = config) do
    case ssl_opts(config.ssl) do
      nil -> {:http, config.host, config.port, []}
      opts -> {:https, config.host, config.port, opts}
    end
  end

  # `ssl: true` must mean "verify the peer", not "TLS with no options". Passing
  # `ssl: []` would defeat the pool's own secure default, because it resolves
  # the endpoint as `opts[:ssl] || default_production_ssl()` and an empty list
  # is truthy — leaving `verify` unset, which is `verify_none` on OTP < 26.
  defp ssl_opts(false), do: nil

  defp ssl_opts(true) do
    # The pool gates its verifying defaults behind
    # `function_exported?(:public_key, :cacerts_get, 0)`, which reports false for
    # a module that merely has not been loaded yet. Without forcing the load,
    # `ssl: true` silently degrades to unverified TLS on a perfectly modern OTP.
    Code.ensure_loaded?(:public_key)
    opts = GrpcConnectionPool.Config.default_production_ssl()

    # Fail closed: on an OTP without a system CA accessor the pool returns `[]`,
    # which would be TLS without peer verification.
    if Keyword.get(opts, :verify) != :verify_peer do
      raise ArgumentError,
            "ssl: true requires OTP 25+ (:public_key.cacerts_get/0) to verify the server " <>
              "certificate against the system CA store; pass explicit ssl options instead, " <>
              "e.g. ssl: [cacertfile: \"/path/to/ca.pem\"]"
    end

    opts
  end

  defp ssl_opts(opts) when is_list(opts) do
    if Keyword.has_key?(opts, :verify) do
      # The caller chose explicitly (including `verify: :verify_none`).
      opts
    else
      # Without `:verify`, OTP < 26 defaults to `verify_none`. Merge verifying
      # defaults *under* the caller's options so their keys win.
      Keyword.merge(verifying_defaults(opts), opts)
    end
  end

  defp verifying_defaults(opts) do
    cacerts =
      if Keyword.has_key?(opts, :cacerts) or Keyword.has_key?(opts, :cacertfile),
        do: [],
        else: [cacerts: :public_key.cacerts_get()]

    [verify: :verify_peer] ++
      cacerts ++
      [customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]]
  end

  @doc """
  Extracts authentication credentials for query building.
  """
  @spec auth_opts(t()) :: keyword()
  def auth_opts(%__MODULE__{auth: nil}), do: []
  def auth_opts(%__MODULE__{auth: {:password, user, pass}}), do: [user_name: user, password: pass]
  def auth_opts(%__MODULE__{auth: {:jwt, token}}), do: [jwt: token]

  # Validation. Messages name options but never echo their values.

  defp validate_keys!(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "Huginn.Clickhouse.Config.new/1 expects a keyword list or a map"
    end

    case Keyword.keys(opts) -- @allowed_keys do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown Huginn.Clickhouse.Config option(s) #{inspect(Enum.uniq(unknown))}; " <>
                "allowed: #{inspect(@allowed_keys)}"
    end
  end

  defp fetch_host!(opts) do
    case Keyword.get(opts, :host) do
      host when is_binary(host) and host != "" ->
        host

      _ ->
        raise ArgumentError, "Huginn.Clickhouse.Config requires :host to be a non-empty string"
    end
  end

  defp positive_integer!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 ->
        value

      _ ->
        raise ArgumentError,
              "Huginn.Clickhouse.Config option #{inspect(key)} must be a positive integer"
    end
  end

  defp database!(opts) do
    case Keyword.get(opts, :database, "default") do
      database when is_binary(database) and database != "" ->
        database

      _ ->
        raise ArgumentError,
              "Huginn.Clickhouse.Config option :database must be a non-empty string"
    end
  end

  defp pool_name!(opts) do
    case Keyword.get(opts, :pool_name, @default_pool) do
      name when is_atom(name) and not is_nil(name) and not is_boolean(name) ->
        name

      _ ->
        raise ArgumentError, "Huginn.Clickhouse.Config option :pool_name must be an atom"
    end
  end

  defp auth!(nil), do: nil
  defp auth!({:password, user, pass} = auth) when is_binary(user) and is_binary(pass), do: auth
  defp auth!({:jwt, token} = auth) when is_binary(token), do: auth

  defp auth!(_) do
    raise ArgumentError,
          "Huginn.Clickhouse.Config option :auth must be {:password, user, password} " <>
            "or {:jwt, token} with string values"
  end

  defp ssl!(ssl) when is_boolean(ssl), do: ssl

  defp ssl!(ssl) when is_list(ssl) do
    if Keyword.keyword?(ssl) do
      ssl
    else
      raise ArgumentError,
            "Huginn.Clickhouse.Config option :ssl must be a boolean or a keyword list"
    end
  end

  defp ssl!(_) do
    raise ArgumentError,
          "Huginn.Clickhouse.Config option :ssl must be a boolean or a keyword list"
  end
end
