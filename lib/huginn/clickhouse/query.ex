defmodule Huginn.Clickhouse.Query do
  @moduledoc """
  Builds QueryInfo messages for ClickHouse gRPC requests.
  """

  alias Clickhouse.Grpc.ExternalTable
  alias Clickhouse.Grpc.NameAndType
  alias Clickhouse.Grpc.QueryInfo
  alias Huginn.Clickhouse.Config

  @type query_opts :: [
          database: String.t(),
          user_name: String.t(),
          password: String.t(),
          jwt: String.t(),
          format: String.t(),
          settings: map(),
          query_id: String.t(),
          session_id: String.t(),
          session_timeout: non_neg_integer(),
          send_output_columns: boolean()
        ]

  @doc """
  Builds a QueryInfo struct for a SQL query.

  ## Options

    * `:database` - Database to use for the query
    * `:user_name` - Username for authentication
    * `:password` - Password for authentication
    * `:jwt` - JWT token for authentication
    * `:format` - Output format (default: "TabSeparated")
    * `:settings` - Map of ClickHouse settings
    * `:query_id` - Custom query ID for tracking
    * `:session_id` - Session ID for stateful queries
    * `:session_timeout` - Session timeout in seconds
    * `:send_output_columns` - Include column metadata in response

  """
  @spec build(String.t(), query_opts()) :: struct()
  def build(sql, opts \\ []) do
    %QueryInfo{
      query: sql,
      query_id: Keyword.get(opts, :query_id, generate_query_id()),
      database: Keyword.get(opts, :database, ""),
      user_name: Keyword.get(opts, :user_name, ""),
      password: Keyword.get(opts, :password, ""),
      jwt: Keyword.get(opts, :jwt, ""),
      output_format: Keyword.get(opts, :format, "TabSeparated"),
      settings: Keyword.get(opts, :settings, %{}),
      session_id: Keyword.get(opts, :session_id, ""),
      session_timeout: Keyword.get(opts, :session_timeout, 0),
      send_output_columns: Keyword.get(opts, :send_output_columns, true)
    }
  end

  @doc """
  Builds a QueryInfo struct for an INSERT query with input data.

  The input data format is taken from the SQL `FORMAT` clause (e.g.
  `INSERT INTO t FORMAT TabSeparated`), not from an option.

  ## Options

  Same as `build/2` (except `:format` and `:send_output_columns`, which do not
  apply to inserts) plus:

    * `:input_compression` - Compression type of `data` (e.g. `"gzip"`); the
      server decompresses it before parsing

  """
  @spec build_insert(String.t(), binary(), keyword()) :: struct()
  def build_insert(sql, data, opts \\ []) do
    %QueryInfo{
      query: sql,
      query_id: Keyword.get(opts, :query_id, generate_query_id()),
      database: Keyword.get(opts, :database, ""),
      user_name: Keyword.get(opts, :user_name, ""),
      password: Keyword.get(opts, :password, ""),
      jwt: Keyword.get(opts, :jwt, ""),
      input_data: data,
      input_compression_type: Keyword.get(opts, :input_compression, ""),
      settings: Keyword.get(opts, :settings, %{}),
      session_id: Keyword.get(opts, :session_id, ""),
      session_timeout: Keyword.get(opts, :session_timeout, 0)
    }
  end

  @doc """
  Builds a continuation QueryInfo for streaming input.

  Used with `ExecuteQueryWithStreamInput` and `ExecuteQueryWithStreamIO` methods.
  """
  @spec build_continuation(binary(), keyword()) :: struct()
  def build_continuation(data, opts \\ []) do
    %QueryInfo{
      input_data: data,
      next_query_info: Keyword.get(opts, :has_more, true)
    }
  end

  @doc """
  Builds a cancel QueryInfo to stop a running query.
  """
  @spec build_cancel() :: struct()
  def build_cancel do
    %QueryInfo{cancel: true}
  end

  @doc """
  Builds an external table definition for use with queries.

  ## Options

    * `:format` - Data format (default: "TabSeparated")
    * `:compression` - Compression type of data
    * `:settings` - Additional settings for the table

  """
  @spec build_external_table(String.t(), [{String.t(), String.t()}], binary(), keyword()) ::
          struct()
  def build_external_table(name, columns, data, opts \\ []) do
    %ExternalTable{
      name: name,
      columns:
        Enum.map(columns, fn {col_name, col_type} ->
          %NameAndType{name: col_name, type: col_type}
        end),
      data: data,
      format: Keyword.get(opts, :format, "TabSeparated"),
      compression_type: Keyword.get(opts, :compression, ""),
      settings: Keyword.get(opts, :settings, %{})
    }
  end

  @doc """
  Applies a pool's config to per-call query options.

  The config's `:database` and authentication credentials become defaults;
  options given in `opts` always win.

  ## Examples

      iex> config = Huginn.Clickhouse.Config.new(host: "localhost", database: "analytics")
      iex> Huginn.Clickhouse.Query.with_config([format: "JSON"], config)
      [database: "analytics", format: "JSON"]

  """
  @spec with_config(keyword(), Config.t()) :: keyword()
  def with_config(opts, %Config{} = config) do
    Keyword.merge([database: config.database] ++ Config.auth_opts(config), opts)
  end

  @doc false
  @spec generate_query_id() :: String.t()
  def generate_query_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end
end
