defmodule Huginn.Clickhouse.Query do
  @moduledoc """
  Builds QueryInfo messages for ClickHouse gRPC requests.
  """

  alias Clickhouse.Grpc.QueryInfo
  alias Clickhouse.Grpc.ExternalTable
  alias Clickhouse.Grpc.NameAndType

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
          compression: String.t(),
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
    * `:compression` - Output compression type
    * `:send_output_columns` - Include column metadata in response

  """
  @spec build(String.t(), query_opts()) :: QueryInfo.t()
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
      output_compression_type: Keyword.get(opts, :compression, ""),
      send_output_columns: Keyword.get(opts, :send_output_columns, true)
    }
  end

  @doc """
  Builds a QueryInfo struct for an INSERT query with input data.

  ## Options

  Same as `build/2` plus:

    * `:input_format` - Format of input data (default: "TabSeparated")
    * `:input_compression` - Compression type of input data

  """
  @spec build_insert(String.t(), binary(), keyword()) :: QueryInfo.t()
  def build_insert(sql, data, opts \\ []) do
    %QueryInfo{
      query: sql,
      query_id: Keyword.get(opts, :query_id, generate_query_id()),
      database: Keyword.get(opts, :database, ""),
      user_name: Keyword.get(opts, :user_name, ""),
      password: Keyword.get(opts, :password, ""),
      jwt: Keyword.get(opts, :jwt, ""),
      input_data: data,
      output_format: Keyword.get(opts, :format, "TabSeparated"),
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
  @spec build_continuation(binary(), keyword()) :: QueryInfo.t()
  def build_continuation(data, opts \\ []) do
    %QueryInfo{
      input_data: data,
      next_query_info: Keyword.get(opts, :has_more, true)
    }
  end

  @doc """
  Builds a cancel QueryInfo to stop a running query.
  """
  @spec build_cancel() :: QueryInfo.t()
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
          ExternalTable.t()
  def build_external_table(name, columns, data, opts \\ []) do
    %ExternalTable{
      name: name,
      columns: Enum.map(columns, fn {col_name, col_type} ->
        %NameAndType{name: col_name, type: col_type}
      end),
      data: data,
      format: Keyword.get(opts, :format, "TabSeparated"),
      compression_type: Keyword.get(opts, :compression, ""),
      settings: Keyword.get(opts, :settings, %{})
    }
  end

  @doc """
  Merges authentication options into query options.
  """
  @spec with_auth(keyword(), Huginn.Clickhouse.Config.t()) :: keyword()
  def with_auth(opts, config) do
    auth_opts = Huginn.Clickhouse.Config.auth_opts(config)
    Keyword.merge(auth_opts, opts)
  end

  defp generate_query_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end
end
