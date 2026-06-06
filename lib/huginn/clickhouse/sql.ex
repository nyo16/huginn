defmodule Huginn.Clickhouse.SQL do
  @moduledoc """
  Helpers for safely building ClickHouse SQL fragments.
  """

  @doc """
  Escapes a value for use inside a single-quoted ClickHouse string literal.

  Backslashes are escaped first, then single quotes, following ClickHouse's
  string-literal escaping rules. This prevents a trailing backslash or an
  embedded quote from breaking out of the literal.

  ## Examples

      iex> Huginn.Clickhouse.SQL.escape("abc")
      "abc"

      iex> Huginn.Clickhouse.SQL.escape("o'brien")
      "o\\\\'brien"

      iex> Huginn.Clickhouse.SQL.escape("ends_with_backslash\\\\")
      "ends_with_backslash\\\\\\\\"

  """
  @spec escape(String.t()) :: String.t()
  def escape(value) when is_binary(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "\\'")
  end
end
