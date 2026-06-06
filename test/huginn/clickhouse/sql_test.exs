defmodule Huginn.Clickhouse.SQLTest do
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.SQL

  doctest SQL

  describe "escape/1" do
    test "leaves plain strings untouched" do
      assert SQL.escape("abc-123") == "abc-123"
    end

    test "escapes single quotes" do
      assert SQL.escape("o'brien") == "o\\'brien"
    end

    test "escapes backslashes before quotes so a trailing backslash can't escape out" do
      # A naive `replace("'", "\\'")` would turn "a\\'" into "a\\\\'" only by luck;
      # the real risk is a trailing backslash. Escaping `\` first is what makes it safe.
      assert SQL.escape("a\\") == "a\\\\"
      assert SQL.escape("a\\'; DROP") == "a\\\\\\'; DROP"
    end

    test "is order-correct for combined backslash + quote" do
      assert SQL.escape("\\'") == "\\\\\\'"
    end
  end
end
