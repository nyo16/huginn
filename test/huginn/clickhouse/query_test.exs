defmodule Huginn.Clickhouse.QueryTest do
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.{Config, Query}

  doctest Query

  describe "build/2" do
    test "creates QueryInfo with defaults" do
      query_info = Query.build("SELECT 1")

      assert query_info.query == "SELECT 1"
      assert query_info.output_format == "TabSeparated"
      assert query_info.output_compression_type == ""
      assert query_info.send_output_columns == true
      assert is_binary(query_info.query_id) and query_info.query_id != ""
    end

    test "accepts options" do
      query_info =
        Query.build("SELECT 1",
          database: "mydb",
          format: "JSONEachRow",
          user_name: "admin",
          password: "secret"
        )

      assert query_info.database == "mydb"
      assert query_info.output_format == "JSONEachRow"
      assert query_info.user_name == "admin"
      assert query_info.password == "secret"
    end
  end

  describe "build_insert/3" do
    test "creates QueryInfo with input data" do
      data = "value1\tvalue2\nvalue3\tvalue4"
      query_info = Query.build_insert("INSERT INTO t FORMAT TabSeparated", data, database: "mydb")

      assert query_info.query == "INSERT INTO t FORMAT TabSeparated"
      assert query_info.input_data == data
      assert query_info.database == "mydb"
    end

    test "does not turn :format into an output format" do
      query_info = Query.build_insert("INSERT INTO t FORMAT CSV", "1,2\n", format: "CSV")

      assert query_info.output_format == ""
    end
  end

  test "build_continuation/2 creates continuation QueryInfo" do
    query_info = Query.build_continuation("more data", has_more: true)

    assert query_info.input_data == "more data"
    assert query_info.next_query_info == true
  end

  test "build_cancel/0 creates cancel QueryInfo" do
    assert Query.build_cancel().cancel == true
  end

  describe "with_config/2" do
    test "uses the config database and credentials as defaults" do
      config =
        Config.new(host: "localhost", database: "analytics", auth: {:password, "user", "pass"})

      opts = Query.with_config([], config)

      assert opts[:database] == "analytics"
      assert opts[:user_name] == "user"
      assert opts[:password] == "pass"
      assert Query.build("SELECT 1", opts).database == "analytics"
    end

    test "per-call options win over the config" do
      config =
        Config.new(host: "localhost", database: "analytics", auth: {:password, "user", "pass"})

      opts = Query.with_config([database: "other", user_name: "admin"], config)

      assert opts[:database] == "other"
      assert opts[:user_name] == "admin"
      assert opts[:password] == "pass"
      assert Keyword.get_values(opts, :database) == ["other"]
    end

    test "includes jwt credentials" do
      config = Config.new(host: "localhost", auth: {:jwt, "token"})

      assert Query.with_config([], config)[:jwt] == "token"
    end
  end
end
