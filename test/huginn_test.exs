defmodule HuginnTest do
  use ExUnit.Case

  alias Huginn.Clickhouse.{Config, Query, Result}

  describe "Config" do
    test "new/1 creates config with required host" do
      config = Config.new(host: "localhost")
      assert config.host == "localhost"
      assert config.port == 9100
      assert config.database == "default"
      assert config.pool_size == 5
    end

    test "new/1 accepts all options" do
      config =
        Config.new(
          host: "clickhouse.example.com",
          port: 9101,
          database: "mydb",
          auth: {:password, "user", "pass"},
          pool_size: 10,
          ssl: true
        )

      assert config.host == "clickhouse.example.com"
      assert config.port == 9101
      assert config.database == "mydb"
      assert config.auth == {:password, "user", "pass"}
      assert config.pool_size == 10
      assert config.ssl == true
    end

    test "auth_opts/1 returns empty for nil auth" do
      config = %Config{host: "localhost", port: 9100}
      assert Config.auth_opts(config) == []
    end

    test "auth_opts/1 returns password credentials" do
      config = %Config{host: "localhost", port: 9100, auth: {:password, "user", "secret"}}
      assert Config.auth_opts(config) == [user_name: "user", password: "secret"]
    end

    test "auth_opts/1 returns jwt credentials" do
      config = %Config{host: "localhost", port: 9100, auth: {:jwt, "token123"}}
      assert Config.auth_opts(config) == [jwt: "token123"]
    end

    test "to_pool_config/1 generates pool config for local" do
      config = Config.new(host: "localhost", port: 9100, pool_size: 3, pool_name: :test_pool)
      pool_config = Config.to_pool_config(config)

      assert pool_config[:endpoint][:type] == :local
      assert pool_config[:endpoint][:host] == "localhost"
      assert pool_config[:endpoint][:port] == 9100
      assert pool_config[:pool][:size] == 3
      assert pool_config[:pool][:name] == :test_pool
    end

    test "to_pool_config/1 generates pool config for ssl" do
      config = Config.new(host: "secure.example.com", port: 443, ssl: true)
      pool_config = Config.to_pool_config(config)

      assert pool_config[:endpoint][:type] == :production
      assert pool_config[:endpoint][:ssl] == []
    end
  end

  describe "Query" do
    test "build/2 creates QueryInfo with defaults" do
      query_info = Query.build("SELECT 1")

      assert query_info.query == "SELECT 1"
      assert query_info.output_format == "TabSeparated"
      assert query_info.send_output_columns == true
      assert query_info.query_id != nil
    end

    test "build/2 accepts options" do
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

    test "build_insert/3 creates QueryInfo with input data" do
      data = "value1\tvalue2\nvalue3\tvalue4"
      query_info = Query.build_insert("INSERT INTO t VALUES", data)

      assert query_info.query == "INSERT INTO t VALUES"
      assert query_info.input_data == data
    end

    test "build_continuation/2 creates continuation QueryInfo" do
      data = "more data"
      query_info = Query.build_continuation(data, has_more: true)

      assert query_info.input_data == data
      assert query_info.next_query_info == true
    end

    test "build_cancel/0 creates cancel QueryInfo" do
      query_info = Query.build_cancel()
      assert query_info.cancel == true
    end

    test "with_auth/2 merges auth opts" do
      config = %Config{host: "localhost", port: 9100, auth: {:password, "user", "pass"}}
      opts = [database: "mydb"]

      merged = Query.with_auth(opts, config)
      assert merged[:user_name] == "user"
      assert merged[:password] == "pass"
      assert merged[:database] == "mydb"
    end
  end

  describe "Result" do
    test "parse_output/2 parses TabSeparated format" do
      output = "a\tb\tc\n1\t2\t3\n"
      rows = Result.parse_output(output, "TabSeparated")

      assert rows == [["a", "b", "c"], ["1", "2", "3"]]
    end

    test "parse_output/2 handles empty output" do
      assert Result.parse_output("", "TabSeparated") == []
      assert Result.parse_output(nil, "TabSeparated") == []
    end

    test "to_maps/1 converts rows to maps" do
      result = %Result{
        columns: [%{name: "name", type: "String"}, %{name: "age", type: "UInt8"}],
        rows: [["alice", "25"], ["bob", "30"]]
      }

      maps = Result.to_maps(result)

      assert maps == [
               %{"name" => "alice", "age" => "25"},
               %{"name" => "bob", "age" => "30"}
             ]
    end

    test "merge/1 combines multiple results" do
      result1 = %Result{
        query_id: "q1",
        columns: [%{name: "x", type: "Int"}],
        rows: [["1"], ["2"]]
      }

      result2 = %Result{
        query_id: "q1",
        columns: [%{name: "x", type: "Int"}],
        rows: [["3"], ["4"]],
        stats: %{rows: 4}
      }

      merged = Result.merge([result1, result2])

      assert merged.rows == [["1"], ["2"], ["3"], ["4"]]
      assert merged.stats == %{rows: 4}
    end
  end
end
