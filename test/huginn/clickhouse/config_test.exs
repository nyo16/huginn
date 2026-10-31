defmodule Huginn.Clickhouse.ConfigTest do
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.Config

  describe "new/1" do
    test "creates config with defaults from a required host" do
      config = Config.new(host: "localhost")

      assert config.host == "localhost"
      assert config.port == 9100
      assert config.database == "default"
      assert config.pool_size == 5
      assert config.pool_name == :clickhouse_pool
      assert config.ssl == false
      assert config.auth == nil
    end

    test "accepts all options" do
      config =
        Config.new(
          host: "clickhouse.example.com",
          port: 9101,
          database: "mydb",
          auth: {:password, "user", "pass"},
          pool_size: 10,
          pool_name: :analytics,
          ssl: true
        )

      assert config.host == "clickhouse.example.com"
      assert config.port == 9101
      assert config.database == "mydb"
      assert config.auth == {:password, "user", "pass"}
      assert config.pool_size == 10
      assert config.pool_name == :analytics
      assert config.ssl == true
    end

    test "accepts a map" do
      config = Config.new(%{host: "localhost", database: "mydb", auth: {:jwt, "token"}})

      assert config.host == "localhost"
      assert config.database == "mydb"
      assert config.auth == {:jwt, "token"}
    end

    test "missing host raises ArgumentError without echoing the password" do
      error =
        assert_raise ArgumentError, fn ->
          Config.new(database: "mydb", auth: {:password, "user", "hunter2"})
        end

      assert error.message =~ ":host"
      refute error.message =~ "hunter2"
    end

    test "invalid auth raises without echoing the credentials" do
      error =
        assert_raise ArgumentError, fn ->
          Config.new(host: "localhost", auth: {:password, "hunter2"})
        end

      assert error.message =~ ":auth"
      refute error.message =~ "hunter2"
    end

    test "rejects unknown options by name only" do
      error =
        assert_raise ArgumentError, fn ->
          Config.new(host: "localhost", compression: "gzip", auth: {:password, "u", "hunter2"})
        end

      assert error.message =~ ":compression"
      refute error.message =~ "hunter2"
    end

    test "rejects non-positive port and pool_size" do
      assert_raise ArgumentError, ~r/:port/, fn -> Config.new(host: "localhost", port: 0) end
      assert_raise ArgumentError, ~r/:port/, fn -> Config.new(host: "localhost", port: "9100") end

      assert_raise ArgumentError, ~r/:pool_size/, fn ->
        Config.new(host: "localhost", pool_size: -1)
      end
    end
  end

  describe "Inspect" do
    test "redacts credentials" do
      config = Config.new(host: "localhost", auth: {:password, "user", "hunter2"})
      inspected = inspect(config)

      assert inspected =~ "localhost"
      refute inspected =~ "hunter2"
    end

    test "redacts jwt tokens" do
      config = Config.new(host: "localhost", auth: {:jwt, "secret-token"})

      refute inspect(config) =~ "secret-token"
    end
  end

  describe "auth_opts/1" do
    test "returns empty for nil auth" do
      assert Config.auth_opts(Config.new(host: "localhost")) == []
    end

    test "returns password credentials" do
      config = Config.new(host: "localhost", auth: {:password, "user", "secret"})
      assert Config.auth_opts(config) == [user_name: "user", password: "secret"]
    end

    test "returns jwt credentials" do
      config = Config.new(host: "localhost", auth: {:jwt, "token123"})
      assert Config.auth_opts(config) == [jwt: "token123"]
    end
  end

  describe "fetch/1" do
    test "returns a :not_configured error for an unknown pool" do
      assert {:error, %Huginn.Error{reason: :not_configured, details: :config_test_unknown_pool}} =
               Config.fetch(:config_test_unknown_pool)
    end

    test "returns a registered config" do
      config = Config.new(host: "localhost", pool_name: :config_test_registered)
      :ok = Config.register(config)

      assert Config.fetch(:config_test_registered) == {:ok, config}
    end
  end

  describe "to_pool_config/1" do
    test "generates a plaintext local endpoint by default" do
      config = Config.new(host: "localhost", port: 9100, pool_size: 3, pool_name: :test_pool)
      pool_config = Config.to_pool_config(config)

      assert pool_config[:endpoint] == [type: :local, host: "localhost", port: 9100]
      assert pool_config[:pool][:size] == 3
      assert pool_config[:pool][:name] == :test_pool
    end

    test "verifies the peer certificate when ssl: true" do
      config = Config.new(host: "secure.example.com", port: 443, ssl: true)
      pool_config = Config.to_pool_config(config)
      ssl = pool_config[:endpoint][:ssl]

      assert pool_config[:endpoint][:type] == :production
      # An empty list here would mean TLS without peer verification, which is
      # `verify_none` on OTP < 26 and silently MITM-able.
      assert ssl[:verify] == :verify_peer
      assert is_list(ssl[:cacerts]) and ssl[:cacerts] != []
      assert ssl[:customize_hostname_check] != nil
    end

    test "merges verifying defaults under ssl options that omit :verify" do
      config = Config.new(host: "secure.example.com", port: 443, ssl: [depth: 5])
      ssl = Config.to_pool_config(config)[:endpoint][:ssl]

      assert ssl[:verify] == :verify_peer
      assert is_list(ssl[:cacerts]) and ssl[:cacerts] != []
      assert ssl[:customize_hostname_check][:match_fun]
      assert ssl[:depth] == 5
    end

    test "keeps a user-supplied CA file instead of the system store" do
      config = Config.new(host: "secure.example.com", port: 443, ssl: [cacertfile: "/tmp/ca.pem"])
      ssl = Config.to_pool_config(config)[:endpoint][:ssl]

      assert ssl[:verify] == :verify_peer
      assert ssl[:cacertfile] == "/tmp/ca.pem"
      refute Keyword.has_key?(ssl, :cacerts)
    end

    test "user ssl keys win over the merged defaults" do
      check = [match_fun: :public_key.pkix_verify_hostname_match_fun(:https), fqdn_fun: & &1]

      config =
        Config.new(host: "secure.example.com", port: 443, ssl: [customize_hostname_check: check])

      ssl = Config.to_pool_config(config)[:endpoint][:ssl]

      assert ssl[:customize_hostname_check] == check
      assert Enum.count(ssl, fn {key, _} -> key == :customize_hostname_check end) == 1
    end

    test "passes ssl options with an explicit :verify through verbatim" do
      for ssl <- [[verify: :verify_peer, cacertfile: "/tmp/ca.pem"], [verify: :verify_none]] do
        config = Config.new(host: "secure.example.com", port: 443, ssl: ssl)
        pool_config = Config.to_pool_config(config)

        assert pool_config[:endpoint][:type] == :production
        assert pool_config[:endpoint][:ssl] == ssl
      end
    end
  end
end
