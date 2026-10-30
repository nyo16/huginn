import Config

# Do not read secrets here: config/prod.exs is evaluated at build time, so
# values would be baked into the release. Configure ClickHouse in
# config/runtime.exs instead, which runs when the release boots:
#
# # config/runtime.exs
# import Config
#
# if config_env() == :prod do
#   config :huginn, :clickhouse,
#     host: System.fetch_env!("CLICKHOUSE_HOST"),
#     port: String.to_integer(System.get_env("CLICKHOUSE_PORT", "9100")),
#     database: System.get_env("CLICKHOUSE_DATABASE", "default"),
#     auth: {:password, System.fetch_env!("CLICKHOUSE_USER"), System.fetch_env!("CLICKHOUSE_PASSWORD")},
#     pool_size: String.to_integer(System.get_env("CLICKHOUSE_POOL_SIZE", "10")),
#     ssl: true
# end
