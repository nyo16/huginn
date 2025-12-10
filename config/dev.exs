import Config

# Development ClickHouse configuration
# Start ClickHouse with: docker-compose up -d
config :huginn, :clickhouse,
  host: "localhost",
  port: 9100,
  database: "default",
  auth: {:password, "default", ""},
  pool_size: 5
