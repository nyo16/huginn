# Integration tests require a running ClickHouse (see docker-compose.yml) and
# are excluded by default. Run them with: mix test --include integration
ExUnit.configure(exclude: [:integration])
ExUnit.start()
