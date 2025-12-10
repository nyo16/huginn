defmodule Huginn.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/nyo16/huginn"

  def project do
    [
      app: :huginn,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      name: "Huginn",
      source_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Huginn.Application, []}
    ]
  end

  defp deps do
    [
      {:grpc_connection_pool, "~> 0.2.1"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp description do
    "ClickHouse client for Elixir using gRPC with connection pooling."
  end

  defp package do
    [
      name: "huginn",
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib priv/protos .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end
