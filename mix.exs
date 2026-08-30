defmodule Huginn.MixProject do
  use Mix.Project

  @version "0.5.0"
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
      source_url: @source_url,
      homepage_url: @source_url,
      dialyzer: [plt_file: {:no_warn, "priv/plts/project.plt"}]
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
      {:grpc_connection_pool, "~> 0.5.2"},
      {:protobuf, "~> 0.17"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
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
        "GitHub" => @source_url,
        "Changelog" => "https://hexdocs.pm/huginn/changelog.html"
      },
      files: ~w(lib priv/protos .formatter.exs mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"],
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end
