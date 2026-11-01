defmodule Huginn.MixProject do
  use Mix.Project

  @version "0.6.0"
  @source_url "https://github.com/nyo16/huginn"

  def project do
    [
      app: :huginn,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      name: "Huginn",
      source_url: @source_url,
      homepage_url: @source_url,
      dialyzer: [plt_file: {:no_warn, "priv/plts/project.plt"}],
      # Generated protobuf/gRPC modules would otherwise dominate coverage numbers.
      test_coverage: [ignore_modules: [~r/^Clickhouse\.Grpc\./]],
      hex: [
        # CVE-2026-43966 (aliases EEF-CVE-2026-43966, GHSA-w4f7-4cxr-rv3c), reported
        # by `mix hex.audit` against cowlib and gun. No release fixes it: cowlib is
        # affected from 2.9.0 with no fixed version, and the GHSA record's gun
        # "fixed 2.16.0" is a cowboy version (its own data says gun < 2.4.0).
        # Unreachable here: the flaw is in cowlib's structured-header *encoder*
        # (`cow_http_struct_hd:item/list/dictionary`, wrapped by `cow_http_hd`'s
        # `variant_key/variants/wt_*`), which nothing in lib/ or in gun, grpc,
        # grpc_core or grpc_connection_pool calls; gun >= 2.4.0 also rejects CR/LF
        # in request headers. Streaming calls use Mint, which does not use cowlib.
        # Revisit when cowlib ships a fix.
        ignore_advisories: ["CVE-2026-43966"]
      ]
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
      {:grpc, "~> 1.0.5"},
      {:grpc_connection_pool, "~> 0.5.3"},
      {:mint, "~> 1.11"},
      {:protobuf, "~> 0.17.1"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
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
