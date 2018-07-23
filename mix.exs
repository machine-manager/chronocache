defmodule ChronoCache.Mixfile do
  use Mix.Project

  def project do
    [
      app: :chronocache,
      version: "1.0.0",
      elixir: "~> 1.6",
      description: "A simple in-memory cache with minimum freshness checks, backed by ETS",
      package: [
        maintainers: ["ivan"],
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/ludios/chronocache"}
      ],
      docs: [main: "ChronoCache"],
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      source_url: "https://github.com/ludios/chronocache"
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [{:ex_doc, "~> 0.18.3", only: :dev, runtime: false}]
  end
end
