defmodule Organizer.MixProject do
  use Mix.Project

  def project do
    [
      app: :organizer,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      elixirc_options: [
        long_compilation_threshold: 1000,
        warnings_as_errors: Mix.env() != :dev
      ],
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:params, git: "https://github.com/ppraisethesun/params.git", branch: "master"},
      {:result, git: "https://github.com/ppraisethesun/result.git", branch: "master"},
      {:libgraph, "~> 0.13"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:mox, "~> 0.4", only: :test}
    ]
  end
end
