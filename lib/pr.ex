defmodule Test.Pr do
  use Organizer.Job,
    params: __MODULE__.Params,
    return: :step_c

  defmodule Params do
    def cast(x), do: {:ok, x}
  end

  def steps(params) do
    {:ok, [
      Step.task(:step_a, __MODULE__, :foo, [params.x]),
      Step.task(:step_b, __MODULE__, :bar, ["y"]),
      Step.task(:step_c, __MODULE__, :res, [{:in, :step_a}, {:in, :step_b}]),
    ]}
  end

  def foo(a) do
    IO.inspect(a, label: :FOO)
    {:ok, a}
  end

  def bar(a) do
    IO.inspect(a, label: :BAR)
    {:ok, 2}
  end

  def res(a, b) do
    IO.inspect(a, label: :A)
    IO.inspect(b, label: :B)
    {:ok, a + b}
  end
end

# Test.Pr.run(%{x: 6})
