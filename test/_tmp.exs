defmodule Test.Pr do
  use Organizer.Job,
    params: __MODULE__.Params,
    return: :step_c

  use Params

  defparams(_params, %{
    x!: :integer,
    y!: :integer
  })

  def steps(params) do
    {:ok,
     [
       Step.task(:step_a, __MODULE__, :foo, [params.x]),
       Step.task(:just_ok, __MODULE__, :just_ok, [params.y]),
       Step.task(:step_b, __MODULE__, :bar, [params.y]),
       Step.task(:step_c, __MODULE__, :res, [{:in, :step_a}, {:in, :step_b}])
     ]}
  end

  def foo(x) do
    IO.inspect(x, label: :FOO)
    {:ok, x}
  end

  def just_ok(_) do
    :ok
  end

  def bar(y) do
    IO.inspect(y, label: :BAR)
    {:ok, y}
  end

  def res(a, b) do
    IO.inspect(a, label: :A)
    IO.inspect(b, label: :B)
    {:ok, a + b}
  end
end

# Test.Pr.run(%{x: 6, y: 10})
