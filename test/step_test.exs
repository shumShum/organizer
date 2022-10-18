defmodule Organizer.StepTest do
  use ExUnit.Case, async: true

  alias Organizer.Step

  def echo(pid, arg) do
    send(pid, arg)
  end

  describe "args resolution" do
    test "resolves regular args" do
      step = Step.task("id", __MODULE__, :echo, [self(), {:in, :other_step}])

      other_step_result = :result
      assert {:ok, _} = Step.start(step, %{other_step: other_step_result})

      assert_receive ^other_step_result
    end

    test "resolves list args" do
      step =
        Step.task("id", __MODULE__, :echo, [self(), {:in, :list, [:other_step_1, :other_step_1]}])

      other_step_1_result = :result
      other_step_2_result = :result

      assert {:ok, _} =
               Step.start(step, %{
                 other_step_1: other_step_1_result,
                 other_step_2: other_step_2_result
               })

      assert_receive [^other_step_1_result, ^other_step_2_result]
    end
  end
end
