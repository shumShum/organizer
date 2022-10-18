defmodule Organizer.PipelineTest do
  use ExUnit.Case, async: true

  alias Organizer.Pipeline
  alias Organizer.Step

  describe "new" do
    test "returns ok if pipeline is valid" do
      steps = [
        Step.task(1, IO, :inspect, []),
        Step.task(2, IO, :inspect, [{:in, 1}]),
        Step.task(3, IO, :inspect, [], wait: [2])
      ]

      assert {:ok, pipeline} = Pipeline.new(steps)

      assert [
               %{v1: 1, v2: 2},
               %{v1: 2, v2: 3}
             ] = Graph.edges(pipeline.graph)
    end

    test "returns error if there are cyclic dependencies in graph" do
      steps = [
        Step.task(1, IO, :inspect, [{:in, 3}]),
        Step.task(2, IO, :inspect, [{:in, 1}]),
        Step.task(3, IO, :inspect, [{:in, 2}])
      ]

      assert {:error, :graph_is_not_acyclic} = Pipeline.new(steps)
    end

    test "returns error if not all inputs are given as steps" do
      steps = [
        Step.task(1, IO, :inspect, [{:in, 2}]),
        Step.task(3, IO, :inspect, [{:in, 4}])
      ]

      assert {:error, {:inputs_not_satisfied, [4, 2]}} = Pipeline.new(steps)
    end

    test "returns ok if all elements in list input are present in steps" do
      steps = [
        Step.task(1, IO, :inspect, []),
        Step.task(2, IO, :inspect, []),
        Step.task(3, IO, :inspect, [{:in, :list, [1, 2]}])
      ]

      assert {:ok, _pipeline} = Pipeline.new(steps)
    end

    test "returns error if element in list input is not present in steps" do
      steps = [
        Step.task(1, IO, :inspect, [{:in, :list, [2]}]),
        Step.task(3, IO, :inspect, [{:in, :list, [4]}])
      ]

      assert {:error, {:inputs_not_satisfied, [4, 2]}} = Pipeline.new(steps)
    end

    test "returns ok if return is present in steps" do
      steps = [
        Step.task(1, IO, :inspect, []),
        Step.task(2, IO, :inspect, [{:in, 1}]),
        Step.task(3, IO, :inspect, [{:in, 2}])
      ]

      return = 1

      assert {:ok, _pipeline} = Pipeline.new(steps, return: return)
    end

    test "returns error if return is not present in steps" do
      steps = [
        Step.task(1, IO, :inspect, []),
        Step.task(2, IO, :inspect, [{:in, 1}]),
        Step.task(3, IO, :inspect, [{:in, 2}])
      ]

      return = 4

      assert {:error, :invalid_return} = Pipeline.new(steps, return: return)
    end
  end

  describe "pop_next_steps" do
    setup do
      {:ok, pipeline} =
        Pipeline.new([
          Step.task(1, IO, :inspect, []),
          Step.task(2, IO, :inspect, [{:in, 1}]),
          Step.task(3, IO, :inspect, [{:in, 2}])
        ])

      [pipeline: pipeline]
    end

    test "returns steps with no required inputs when first called", %{pipeline: pipeline} do
      assert {[step_1], new_pipeline} = Pipeline.pop_next_steps(pipeline)

      assert step_1.output == 1
      assert length(new_pipeline.steps) == 2
    end

    test "returns steps with satisfied inputs", %{pipeline: pipeline} do
      {[step_1], new_pipeline} = Pipeline.pop_next_steps(pipeline)
      new_pipeline = Pipeline.put_result(new_pipeline, step_1.output, :result)

      assert {[step_2], new_pipeline} = Pipeline.pop_next_steps(new_pipeline)
      assert step_2.output == 2
      assert length(new_pipeline.steps) == 1
    end
  end
end
