defmodule Organizer.Pipeline do
  @enforce_keys [:steps, :graph]
  defstruct steps: [],
            graph: nil,
            results: %{},
            return: nil

  alias Organizer.Step

  @type t :: %__MODULE__{
          steps: [Step.t()],
          graph: Graph.t(),
          results: %{Step.step_id() => term},
          return: term | nil
        }

  @type error :: :graph_is_not_acyclic

  @spec new([Step.t()], keyword) :: Result.t(t, :graph_is_not_acyclic)
  def new(steps, opts \\ []) do
    return = Keyword.get(opts, :return, nil)

    steps
    |> validate_return(return)
    |> Result.map(&make_graph/1)
    |> Result.chain(&check_is_acyclic/1)
    |> Result.chain(&validate_inputs(&1, steps))
    |> Result.map(fn graph ->
      %__MODULE__{
        steps: sort_steps(graph, steps),
        graph: graph,
        results: %{},
        return: return
      }
    end)
  end

  @spec put_result(t, Step.step_id(), term) :: t
  def put_result(%__MODULE__{} = p, step_id, result) do
    put_in(p.results[step_id], result)
  end

  @spec get_returning_value(t) :: term | nil
  def get_returning_value(%__MODULE__{return: nil}), do: nil
  def get_returning_value(%__MODULE__{results: results, return: return}), do: results[return]

  @spec pop_next_steps(t) :: {[Step.t()], t}
  def pop_next_steps(%__MODULE__{} = p) do
    {steps, remaining} =
      Enum.split_while(p.steps, fn %{output: step_id} ->
        p.graph
        |> Graph.in_neighbors(step_id)
        |> Enum.all?(fn in_step ->
          Map.has_key?(p.results, in_step)
        end)
      end)

    pipeline = %{p | steps: remaining}

    {steps, pipeline}
  end

  defp validate_return(steps, nil), do: {:ok, steps}

  defp validate_return(steps, return) do
    case Enum.any?(steps, &(&1.output == return)) do
      true -> {:ok, steps}
      false -> {:error, :invalid_return}
    end
  end

  defp make_graph(steps) do
    Enum.reduce(steps, Graph.new(type: :directed), fn step, graph ->
      edges =
        [step.inputs, step.wait]
        |> Enum.concat()
        |> Enum.map(&{&1, step.output})

      graph
      |> Graph.add_vertex(step.output)
      |> Graph.add_edges(edges)
    end)
  end

  defp check_is_acyclic(graph) do
    case Graph.is_acyclic?(graph) do
      true -> {:ok, graph}
      false -> {:error, :graph_is_not_acyclic}
    end
  end

  defp validate_inputs(graph, steps) do
    graph
    |> Graph.edges()
    |> Enum.map(& &1.v1)
    |> Enum.filter(fn input ->
      not Enum.find_value(steps, false, &(&1.output == input))
    end)
    |> case do
      [] -> {:ok, graph}
      unsatisfied_inputs -> {:error, {:inputs_not_satisfied, unsatisfied_inputs}}
    end
  end

  defp sort_steps(graph, steps) do
    graph
    |> Graph.topsort()
    |> Enum.map(fn step_id ->
      Enum.find(steps, &(&1.output == step_id))
    end)
  end
end
