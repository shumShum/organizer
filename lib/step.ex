defmodule Organizer.Step do
  require Logger

  @derive {Inspect, only: [:kind, :output, :module, :func, :timeout, :safe]}
  @enforce_keys [:kind, :output]
  defstruct kind: :task,
            inputs: [],
            wait: [],
            output: nil,
            module: nil,
            func: nil,
            args: nil,
            safe: false,
            timeout: nil

  @type kind :: :task | :gen_server
  @type step_id :: term
  @type arg :: term | {:in, step_id} | {:in, :list, [step_id]}
  @type t :: %__MODULE__{
          kind: kind,
          inputs: [step_id],
          wait: [step_id],
          output: step_id,
          module: module,
          func: atom,
          args: [arg],
          safe: boolean,
          timeout: timeout()
        }

  @type start_error :: term
  @default_timeout 60_000

  @spec task(step_id, module, atom, [arg], keyword) :: t
  def task(id, module, func, args, opts \\ []) do
    new(:task, id, module, func, args, opts)
  end

  @spec gen_server(step_id, module, [arg], keyword) :: t
  def gen_server(id, module, args, opts \\ []) do
    new(:gen_server, id, module, nil, args, opts)
  end

  def start(
        %__MODULE__{
          kind: :task,
          module: module,
          func: func,
          args: args,
          timeout: timeout
        } = step,
        map
      ) do
    args_resolved = resolve_args(args, map)

    Logger.info("Starting #{inspect(module)}.#{func}")

    task = Task.async(module, func, args_resolved)
    timer = start_timeout(self(), task.ref, timeout)

    {:ok,
     {task.ref,
      %{
        step: step,
        status: :in_progress,
        task: task,
        pid: task.pid,
        timer: timer
      }}}
  end

  def start(
        %__MODULE__{
          kind: :gen_server,
          module: module,
          args: args,
          timeout: timeout
        } = step,
        map
      ) do
    args_resolved = resolve_args(args, map)
    parent = self()
    ref = make_ref()

    Logger.info("Starting #{inspect(module)} gen_server")

    [parent, ref | args_resolved]
    |> module.start_link()
    |> Result.map(fn pid ->
      timer = start_timeout(self(), ref, timeout)

      {ref,
       %{
         step: step,
         status: :in_progress,
         pid: pid,
         timer: timer
       }}
    end)
  end

  defp start_timeout(_, _, :infinity), do: nil

  defp start_timeout(parent, ref, timeout) do
    Process.send_after(parent, {:kill, ref}, timeout)
  end

  defp inputs_from_args(args) do
    Enum.reduce(args, [], fn
      {:in, input}, acc -> [input | acc]
      {:in, :list, inputs}, acc -> Enum.concat(inputs, acc)
      _, acc -> acc
    end)
  end

  defp resolve_args(args, %{} = map) when is_list(args) do
    do_resolve_args(args, map, [])
  end

  defp do_resolve_args([], _map, acc), do: Enum.reverse(acc)

  defp do_resolve_args([{:in, input} | rest], map, acc) do
    do_resolve_args(rest, map, [map[input] | acc])
  end

  defp do_resolve_args([{:in, :list, inputs} | rest], map, acc) do
    values = Enum.map(inputs, &map[&1])
    do_resolve_args(rest, map, [values | acc])
  end

  defp do_resolve_args([h | rest], map, acc) do
    do_resolve_args(rest, map, [h | acc])
  end

  defp new(kind, id, module, func, args, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    wait = Keyword.get(opts, :wait, [])
    safe = Keyword.get(opts, :safe, false)

    %__MODULE__{
      kind: kind,
      output: id,
      module: module,
      func: func,
      args: args,
      inputs: inputs_from_args(args),
      wait: wait,
      safe: safe,
      timeout: timeout
    }
  end
end
