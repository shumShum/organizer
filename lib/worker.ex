defmodule Organizer.Worker do
  use GenServer
  alias Organizer.Pipeline
  alias Organizer.Step
  require Logger

  defstruct pipeline: nil,
            spawned_workers: %{},
            finished_pids: %{},
            from: nil

  @type worker_status :: :in_progress | :finished | :timeout
  @type worker_state :: %{
          step: Step.t(),
          status: worker_status,
          task: Task.t() | nil,
          ref: reference(),
          pid: pid(),
          timer: reference()
        }
  @type state :: %__MODULE__{
          pipeline: Pipeline.t(),
          spawned_workers: %{reference() => worker_state},
          finished_pids: %{pid => true},
          from: GenServer.from()
        }

  @type error :: Pipeline.error() | Step.start_error() | :child_timeout

  ##############################
  ## Public API
  ##############################
  def start_link(params \\ []) do
    __MODULE__
    |> GenServer.start_link(params)
    |> Result.tap(fn pid ->
      Logger.debug("Started. pid: #{inspect(pid)}")
    end)
  end

  @doc """
  Opts:
    return:   selects which step result to return.
                when `nil`, will return :ok
  """

  @spec call(atom | pid, [Step.t()] | [[Step.t()]], keyword) :: Result.t(:ok, error)
  def call(pid, steps, opts \\ []) when is_pid(pid) do
    GenServer.call(pid, %{steps: steps, opts: opts}, :infinity)
  end

  ##############################
  ## Private
  ##############################
  @spec start_jobs(state) :: Result.t(state, Step.start_error())
  defp start_jobs(%__MODULE__{} = state) do
    {steps, pipeline} = Pipeline.pop_next_steps(state.pipeline)

    steps
    |> Result.map_all(&Step.start(&1, pipeline.results))
    |> Result.map(fn spawned_worker_list ->
      %{
        state
        | pipeline: pipeline,
          spawned_workers: Map.merge(state.spawned_workers, Map.new(spawned_worker_list))
      }
    end)
  end

  @spec handle_step_result(reference, term, worker_state, state) ::
          Result.t({:cont | :halt, state}, term)
  defp handle_step_result(ref, result, worker_state, %__MODULE__{} = state) do
    cancel_timer(worker_state)

    step_id = worker_state.step.output

    state
    |> Map.update!(:spawned_workers, &put_in(&1[ref][:status], :finished))
    |> Map.update!(:pipeline, &Pipeline.put_result(&1, step_id, result))
    |> Map.update!(:finished_pids, &Map.put(&1, worker_state.pid, true))
    |> case do
      %{pipeline: %{steps: []} = p} = state ->
        cont =
          case all_finished?(state) do
            true -> {:reply, get_reply(p)}
            false -> :cont
          end

        {:ok, {cont, state}}

      state ->
        state
        |> start_jobs()
        |> Result.map(&{:cont, &1})
    end
  end

  defp try_save_step(result, step) do
    case {result, step} do
      {{:ok, _} = ok_result, _} ->
        ok_result

      {{:error, error}, %{safe: true}} ->
        Logger.warn("Safe step failed", error: inspect(error))
        {:ok, {:safe_step_failed, error: error}}

      {err_result, _} ->
        err_result
    end
  end

  defp all_finished?(%__MODULE__{spawned_workers: spawned}) do
    Enum.all?(spawned, fn {_ref, %{status: status}} -> status == :finished end)
  end

  defp get_reply(pipeline) do
    case Pipeline.get_returning_value(pipeline) do
      nil -> {:ok, :no_output}
      value -> {:ok, value}
    end
  end

  defp get_worker_state_by_ref!(%__MODULE__{} = state, ref) do
    Map.fetch!(state.spawned_workers, ref)
  end

  defp get_step_id_by_ref!(%__MODULE__{} = state, ref) do
    Map.fetch!(state.spawned_workers, ref).step.output
  end

  defp get_step_id_by_pid!(%__MODULE__{} = state, pid) do
    state.spawned_workers
    |> Map.values()
    |> Enum.find(&(&1.pid == pid))
    |> Map.fetch!(:step)
    |> Map.fetch!(:output)
  end

  defp get_ref_by_pid!(%__MODULE__{} = state, pid) do
    Enum.find_value(state.spawned_workers, fn
      {ref, %{pid: ^pid}} -> ref
      _ -> false
    end)
  end

  defp handle_worker_timeout(ref, %__MODULE__{} = state) do
    case state.spawned_workers[ref] do
      %{status: :finished} ->
        {:ignore, state}

      _ ->
        # TODO: возможно, это нужно убрать,
        # но не уверен
        stop_children(state)
        state = put_in(state.spawned_workers[ref].status, :timeout)
        {:die, state}
    end
  end

  defp stop_children(%__MODULE__{} = state) do
    Enum.each(state.spawned_workers, fn {_ref, worker_state} ->
      stop_child(worker_state)
      cancel_timer(worker_state)
    end)
  end

  defp stop_child(%{task: task}) when not is_nil(task), do: Task.shutdown(task)
  defp stop_child(%{pid: pid}), do: Process.alive?(pid) && GenServer.stop(pid)

  defp cancel_timer(%{timer: nil}), do: :skip
  defp cancel_timer(%{timer: timer}), do: Process.cancel_timer(timer)

  defp handle_worker_exit(pid, _reason, %__MODULE__{} = state) do
    case Map.fetch(state.finished_pids, pid) do
      {:ok, _} -> :ok
      _ -> :error
    end
  end

  defp reply_step_error(%__MODULE__{from: from} = state, _, %{step_id: _, reason: _} = err) do
    do_reply(from, {:error, err}, state)
  end

  defp reply_step_error(%__MODULE__{from: from} = state, ref, reason) when is_reference(ref) do
    step_id = get_step_id_by_ref!(state, ref)
    do_reply(from, {:error, %{step_id: step_id, reason: reason}}, state)
  end

  defp reply_step_error(%__MODULE__{from: from} = state, pid, reason) when is_pid(pid) do
    step_id = get_step_id_by_pid!(state, pid)
    do_reply(from, {:error, %{step_id: step_id, reason: reason}}, state)
  end

  defp do_reply(from, result, state) do
    GenServer.reply(from, result)
    {:noreply, state}
  end

  ##############################
  ## GenServer callbacks
  ##############################
  def init(_) do
    Process.flag(:trap_exit, true)
    {:ok, nil}
  end

  def handle_call(%{steps: []}, from, state) do
    Logger.info("Called with empty steps")
    do_reply(from, {:ok, :ok}, state)
  end

  def handle_call(%{steps: steps, opts: opts}, from, state) do
    steps
    |> Pipeline.new(opts)
    |> Result.map(&%__MODULE__{pipeline: &1, from: from})
    |> Result.chain(&start_jobs/1)
    |> Result.fold(
      &{:noreply, &1},
      &do_reply(from, {:error, &1}, state)
    )
  end

  def handle_info({ref, result}, %__MODULE__{from: from} = state) when is_reference(ref) do
    worker_state = get_worker_state_by_ref!(state, ref)

    Logger.info("Received #{inspect(worker_state.step.output)} task result",
      ref: inspect(ref),
      from: inspect(from)
    )

    result
    |> Result.of()
    |> try_save_step(worker_state.step)
    |> Result.chain(&handle_step_result(ref, &1, worker_state, state))
    |> Result.fold(
      fn
        {:cont, state} -> {:noreply, state}
        {{:reply, reply}, state} -> do_reply(from, reply, state)
      end,
      fn reason -> reply_step_error(state, ref, reason) end
    )
  end

  def handle_info({:kill, timeout_ref}, %__MODULE__{} = state) do
    Logger.error("Received task timeout", ref: inspect(timeout_ref))

    case handle_worker_timeout(timeout_ref, state) do
      {:ignore, state} ->
        {:noreply, state}

      {:die, state} ->
        result = {:error, :child_timeout}
        send(self(), {timeout_ref, result})
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, reason}, %__MODULE__{} = state) do
    if handle_worker_exit(pid, reason, state) == :error do
      result = {:error, format_crash_reason(reason)}
      ref = get_ref_by_pid!(state, pid)

      send(self(), {ref, result})
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, _, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(message, state) do
    Logger.warn("Unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  defp format_crash_reason({error, [_ | _] = stacktrace}) do
    {:child_crashed, Exception.format(:error, error, stacktrace)}
  end

  defp format_crash_reason(error), do: {:child_crashed, error}
end
