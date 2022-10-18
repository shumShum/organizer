defmodule Organizer.Job do
  alias Organizer.Pipeline

  @type params :: struct
  @callback steps(params) :: Result.t(Pipeline.t(), term)
  @callback handle_error(term) :: any

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @params Keyword.fetch!(opts, :params)

      @return Keyword.get(opts, :return)
      @max_attempts Keyword.get(opts, :max_attempts, 1)

      alias Organizer.Step
      alias Organizer.Worker

      require Logger

      def run(args) do
        args
        |> cast()
        |> Result.chain(&steps/1)
        |> Result.chain(&run_worker/1)
        |> Result.tap_err(&try_to_handle_error(&1, args))
      end

      defp cast(args) do
        args
        |> @params.cast()
        |> Result.map_err(fn error ->
          Logger.error("Failed to cast job args.",
            args: inspect(args),
            error: inspect(error)
          )

          {:error, %{reason: :params_error, details: error}}
        end)
      end

      defp run_worker(steps) do
        Worker.start_link()
        |> Result.chain(&Worker.call(&1, steps, return: @return))
      end

      defp try_to_handle_error(error, args) do
        handle_error(error, args)
      rescue
        e ->
          Logger.error(Exception.format(:error, e, __STACKTRACE__))
      end

      def handle_error(_error, _args), do: :noop

      defoverridable handle_error: 2
    end
  end
end
