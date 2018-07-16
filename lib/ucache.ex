defmodule UCache do
  @cache_strategy Memoize.Application.cache_strategy()

  defp tab(key) do
    @cache_strategy.tab(key)
  end

  defp compare_and_swap(key, :nothing, value) do
    :ets.insert_new(tab(key), value)
  end

  defp compare_and_swap(key, expected, :nothing) do
    num_deleted = :ets.select_delete(tab(key), [{expected, [], [true]}])
    num_deleted == 1
  end

  defp compare_and_swap(key, expected, value) do
    num_replaced = :ets.select_replace(tab(key), [{expected, [], [{:const, value}]}])
    num_replaced == 1
  end

  defp set_result_and_get_waiter_pids(key, result, context) do
    runner_pid = self()
    [{^key, {:running, ^runner_pid, waiter_pids}} = expected] = :ets.lookup(tab(key), key)

    if compare_and_swap(key, expected, {key, {:completed, result, context}}) do
      waiter_pids
    else
      # retry
      set_result_and_get_waiter_pids(key, result, context)
    end
  end

  defp delete_and_get_waiter_pids(key) do
    runner_pid = self()
    [{^key, {:running, ^runner_pid, waiter_pids}} = expected] = :ets.lookup(tab(key), key)

    if compare_and_swap(key, expected, :nothing) do
      waiter_pids
    else
      # retry
      delete_and_get_waiter_pids(key)
    end
  end

  def get_or_run(key, fun, opts \\ []) do
    do_get_or_run(key, fun, opts)
  end

  defp do_get_or_run(key, fun, opts) do
    case :ets.lookup(tab(key), key) do
      # not started
      [] ->
        # calc
        runner_pid = self()

        if compare_and_swap(key, :nothing, {key, {:running, runner_pid, []}}) do
          try do
            fun.()
          else
            result ->
              context = @cache_strategy.cache(key, result, opts)
              waiter_pids = set_result_and_get_waiter_pids(key, result, context)

              Enum.map(waiter_pids, fn pid ->
                send(pid, {self(), :completed})
              end)

              do_get_or_run(key, fun, opts)
          rescue
            error ->
              # the status should be :running
              waiter_pids = delete_and_get_waiter_pids(key)

              Enum.map(waiter_pids, fn pid ->
                send(pid, {self(), :failed})
              end)

              reraise error, System.stacktrace()
          end
        else
          do_get_or_run(key, fun, opts)
        end

      # running
      [{^key, {:running, runner_pid, waiter_pids}} = expected] ->
        waiter_pids = [self() | waiter_pids]

        if compare_and_swap(key, expected, {key, {:running, runner_pid, waiter_pids}}) do
          ref = Process.monitor(runner_pid)

          receive do
            {^runner_pid, :completed} -> :ok
            {^runner_pid, :failed} -> :ok
            {:DOWN, ^ref, :process, ^runner_pid, _reason} -> :ok
          after
            5000 -> :ok
          end

          Process.demonitor(ref, [:flush])
          # flush existing messages
          receive do
            {^runner_pid, _} -> :ok
          after
            0 -> :ok
          end

          do_get_or_run(key, fun, opts)
        else
          do_get_or_run(key, fun, opts)
        end

      # completed
      [{^key, {:completed, value, context}}] ->
        case @cache_strategy.read(key, value, context) do
          :retry -> do_get_or_run(key, fun, opts)
          :ok -> value
        end
    end
  end

  def invalidate() do
    @cache_strategy.invalidate()
  end

  def invalidate(key) do
    @cache_strategy.invalidate(key)
  end

  def garbage_collect() do
    @cache_strategy.garbage_collect()
  end
end
