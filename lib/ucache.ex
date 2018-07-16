defmodule UCache do
  def new() do
    :ets.new(:ucache, [:public, :set, {:read_concurrency, true}])
  end

  def invalidate(table, key) do
    :ets.select_delete(table, [{{key, {:completed, :_}}, [], [true]}])
  end

  def clear(table) do
    :ets.select_delete(table, [{{:_, {:completed, :_}}, [], [true]}])
  end

  def destroy(table) do
    :ets.delete(table)
  end

  def get_or_run(table, key, fun) do
    do_get_or_run(table, key, fun)
  end

  defp do_get_or_run(table, key, fun) do
    case :ets.lookup(table, key) do
      # not started
      [] ->
        # calc
        runner_pid = self()

        if compare_and_swap(table, :nothing, {key, {:running, runner_pid, []}}) do
          try do
            fun.()
          else
            result ->
              waiter_pids = set_result_and_get_waiter_pids(table, key, result)

              Enum.map(waiter_pids, fn pid ->
                send(pid, {self(), :completed})
              end)

              do_get_or_run(table, key, fun)
          rescue
            error ->
              # the status should be :running
              waiter_pids = delete_and_get_waiter_pids(table, key)

              Enum.map(waiter_pids, fn pid ->
                send(pid, {self(), :failed})
              end)

              reraise error, System.stacktrace()
          end
        else
          do_get_or_run(table, key, fun)
        end

      # running
      [{^key, {:running, runner_pid, waiter_pids}} = expected] ->
        waiter_pids = [self() | waiter_pids]

        if compare_and_swap(table, expected, {key, {:running, runner_pid, waiter_pids}}) do
          ref = Process.monitor(runner_pid)

          receive do
            {^runner_pid, :completed} -> :ok
            {^runner_pid, :failed} -> :ok
            {:DOWN, ^ref, :process, ^runner_pid, _reason} -> :ok
          end

          Process.demonitor(ref, [:flush])
          # flush existing messages
          receive do
            {^runner_pid, _} -> :ok
          after
            0 -> :ok
          end

          do_get_or_run(table, key, fun)
        else
          do_get_or_run(table, key, fun)
        end

      # completed
      [{^key, {:completed, value}}] ->
        value
    end
  end

  defp set_result_and_get_waiter_pids(table, key, result) do
    runner_pid = self()
    [{^key, {:running, ^runner_pid, waiter_pids}} = expected] = :ets.lookup(table, key)

    if compare_and_swap(table, expected, {key, {:completed, result}}) do
      waiter_pids
    else
      # retry
      set_result_and_get_waiter_pids(table, key, result)
    end
  end

  defp delete_and_get_waiter_pids(table, key) do
    runner_pid = self()
    [{^key, {:running, ^runner_pid, waiter_pids}} = expected] = :ets.lookup(table, key)

    if compare_and_swap(table, expected, :nothing) do
      waiter_pids
    else
      # retry
      delete_and_get_waiter_pids(table, key)
    end
  end

  defp compare_and_swap(table, :nothing, value) do
    :ets.insert_new(table, value)
  end

  defp compare_and_swap(table, expected, :nothing) do
    num_deleted = :ets.select_delete(table, [{expected, [], [true]}])
    num_deleted == 1
  end

  defp compare_and_swap(table, expected, value) do
    num_replaced = :ets.select_replace(table, [{expected, [], [{:const, value}]}])
    num_replaced == 1
  end
end
