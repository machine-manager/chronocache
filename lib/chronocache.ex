# value_table schema:
# {key, {current_value, time_computed}}
#
# waiter_table schema:
# {key, [{time_started, runner_pid, waiter_pids}, ...]}

defmodule ChronoCache do
  @enforce_keys [:value_table, :waiter_table, :get_time, :compute_value]
  defstruct value_table: nil, waiter_table: nil, get_time: nil, compute_value: nil

  def new(get_time, compute_value) do
    value_table = :ets.new(:chronocache_value_table, [:public, :set, {:read_concurrency, true}])

    waiter_table =
      :ets.new(:chronocache_waiter_table, [
        :public,
        :set,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    %ChronoCache{
      value_table: value_table,
      waiter_table: waiter_table,
      get_time: get_time,
      compute_value: compute_value
    }
  end

  def destroy(cc) do
    :ets.delete(cc.value_table)
    :ets.delete(cc.waiter_table)
  end

  def get_or_run(cc, key) do
    do_get_or_run(cc, key)
  end

  # If we're still running for Time 1 and someone else requests Time 2,
  # we need to run the compute_value function again.  The value returned
  # from the function that is still running for Time 1 might not be fresh
  # enough for Time 2.  Time 1 callers will be given the Time 1 calculation,
  # but the value will be replaced in the cache with the Time 2 value as soon
  # as the Time 2 calculation finishes.
  defp do_get_or_run(cc, key, minimum_time) do
    case :ets.lookup(cc.value_table, key) do
      # not started
      [] ->
        compute_value(cc, key)

      # running
      [{^key, {:running, runner_pid, waiter_pids}} = expected] ->
        waiter_pids = [self() | waiter_pids]

        if compare_and_swap(cc, expected, {key, {:running, runner_pid, waiter_pids}}) do
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

          do_get_or_run(cc, key)
        else
          do_get_or_run(cc, key)
        end

      # completed
      [{^key, {:completed, value, _time}}] ->
        value
    end
  end

  defp compute_value(cc, key) do
    runner_pid = self()

    if compare_and_swap(cc, :nothing, {key, {:running, runner_pid, []}}) do
      try do
        fallback.()
      else
        result ->
          waiter_pids = set_result_and_get_waiter_pids(cc, key, result, time)

          Enum.map(waiter_pids, fn pid ->
            send(pid, {self(), :completed})
          end)

          do_get_or_run(cc, key)
      rescue
        error ->
          # the status should be :running
          waiter_pids = delete_and_get_waiter_pids(cc, key)

          Enum.map(waiter_pids, fn pid ->
            send(pid, {self(), :failed})
          end)

          reraise error, System.stacktrace()
      end
    else
      do_get_or_run(cc, key)
    end
  end

  defp set_result_and_get_waiter_pids(cc, key, result, time) do
    runner_pid = self()
    [{^key, {:running, ^runner_pid, waiter_pids}} = expected] = :ets.lookup(cc.value_table, key)

    if compare_and_swap(cc, expected, {key, {:completed, result, time}}) do
      waiter_pids
    else
      # retry
      set_result_and_get_waiter_pids(cc, key, result, time)
    end
  end

  defp delete_and_get_waiter_pids(cc, key) do
    runner_pid = self()
    [{^key, {:running, ^runner_pid, waiter_pids}} = expected] = :ets.lookup(cc.value_table, key)

    if compare_and_swap(cc, expected, :nothing) do
      waiter_pids
    else
      # retry
      delete_and_get_waiter_pids(cc, key)
    end
  end

  defp compare_and_swap(cc, :nothing, value) do
    :ets.insert_new(cc.value_table, value)
  end

  defp compare_and_swap(cc, expected, :nothing) do
    num_deleted = :ets.select_delete(cc.value_table, [{expected, [], [true]}])
    num_deleted == 1
  end

  defp compare_and_swap(cc, expected, value) do
    num_replaced = :ets.select_replace(cc.value_table, [{expected, [], [{:const, value}]}])
    num_replaced == 1
  end
end
