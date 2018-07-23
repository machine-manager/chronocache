# value_table schema:
# {key, {current_value, time_computed}}
#
# waiter_table schema:
# {{key, time_started}, {runner_pid, waiter_pids}}

defmodule ChronoCache do
  @enforce_keys [:value_table, :waiter_table, :get_time, :compute_value]
  defstruct value_table: nil, waiter_table: nil, get_time: nil, compute_value: nil

  def new(get_time, compute_value) do
    value_table = :ets.new(:chronocache_value_table, [:public, :set, {:read_concurrency, true}])

    waiter_table =
      :ets.new(:chronocache_waiter_table, [
        :public,
        :ordered_set,
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

  def get_or_run(cc, key, minimum_time) do
    do_get_or_run(cc, key, minimum_time)
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
        compute_value(cc, key, minimum_time)

      [{^key, value, start_time}] when start_time >= minimum_time ->
        value

      _ ->
        expected = get_latest_waiter(cc, key)
        case expected do
          nil ->
            compute_value(cc, key, minimum_time)

          {{^key, start_time}, {runner_pid, waiter_pids}} ->
            waiter_pids = [self() | waiter_pids]

            if compare_and_swap(cc.waiter_table, expected, {{key, start_time}, {runner_pid, waiter_pids}}) do
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

              do_get_or_run(cc, key, minimum_time)
            else
              do_get_or_run(cc, key, minimum_time)
            end
        end
    end
  end

  defp get_latest_waiter(cc, key) do
    limit = 1
    case :ets.match_object(cc.waiter_table, {{key, :_}, :_}, limit) do
      {[object], _continuation} -> object
      _ -> nil
    end
  end

  defp compute_value(cc, key, minimum_time) do
    runner_pid = self()
    start_time = cc.get_time.()

    if compare_and_swap(cc.waiter_table, :nothing, {{key, start_time}, {runner_pid, []}}) do
      try do
        cc.fallback.()
      else
        result ->
          waiter_pids = set_result_and_get_waiter_pids(cc, key, start_time, result)

          Enum.map(waiter_pids, fn pid ->
            send(pid, {self(), :completed})
          end)

          do_get_or_run(cc, key, minimum_time)
      rescue
        error ->
          # the status should be :running
          waiter_pids = delete_and_get_waiter_pids(cc, key, start_time)

          Enum.map(waiter_pids, fn pid ->
            send(pid, {self(), :failed})
          end)

          reraise error, System.stacktrace()
      end
    else
      # retry
      do_get_or_run(cc, key, minimum_time)
    end
  end

  defp set_result_and_get_waiter_pids(cc, key, start_time, result) do
    case :ets.lookup(cc.value_table, key) do
      [{^key, _existing_value, existing_start_time} = expected] ->
        if start_time > existing_start_time do
          # If this returns false, we don't need to retry it; it can only have been
          # replaced by an even-newer value.
          compare_and_swap(cc.value_table, expected, {key, start_time, result})
        end
      [] ->
        compare_and_swap(cc.value_table, :nothing, {key, start_time, result})
    end

    runner_pid = self()
    [{^key, {:running, ^runner_pid, waiter_pids}} = expected] = :ets.lookup(cc.waiter_table, key)

    if compare_and_swap(cc.waiter_table, expected, :nothing) do
      waiter_pids
    else
      # retry
      set_result_and_get_waiter_pids(cc, key, start_time, result)
    end
  end

  defp delete_and_get_waiter_pids(cc, key, start_time) do
    runner_pid = self()
    [{{^key, ^start_time}, {^runner_pid, waiter_pids}} = expected] = :ets.lookup(cc.waiter_table, {key, start_time})

    if compare_and_swap(cc.waiter_table, expected, :nothing) do
      waiter_pids
    else
      # retry
      delete_and_get_waiter_pids(cc, key, start_time)
    end
  end

  defp compare_and_swap(table, :nothing, value) do
    # insert_new returns false instead of overwriting
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
