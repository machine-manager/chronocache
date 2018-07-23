# value_table schema:
# {key, {current_value, time_computed}}
#
# waiter_table schema:
# {{key, time_started}, {runner_pid, waiter_pids}}

defmodule ChronoCache do
  @enforce_keys [:value_table, :waiter_table, :get_time, :compute_value]
  defstruct value_table: nil, waiter_table: nil, get_time: nil, compute_value: nil

  def new(compute_value, get_time) do
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

  @doc """
  Returns the cached value for `key` with minimum freshness `minimum_time`, or
  if not in cache, calls `cc.compute_value` with the key as the only argument.
  `cc.compute_value` will be called just once if more than one process is waiting,
  unless the `minimum_time` is raised, in which case `cc.compute_value` may be
  called again.  Note that the value returned may be newer than expected, as
  only the newest value is kept in the cache.
  """
  def get(cc, key, minimum_time) do
    do_get(cc, key, minimum_time)
  end

  # If we're still running for Time 1 and someone else requests Time 2,
  # we need to run the compute_value function again.  The value returned
  # from the function that is still running for Time 1 might not be fresh
  # enough for Time 2.  Time 1 callers may be given the Time 1 or the Time 2
  # calculation.  The value in the cache will be replaced with the Time 2
  # value as soon as the Time 2 calculation finishes.
  defp do_get(cc, key, minimum_time) do
    case :ets.lookup(cc.value_table, key) do
      # not started
      [] ->
        compute_value(cc, key, minimum_time)

      [{^key, start_time, result}] when start_time >= minimum_time ->
        result

      _ ->
        expected = get_latest_waiter(cc, key)
        case expected do
          {{^key, start_time}, {runner_pid, waiter_pids}} when start_time >= minimum_time ->
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

              do_get(cc, key, minimum_time)
            else
              do_get(cc, key, minimum_time)
            end

          _ ->
            compute_value(cc, key, minimum_time)
        end
    end
  end

  defp get_latest_waiter(cc, key) do
    limit = 1
    match_pattern = {{key, :_}, :_}
    # http://erlang.org/doc/apps/erts/match_spec.html#ets-examples
    match_spec = [{match_pattern, [], [:"$_"]}]

    case :ets.select_reverse(cc.waiter_table, match_spec, limit) do
      {[object], _continuation} -> object
      _ -> nil
    end
  end

  defp compute_value(cc, key, minimum_time) do
    runner_pid = self()
    start_time = cc.get_time.()

    if compare_and_swap(cc.waiter_table, :nothing, {{key, start_time}, {runner_pid, []}}) do
      try do
        cc.compute_value.(key)
      else
        result ->
          waiter_pids = set_result_and_get_waiter_pids(cc, key, start_time, result)

          Enum.map(waiter_pids, fn pid ->
            send(pid, {self(), :completed})
          end)

          do_get(cc, key, minimum_time)
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
      do_get(cc, key, minimum_time)
    end
  end

  defp set_result_and_get_waiter_pids(cc, key, start_time, result) do
    case :ets.lookup(cc.value_table, key) do
      [{^key, _existing_value, existing_start_time} = expected] ->
        if start_time > existing_start_time do
          # If this fails, it may have been CAS'ed by another process, but we
          # still need to retry because we might have an even-newer value.
          if compare_and_swap(cc.value_table, expected, {key, start_time, result}) do
            clear_and_get_waiter_pids(cc, key, start_time)
          else
            # retry
            set_result_and_get_waiter_pids(cc, key, start_time, result)
          end
        end
      [] ->
        if compare_and_swap(cc.value_table, :nothing, {key, start_time, result}) do
          clear_and_get_waiter_pids(cc, key, start_time)
        else
          # retry
          set_result_and_get_waiter_pids(cc, key, start_time, result)
        end
    end
  end

  defp clear_and_get_waiter_pids(cc, key, start_time) do
    runner_pid = self()
    [{{^key, ^start_time}, {^runner_pid, waiter_pids}} = expected] = :ets.lookup(cc.waiter_table, {key, start_time})

    if compare_and_swap(cc.waiter_table, expected, :nothing) do
      waiter_pids
    else
      # retry
      clear_and_get_waiter_pids(cc, key, start_time)
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
