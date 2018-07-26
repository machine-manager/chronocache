defmodule ChronoCache do
  @enforce_keys [:value_table, :waiter_table, :get_time, :compute_value]
  defstruct value_table: nil, waiter_table: nil, get_time: nil, compute_value: nil

  require Logger

  def new(compute_value, get_time) do
    Logger.debug(
      "Creating chronocache with " <>
        "compute_value=#{inspect(compute_value)}, get_time=#{inspect(get_time)}"
    )

    # {key, {result, time_computed}}
    value_table = :ets.new(:chronocache_value_table, [:public, :set, {:read_concurrency, true}])

    # {{key, time_started}, {runner_pid, waiter_pids}}
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
    Logger.debug("Destroying chronocache #{inspect(cc)}")

    :ets.delete(cc.value_table)
    :ets.delete(cc.waiter_table)
  end

  @doc """
  Returns the cached value for `key` with minimum freshness `minimum_time`.
  If not available in the cache, internally calls `cc.compute_value` with `key`
  and `minimum_time` as arguments.  `cc.compute_value` will be called just once
  if more than one process is waiting, unless the `minimum_time` is raised, in
  which case `cc.compute_value` may be called again.  The value returned may be
  newer than expected, as only the newest value is kept in the cache.
  """
  def get(cc, key, minimum_time) do
    do_get(cc, key, minimum_time)
  end

  # If we're still running for Time 1 and another process requests Time 2,
  # we need to run the compute_value function again.  The value returned
  # from the function still running for Time 1 might not be fresh enough for
  # the Time 2 caller.  Time 1 callers may be given the Time 1 or the Time 2
  # result.  The value in the cache will be replaced with the Time 2 value as
  # soon as the Time 2 calculation finishes.
  defp do_get(cc, key, minimum_time) do
    case :ets.lookup(cc.value_table, key) do
      [{^key, start_time, result}] when start_time >= minimum_time ->
        Logger.debug(
          "get(#{inspect(key)}, #{inspect(minimum_time)}) returning cached result " <>
            "#{inspect(result)} from time #{inspect(start_time)}"
        )

        result

      _ ->
        expected = get_latest_waiter(cc, key)

        case expected do
          {{^key, start_time}, {runner_pid, waiter_pids}} when start_time >= minimum_time ->
            Logger.debug(
              "get(#{inspect(key)}, #{inspect(minimum_time)}) waiting for " <>
                "#{inspect(runner_pid)} with #{length(waiter_pids)} other waiters"
            )

            waiter_pids = [self() | waiter_pids]

            if compare_and_swap(
                 cc.waiter_table,
                 expected,
                 {{key, start_time}, {runner_pid, waiter_pids}}
               ) do
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

              # this will get the now-cached value
              do_get(cc, key, minimum_time)
            else
              Logger.debug(
                "Retrying do_get(#{inspect(key)}, #{inspect(minimum_time)}) " <>
                  "after failing to CAS existing waiter"
              )

              do_get(cc, key, minimum_time)
            end

          # no waiter, or need newer waiter
          _ ->
            Logger.debug("get(#{inspect(key)}, #{inspect(minimum_time)}) creating new waiter")

            runner_pid = self()
            start_time = cc.get_time.()

            if minimum_time > start_time do
              raise(
                "Invalid minimum_time #{inspect(minimum_time)}, greater than current time #{
                  inspect(start_time)
                }"
              )
            end

            if compare_and_swap(cc.waiter_table, :nothing, {{key, start_time}, {runner_pid, []}}) do
              try do
                cc.compute_value.(key, minimum_time)
              else
                result ->
                  waiter_pids = set_result_and_get_waiter_pids(cc, key, start_time, result)

                  Enum.map(waiter_pids, fn pid ->
                    send(pid, {self(), :completed})
                  end)

                  # get the now-cached value
                  do_get(cc, key, minimum_time)
              rescue
                error ->
                  waiter_pids = get_and_clear_waiter(cc, key, start_time)

                  Enum.map(waiter_pids, fn pid ->
                    send(pid, {self(), :failed})
                  end)

                  reraise error, System.stacktrace()
              end
            else
              Logger.debug(
                "Retrying do_get(#{inspect(key)}, #{inspect(minimum_time)}) " <>
                  "after failing to CAS new waiter"
              )

              do_get(cc, key, minimum_time)
            end
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

  defp set_result_and_get_waiter_pids(cc, key, start_time, result) do
    case :ets.lookup(cc.value_table, key) do
      [{^key, existing_start_time, _existing_value} = expected] ->
        if start_time > existing_start_time do
          # If this fails, it may have been CAS'ed by another process, but we
          # still need to retry because we might have an even-newer value.
          if compare_and_swap(cc.value_table, expected, {key, start_time, result}) do
            get_and_clear_waiter(cc, key, start_time)
          else
            Logger.debug(
              "Retrying set_result_and_get_waiter_pids after failing to CAS existing with " <>
                inspect({key, start_time, result})
            )

            set_result_and_get_waiter_pids(cc, key, start_time, result)
          end
        else
          get_and_clear_waiter(cc, key, start_time)
        end

      [] ->
        if compare_and_swap(cc.value_table, :nothing, {key, start_time, result}) do
          get_and_clear_waiter(cc, key, start_time)
        else
          Logger.debug(
            "Retrying set_result_and_get_waiter_pids after failing to CAS nothing with " <>
              inspect({key, start_time, result})
          )

          set_result_and_get_waiter_pids(cc, key, start_time, result)
        end
    end
  end

  defp get_and_clear_waiter(cc, key, start_time) do
    runner_pid = self()

    [{{^key, ^start_time}, {^runner_pid, waiter_pids}} = expected] =
      :ets.lookup(cc.waiter_table, {key, start_time})

    if compare_and_swap(cc.waiter_table, expected, :nothing) do
      waiter_pids
    else
      Logger.debug(
        "Retrying get_and_clear_waiter after failing to clear waiter for " <>
          inspect({key, start_time})
      )

      get_and_clear_waiter(cc, key, start_time)
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
