defmodule UCacheTest do
  use ExUnit.Case

  @table __MODULE__
  @fun_lock :fun_lock
  @call_count :call_count

  setup do
    :ets.new(@table, [:public, :set, :named_table])
    :ets.insert(@table, {@fun_lock, false})
    :ets.insert(@table, {@call_count, 0})

    ExUnit.Callbacks.on_exit(fn -> UCache.clear(@table) end)

    :ok
  end

  test "get_or_run" do
    assert 10 == UCache.get_or_run(@table, :key, fn -> 10 end)
    assert 10 == UCache.get_or_run(@table, :key, fn -> 10 end)
    assert 1 == UCache.invalidate(@table, :key)
    assert 0 == UCache.invalidate(@table, :key)
    assert 10 == UCache.get_or_run(@table, :key, fn -> 10 end)
  end

  defp cache_single(wait_time, result) do
    case :ets.lookup(@table, @fun_lock) do
      [{@fun_lock, false}] ->
        :ets.insert(@table, {@fun_lock, true})
        Process.sleep(wait_time)
        :ets.insert(@table, {@fun_lock, false})
        result

      _ ->
        raise "not single"
    end
  end

  test "many processes call get_or_run, but one process only calls the caching function" do
    fun = fn ->
      assert 20 == UCache.get_or_run(@table, :key, fn -> cache_single(100, 20) end)
    end

    ps =
      for _ <- 1..1000, into: %{} do
        {pid, ref} = Process.spawn(fun, [:monitor])
        {pid, ref}
      end

    for _ <- 1..1000 do
      receive do
        {:DOWN, ref, :process, pid, reason} ->
          assert ps[pid] == ref
          assert reason == :normal
      end
    end
  end

  defp cache_raise(wait_time, result) do
    case :ets.update_counter(@table, @call_count, {2, 1}) do
      1 ->
        # first call is failed
        Process.sleep(wait_time)
        raise "failed"

      _ ->
        # other calls are passed
        Process.sleep(wait_time)
        result
    end
  end

  test "if caching process is crashed when waiting many processes, one of the processes call the caching function" do
    fun1 = fn ->
      assert_raise(RuntimeError, "failed", fn ->
        UCache.get_or_run(@table, :key, fn -> cache_raise(100, 20) end)
      end)
    end

    fun2 = fn ->
      assert 30 == UCache.get_or_run(@table, :key, fn -> cache_raise(100, 30) end)
    end

    ps =
      for n <- 1..1000, into: %{} do
        # raise an exception at first call
        if n == 1 do
          {pid, ref} = Process.spawn(fun1, [:monitor])
          Process.sleep(10)
          {pid, ref}
        else
          {pid, ref} = Process.spawn(fun2, [:monitor])
          {pid, ref}
        end
      end

    for _ <- 1..1000 do
      receive do
        {:DOWN, ref, :process, pid, reason} ->
          assert ps[pid] == ref
          assert reason == :normal
      end
    end
  end

  defp cache_with_call_count(key, wait_time) do
    Process.sleep(wait_time)

    case :ets.update_counter(@table, {@call_count, key}, {2, 1}, {{@call_count, key}, 0}) do
      1 ->
        # first call
        10

      2 ->
        # second call
        20

      3 ->
        # third call
        30
    end
  end
end
