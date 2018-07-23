defmodule UCacheTest do
  use ExUnit.Case

  test "get_or_run" do
    table = UCache.new()
    assert 10 == UCache.get_or_run(table, :key, fn -> 10 end)
    assert 10 == UCache.get_or_run(table, :key, fn -> 10 end)
    assert 1 == UCache.invalidate(table, :key)
    assert 0 == UCache.invalidate(table, :key)
    assert 10 == UCache.get_or_run(table, :key, fn -> 10 end)
  end

  test "if many processes call get_or_run for the same key, only one process calls the fallback function" do
    table = UCache.new()

    fun = fn ->
      assert 20 == UCache.get_or_run(table, :key, fn -> cache_single(table, 100, 20) end)
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

  defp cache_single(table, wait_time, result) do
    :ets.insert(table, {:fun_lock, false})
    :ets.insert(table, {:call_count, 0})

    case :ets.lookup(table, :fun_lock) do
      [{:fun_lock, false}] ->
        :ets.insert(table, {:fun_lock, true})
        Process.sleep(wait_time)
        :ets.insert(table, {:fun_lock, false})
        result

      _ ->
        raise "not single"
    end
  end

  test "if fallback function raises an error, it is reraised for the first caller for the key" do
    table = UCache.new()
    :ets.insert(table, {:call_count, 0})

    fun1 = fn ->
      assert_raise(RuntimeError, "failed", fn ->
        UCache.get_or_run(table, :key, fn -> cache_raise(table, 100, 20) end)
      end)
    end

    fun2 = fn ->
      assert 30 == UCache.get_or_run(table, :key, fn -> cache_raise(table, 100, 30) end)
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

  defp cache_raise(table, wait_time, result) do
    case :ets.update_counter(table, :call_count, {2, 1}) do
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
end
