defmodule Broadway.ConfigStorage.ETS do
  @moduledoc false

  @behaviour Broadway.ConfigStorage

  @table __MODULE__

  # Used in tests.
  def table, do: @table

  @impl true
  def setup do
    if :undefined == :ets.whereis(@table) do
      :ets.new(@table, [:named_table, :public, :set, {:read_concurrency, true}])
    end

    :ok
  end

  @impl true
  def list do
    if :undefined != :ets.whereis(@table) do
      :ets.select(@table, [{{:"$1", :_}, [], [:"$1"]}])
    else
      []
    end
  end

  @impl true
  def get(server) do
    if :undefined != :ets.whereis(@table) do
      case :ets.match(@table, {server, :"$1"}) do
        [[topology]] -> topology
        _ -> nil
      end
    end
  end

  @impl true
  def put(server, topology) do
    if :undefined != :ets.whereis(@table) do
      :ets.insert(@table, {server, topology})
    else
      false
    end
  end

  @impl true
  def delete(server) do
    if :undefined != :ets.whereis(@table) do
      :ets.delete(@table, server)
    else
      false
    end
  end
end
