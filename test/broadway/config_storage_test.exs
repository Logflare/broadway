defmodule Broadway.ConfigStorageTest do
  use ExUnit.Case, async: false
  alias Broadway.ConfigStorage.Ets

  setup do
    prev = Application.get_env(Broadway, :config_storage)
    prev_opts = Application.get_env(Broadway, :config_storage_opts)

    on_exit(fn ->
      Application.put_env(Broadway, :config_storage, prev)
      Application.put_env(Broadway, :config_storage_opts, prev_opts)
    end)
  end

  test "ets default options" do
    Application.put_env(Broadway, :config_storage, :ets)
    Ets.setup()
    assert [] = Ets.list()
    assert Ets.put("some name", %Broadway.Topology{})
    assert ["some name"] = Ets.list()
    assert %Broadway.Topology{} = Ets.get("some name")
    assert :ets.info(Ets.default_table(), :size) == 1
    Ets.delete("some name")
    assert :ets.info(Ets.default_table(), :size) == 0
  end

  test "ets custom name" do
    Application.put_env(Broadway, :config_storage, :ets)
    Application.put_env(Broadway, :config_storage_opts, table_name: :my_table)
    Ets.setup()
    assert :ets.info(:my_table, :size) == 0
    assert [] = Ets.list()
    assert Ets.put("some name", %Broadway.Topology{})
    assert ["some name"] = Ets.list()
    assert %Broadway.Topology{} = Ets.get("some name")
    assert :ets.info(:my_table, :size) == 1
    Ets.delete("some name")
    assert :ets.info(:my_table, :size) == 0
  end
end
