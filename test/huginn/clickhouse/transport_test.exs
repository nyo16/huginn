defmodule Huginn.Clickhouse.TransportTest do
  @moduledoc """
  Send-side flow control of the streaming calls, against a fake HTTP/2 server
  that stops consuming: it completes the handshake and reads every byte, but
  never grants flow-control window beyond the protocol's initial 65_535 bytes.
  """
  use ExUnit.Case, async: true

  alias Huginn.Clickhouse.Config

  # One pre-encoded element per message.
  @element :binary.copy("x", 10_000) <> "\n"
  @opts [chunk_size: 10_000, timeout: 500]

  setup do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    spawn_link(fn -> serve(listen) end)

    pool = :"transport_test_#{System.unique_integer([:positive])}"
    Config.register(Config.new(host: "127.0.0.1", port: port, pool_name: pool))

    on_exit(fn ->
      :persistent_term.erase({Config, pool})
      :gen_tcp.close(listen)
    end)

    {:ok, pool: pool}
  end

  test "insert_stream/3 pulls from the source only as fast as the server consumes",
       %{pool: pool} do
    assert {:error, %Huginn.Error{reason: :timeout}} =
             Huginn.insert_stream(
               "INSERT INTO t FORMAT TabSeparated",
               endless_source(),
               [pool: pool] ++ @opts
             )

    assert_bounded_pulls()
  end

  test "stream_io/3 pulls its input only as fast as the server consumes", %{pool: pool} do
    assert [{:error, %Huginn.Error{reason: :timeout}}] =
             "INSERT INTO t SELECT * FROM input('s String') FORMAT TabSeparated"
             |> Huginn.stream_io(endless_source(), [pool: pool] ++ @opts)
             |> Enum.to_list()

    assert_bounded_pulls()
  end

  defp endless_source do
    test = self()

    Stream.repeatedly(fn ->
      send(test, :pulled)
      @element
    end)
  end

  # 65_535 bytes of window fit six 10 KB messages; one more is held back and
  # one is being encoded. Without send-side flow control the endless source
  # would be read until the deadline (or memory) ran out.
  defp assert_bounded_pulls do
    pulled = count_pulls(0)
    assert pulled > 0
    assert pulled <= 10
  end

  defp count_pulls(count) do
    receive do
      :pulled -> count_pulls(count + 1)
    after
      0 -> count
    end
  end

  defp serve(listen) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        # Our SETTINGS (all defaults, so a 65_535-byte initial window), then the
        # ACK of the client's SETTINGS, which it sent with its preface.
        :ok = :gen_tcp.send(socket, settings_frame(0))
        :ok = :gen_tcp.send(socket, settings_frame(1))
        read_forever(socket)

      {:error, _closed} ->
        :ok
    end
  end

  defp read_forever(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, _data} -> read_forever(socket)
      {:error, _closed} -> :ok
    end
  end

  # length (24 bits), type 4 = SETTINGS, flags (1 = ACK), stream 0
  defp settings_frame(flags), do: <<0::24, 4::8, flags::8, 0::1, 0::31>>
end
