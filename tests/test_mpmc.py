import asyncio
from asyncio import QueueShutDown
from aiosync.mpmc import mpmc_channel


def test_mpmc_capacity_and_empty_full_states():
    sender, receiver = mpmc_channel(None)
    assert sender.capacity() is None
    assert receiver.capacity() is None
    assert sender.is_empty()
    assert receiver.is_empty()
    assert not sender.is_full()
    assert not receiver.is_full()


def test_mpmc_try_send_and_try_recv_behavior():
    sender, receiver = mpmc_channel(1)
    assert sender.try_send(1) is True
    assert sender.try_send(2) is False
    assert receiver.try_recv() == 1
    assert receiver.try_recv() is None


def test_mpmc_send_and_recv_and_lengths():
    async def _run():
        sender, receiver = mpmc_channel(2)
        assert await sender.send(3) is None
        assert await sender.send(4) is None
        assert len(sender) == 2
        assert len(receiver) == 2
        assert await receiver.recv() == 3
        assert receiver.try_recv() == 4
        assert sender.is_empty()
        assert receiver.is_empty()

    asyncio.run(_run())


def test_mpmc_shutdown_paths():
    async def _run():
        sender, receiver = mpmc_channel(1)
        receiver.shutdown(immediate=True)
        send_result = await sender.send(5)
        assert isinstance(send_result, QueueShutDown)
        recv_result = await receiver.recv()
        assert isinstance(recv_result, QueueShutDown)

    asyncio.run(_run())


def test_mpmc_drain_and_recv_till_closed():
    async def _run():
        sender, receiver = mpmc_channel(None)
        for i in range(3):
            assert await sender.send(i) is None
        assert [item async for item in receiver.drain()] == [0, 1, 2]
        for i in range(2):
            assert await sender.send(i + 10) is None
        receiver.shutdown()
        assert [item async for item in receiver.recv_till_closed()] == [10, 11]

    asyncio.run(_run())
