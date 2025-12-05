import asyncio
import pytest
from aiosync.oneshot import OneShot


def test_oneshot_send_and_try_recv():
    channel = OneShot[int]()
    assert channel.try_recv() is None
    channel.send(4)
    assert channel.try_recv() == 4
    with pytest.raises(ValueError):
        channel.send(5)


def test_oneshot_recv_waits_for_value():
    async def _run():
        channel = OneShot[int]()

        async def _send():
            await asyncio.sleep(0)
            channel.send(9)

        task = asyncio.create_task(_send())
        assert await channel.recv() == 9
        await task

    asyncio.run(_run())
