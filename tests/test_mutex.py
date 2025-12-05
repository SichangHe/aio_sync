import asyncio
import pytest
from aiosync.mutex import Mutex


def test_mutex_lock_and_take():
    async def _run():
        mutex = Mutex(1)
        async with mutex.lock() as val:
            assert val == 1
        taken = await mutex.take()
        assert taken == 1
        with pytest.raises(ValueError):
            await mutex.take()

    asyncio.run(_run())


def test_mutex_lock4swap_updates_inner():
    async def _run():
        mutex = Mutex(2)
        async with mutex.lock4swap() as (val, swap):
            assert val == 2
            prior = swap(3)
            assert prior == 2
        async with mutex.lock() as final:
            assert final == 3

    asyncio.run(_run())
