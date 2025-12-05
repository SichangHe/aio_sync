from asyncio import Queue, QueueEmpty, QueueFull, QueueShutDown
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import NamedTuple

_ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED = None


@dataclass(slots=True)
class MPMCReceiver[T]:
    _queue: Queue[T] = field(init=True, repr=True)

    def capacity(self) -> int | None:
        """Queuing capacity, or `None` if unlimited.
        Examples:
            >>> sender, receiver = mpmc_channel[int](2)
            >>> receiver.capacity()
            2
        """
        maxsize = self._queue.maxsize
        _ = _ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED
        return maxsize if maxsize > 0 else None

    def is_empty(self) -> bool:
        """Whether the underlying queue is empty.
        Examples:
            >>> sender, receiver = mpmc_channel[int](1)
            >>> receiver.is_empty()
            True
            >>> _ = sender.try_send(1)
            >>> receiver.is_empty()
            False
        """
        return self._queue.empty()

    def is_full(self) -> bool:
        """Whether the underlying queue is full.
        Examples:
            >>> sender, receiver = mpmc_channel[int](1)
            >>> receiver.is_full()
            False
            >>> _ = sender.try_send(1)
            >>> receiver.is_full()
            True
        """
        return self._queue.full()

    def try_recv(self) -> T | None:
        """Try to receive a value from the channel.
        @return the value if exists, or else None.
        Examples:
            >>> sender, receiver = mpmc_channel[int](1)
            >>> receiver.try_recv() is None
            True
            >>> _ = sender.try_send(3)
            >>> receiver.try_recv()
            3
        """
        try:
            return self._queue.get_nowait()
        except QueueEmpty:
            return None

    async def recv(self) -> T | QueueShutDown:
        """Wait to receive a value from the channel, or
        return `QueueShutDown` if the channel is shutdown immediately/and is
        empty.
        Examples:
            >>> import asyncio
            >>> async def main():
            ...     sender, receiver = mpmc_channel[int](1)
            ...     await sender.send(7)
            ...     return await receiver.recv()
            >>> asyncio.run(main())
            7
        """
        try:
            return await self._queue.get()
        except QueueShutDown as err:
            return err

    async def drain(self) -> AsyncGenerator[T, None]:
        """Drain all available values from the channel until empty.
        Examples:
            >>> import asyncio
            >>> async def main():
            ...     sender, receiver = mpmc_channel[int]()
            ...     for i in range(3):
            ...         await sender.send(i)
            ...     return [item async for item in receiver.drain()]
            >>> asyncio.run(main())
            [0, 1, 2]
        """
        while (maybe_item := self.try_recv()) is not None:
            yield maybe_item

    async def recv_till_closed(self) -> AsyncGenerator[T, None]:
        """Receive all values from the channel until shut down.
        Examples:
            >>> import asyncio
            >>> async def main():
            ...     sender, receiver = mpmc_channel[int]()
            ...     for i in range(2):
            ...         await sender.send(i)
            ...     receiver.shutdown(immediate=True)
            ...     return [item async for item in receiver.recv_till_closed()]
            >>> asyncio.run(main())
            [0, 1]
        """
        while not isinstance(item := await self.recv(), QueueShutDown):
            yield item

    def shutdown(self, immediate: bool = False) -> None:
        """Shutdown the channel immediately.
        Further sends will return `QueueShutDown`.
        Pending and future receives will return `QueueShutDown` when
        the underlying queue is empty or if `immediate` is True.
        Examples:
            >>> sender, receiver = mpmc_channel[int](1)
            >>> receiver.shutdown(immediate=True)
            >>> import asyncio
            >>> asyncio.run(receiver.recv())
            QueueShutDown
        """
        self._queue.shutdown(immediate)

    def __len__(self) -> int:
        return self._queue.qsize()


@dataclass(slots=True)
class MPMCSender[T]:
    _queue: Queue[T] = field(init=True, repr=True)

    def capacity(self) -> int | None:
        """Queuing capacity, or `None` if unlimited.
        Examples:
            >>> sender, _ = mpmc_channel[int](4)
            >>> sender.capacity()
            4
        """
        maxsize = self._queue.maxsize
        _ = _ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED
        return maxsize if maxsize > 0 else None

    def is_empty(self) -> bool:
        """Whether the underlying queue is empty.
        Examples:
            >>> sender, receiver = mpmc_channel[int](1)
            >>> sender.is_empty()
            True
            >>> _ = sender.try_send(1)
            >>> sender.is_empty()
            False
            >>> _ = receiver.try_recv()
            >>> sender.is_empty()
            True
        """
        return self._queue.empty()

    def is_full(self) -> bool:
        """Whether the underlying queue is full.
        Examples:
            >>> sender, _ = mpmc_channel[int](1)
            >>> sender.is_full()
            False
            >>> _ = sender.try_send(2)
            >>> sender.is_full()
            True
        """
        return self._queue.full()

    def try_send(self, item: T) -> bool | QueueShutDown:
        """Try to send a value through the channel.
        @return True if sent, or False if full, or `QueueShutDown` if
        the channel is shut down.
        Examples:
            >>> sender, receiver = mpmc_channel[int](1)
            >>> sender.try_send(10)
            True
            >>> sender.try_send(11)
            False
            >>> _ = receiver.try_recv()
            >>> sender.try_send(12)
            True
        """
        try:
            self._queue.put_nowait(item)
            return True
        except QueueFull:
            return False
        except QueueShutDown as err:
            return err

    async def send(self, item: T) -> None | QueueShutDown:
        """Send a value through the channel, waiting if full, or
        return `QueueShutDown` if the channel is shutdown.
        Examples:
            >>> import asyncio
            >>> async def main():
            ...     sender, receiver = mpmc_channel[int](1)
            ...     await sender.send(8)
            ...     return receiver.try_recv()
            >>> asyncio.run(main())
            8
        """
        try:
            await self._queue.put(item)
        except QueueShutDown as err:
            return err

    def shutdown(self, immediate: bool = False) -> None:
        """Shutdown the channel immediately.
        Further sends will return `QueueShutDown`.
        Pending and future receives will return `QueueShutDown` when
        the underlying queue is empty or if `immediate` is True.
        Examples:
            >>> sender, _ = mpmc_channel[int](1)
            >>> sender.shutdown(immediate=True)
            >>> sender.try_send(1)
            QueueShutDown
        """
        self._queue.shutdown(immediate)

    def __len__(self) -> int:
        return self._queue.qsize()


def mpmc_channel[T](
    capacity: int | None = None,
) -> tuple[MPMCSender[T], MPMCReceiver[T]]:
    """Multi-producer multi-consumer (MPMC) channel with the given capacity.
    Prefer using `MPMC[T].channel` instead.
    @param capacity: the queuing capacity, or `None` for unlimited.
    @return: (sender, receiver) pair.
    Examples:
        >>> sender, receiver = mpmc_channel(2)
        >>> sender.try_send(1)
        True
        >>> receiver.try_recv()
        1
    """
    _ = _ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED
    maxsize = 0 if capacity is None else capacity
    _queue = Queue[T](maxsize)
    sender = MPMCSender(_queue)
    receiver = MPMCReceiver(_queue)
    return sender, receiver


class MPMC[T](NamedTuple):
    """Multi-producer multi-consumer (MPMC) channel `NamedTuple` for
    convenience of type-parametrized construction over `mpmc_channel`.
    Examples:
        >>> sender, receiver = MPMC[int].channel(2)
        >>> sender.try_send(1)
        True
        >>> receiver.try_recv()
        1
    """

    sender: MPMCSender[T]
    receiver: MPMCReceiver[T]

    @classmethod
    def channel(
        cls, capacity: int | None = None
    ) -> tuple[MPMCSender[T], MPMCReceiver[T]]:
        """Multi-producer multi-consumer (MPMC) channel with
        the given capacity.
        @param capacity: the queuing capacity, or `None` for unlimited.
        @return: (sender, receiver) pair.
        Examples:
            >>> sender, receiver = MPMC[int].channel(2)
            >>> sender.try_send(1)
            True
            >>> receiver.try_recv()
            1
        """
        sender: MPMCSender[T]
        receiver: MPMCReceiver[T]
        sender, receiver = mpmc_channel(capacity)
        return cls(sender, receiver)
