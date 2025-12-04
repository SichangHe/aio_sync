from asyncio import Queue, QueueEmpty, QueueFull, QueueShutDown
from dataclasses import dataclass, field
from typing import AsyncGenerator

_ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED = None


@dataclass(slots=True)
class MPMCReceiver[T]:
    _queue: Queue[T] = field(init=True, repr=True)

    def capacity(self) -> int | None:
        """Queuing capacity, or `None` if unlimited."""
        maxsize = self._queue.maxsize
        _ = _ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED
        return maxsize if maxsize > 0 else None

    def is_empty(self) -> bool:
        """Whether the underlying queue is empty."""
        return self._queue.empty()

    def is_full(self) -> bool:
        """Whether the underlying queue is full."""
        return self._queue.full()

    def try_recv(self) -> T | None:
        """Try to receive a value from the channel.
        @return the value if exists, or else None."""
        try:
            return self._queue.get_nowait()
        except QueueEmpty:
            return None

    async def recv(self) -> T | QueueShutDown:
        """Wait to receive a value from the channel, or
        return `QueueShutDown` if the channel is shutdown immediately/and is
        empty."""
        try:
            return await self._queue.get()
        except QueueShutDown as err:
            return err

    async def drain(self) -> AsyncGenerator[T, None]:
        """Drain all available values from the channel until empty."""
        while (maybe_item := self.try_recv()) is not None:
            yield maybe_item

    async def recv_till_closed(self) -> AsyncGenerator[T, None]:
        """Receive all values from the channel until shut down."""
        while not isinstance(item := await self.recv(), QueueShutDown):
            yield item

    def shutdown(self, immediate: bool = False) -> None:
        """Shutdown the channel immediately.
        Further sends will return `QueueShutDown`.
        Pending and future receives will return `QueueShutDown` when
        the underlying queue is empty or if `immediate` is True."""
        self._queue.shutdown(immediate)

    def __len__(self) -> int:
        return self._queue.qsize()


@dataclass(slots=True)
class MPMCSender[T]:
    _queue: Queue[T] = field(init=True, repr=True)

    def capacity(self) -> int | None:
        """Queuing capacity, or `None` if unlimited."""
        maxsize = self._queue.maxsize
        _ = _ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED
        return maxsize if maxsize > 0 else None

    def is_empty(self) -> bool:
        """Whether the underlying queue is empty."""
        return self._queue.empty()

    def is_full(self) -> bool:
        """Whether the underlying queue is full."""
        return self._queue.full()

    def try_send(self, item: T) -> bool | QueueShutDown:
        """Try to send a value through the channel.
        @return True if sent, or False if full, or `QueueShutDown` if
        the channel is shut down."""
        try:
            self._queue.put_nowait(item)
            return True
        except QueueFull:
            return False
        except QueueShutDown as err:
            return err

    async def send(self, item: T) -> None | QueueShutDown:
        """Send a value through the channel, waiting if full, or
        return `QueueShutDown` if the channel is shutdown."""
        try:
            await self._queue.put(item)
        except QueueShutDown as err:
            return err

    def shutdown(self, immediate: bool = False) -> None:
        """Shutdown the channel immediately.
        Further sends will return `QueueShutDown`.
        Pending and future receives will return `QueueShutDown` when
        the underlying queue is empty or if `immediate` is True."""
        self._queue.shutdown(immediate)

    def __len__(self) -> int:
        return self._queue.qsize()


def mpmc_channel[T](
    capacity: int | None = None,
) -> tuple[MPMCSender[T], MPMCReceiver[T]]:
    """Multi-producer multi-consumer (MPMC) channel with the given capacity.
    @param capacity: the queuing capacity, or `None` for unlimited.
    @return: (sender, receiver) pair."""
    _ = _ASSUME_QUEUE_MAXSIZE_LE0MEANS_UNLIMITED
    maxsize = 0 if capacity is None else capacity
    _queue = Queue[T](maxsize)
    sender = MPMCSender(_queue)
    receiver = MPMCReceiver(_queue)
    return sender, receiver
