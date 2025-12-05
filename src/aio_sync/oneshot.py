from asyncio import Event
from dataclasses import dataclass, field
from typing import cast

_ASSUME_ONE_SHOT_SENT_IS_TRUE_MEANS_ALREADY_SENT = None


@dataclass(slots=True)
class OneShot[T]:
    """A one-shot channel to send and receive 1 value."""

    _waker: Event = field(default_factory=Event, init=False, repr=True)
    _sent: bool = field(default=False, init=False, repr=True)
    _value: T | None = field(default=None, init=False, repr=True)

    def send(self, value: T):
        """Send a value through the one-shot channel, immediately.
        @raise ValueError if sending more than once.
        Examples:
            >>> channel = OneShot[int]()
            >>> channel.send(4)
            >>> channel.try_recv()
            4
        """
        if self._waker.is_set():
            raise ValueError(f"OneShot can only send once, {self}.")
        self._value = value
        _ = _ASSUME_ONE_SHOT_SENT_IS_TRUE_MEANS_ALREADY_SENT
        self._sent = True
        self._waker.set()

    def try_recv(self) -> T | None:
        """Try to receive the value sent through the one-shot channel.
        @return the value if already sent, or None if not yet sent.
        Examples:
            >>> channel = OneShot[str]()
            >>> channel.try_recv() is None
            True
            >>> channel.send(1)
            >>> channel.try_recv()
            1
        """
        if self._waker.is_set():
            _ = _ASSUME_ONE_SHOT_SENT_IS_TRUE_MEANS_ALREADY_SENT
            assert self._sent, (
                "OneShot `_sent` should've been set when waker is set.",
                self,
            )
            return self._value
        else:
            return

    async def recv(self) -> T:
        """Wait to receive the value sent through the one-shot channel.
        Examples:
            >>> import asyncio
            >>> async def main():
            ...     channel = OneShot[int]()
            ...     channel.send(9)
            ...     return await channel.recv()
            >>> asyncio.run(main())
            9
        """
        _ = await self._waker.wait()
        _ = _ASSUME_ONE_SHOT_SENT_IS_TRUE_MEANS_ALREADY_SENT
        assert self._sent, (
            "OneShot `_sent` should've been set when waker is set.",
            self,
        )
        return cast(T, self._value)
