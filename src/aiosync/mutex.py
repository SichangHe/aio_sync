from asyncio import Lock
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

_ASSUME_MUTEX_DISABLED_WHEN_INNER_IS_TAKEN = None


@dataclass(slots=True)
class Mutex[V]:
    """A mutual exclusion that yields the guarded value when used as
    a context manager.
    Useful to guarantee locking before accessing the guarded value.
    The caller is responsible to not use the yielded value outside!!"""

    _inner: V
    _lock: Lock = field(default_factory=Lock, init=False, repr=True)
    _disabled: bool = field(default=False, init=False, repr=True)

    @asynccontextmanager
    async def lock(self):
        """Context manager that locks and yields the guarded value.
        The caller is responsible to not use the yielded value outside!!"""
        _ = _ASSUME_MUTEX_DISABLED_WHEN_INNER_IS_TAKEN
        if self._disabled:
            raise ValueError(f"Mutex inner value is taken, thus disabled, {self=}.")
        async with self._lock:
            yield self._inner

    def _swap(self, new_value: V) -> V:
        """Set a new value and return the old one.
        Should only be called when locked."""
        old_value, self._inner = self._inner, new_value
        return old_value

    @asynccontextmanager
    async def lock4swap(self):
        """Context manager that locks and yields (the guarded value,
        a function that allows swapping in a new value)."""
        async with self._lock:
            yield self._inner, self._swap

    async def take(self) -> V:
        """Take out the inner value, marking this mutex disabled."""
        async with self.lock() as inner:
            _ = _ASSUME_MUTEX_DISABLED_WHEN_INNER_IS_TAKEN
            if self._disabled:
                raise ValueError(
                    f"Mutex inner value is already taken, and cannot be taken again, {self=}."
                )
            self._disabled = True
            return inner
