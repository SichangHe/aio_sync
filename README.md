# AioSync

Your ordinary synchronization structures for `asyncio`,
mainly wrappers around `asyncio`'s existing synchronization primitives but with
better type checking.

- `Mutex` that guard inner values.
- `OneShot` channel for one-time communication.
- Multi-producer multi-consumer (MPMC) channel `MPMC[T].channel`.
- TODO: Broadcast channel.
- TODO: Watch channel.
