from collections.abc import Callable
from typing import Protocol

Callback = Callable[[], None]


class Timer(Protocol):
    @property
    def due(self) -> int: ...

    @property
    def cancelled(self) -> bool: ...

    def cancel(self) -> None: ...


class Clock(Protocol):
    def now(self) -> int: ...

    def schedule(self, delay: int, callback: Callback) -> Timer: ...
