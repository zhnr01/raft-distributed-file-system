from collections.abc import Callable
from typing import Protocol

Callback = Callable[[], None]


class Clock(Protocol):
    def now(self) -> int: ...

    def schedule(self, delay: int, callback: Callback) -> None: ...
