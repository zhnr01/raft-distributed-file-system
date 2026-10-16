import heapq

from dfs.clock import Callback

ScheduledEvent = tuple[int, int, Callback]


class SimClock:
    def __init__(self) -> None:
        self._now = 0
        self._sequence = 0
        self._queue: list[ScheduledEvent] = []
        self._advancing = False

    def now(self) -> int:
        return self._now

    def next_due(self) -> int | None:
        return self._queue[0][0] if self._queue else None

    def schedule(self, delay: int, callback: Callback) -> None:
        if isinstance(delay, bool) or not isinstance(delay, int):
            raise TypeError("delay must be an integer number of milliseconds")
        if delay < 0:
            raise ValueError("delay must not be negative")
        heapq.heappush(self._queue, (self._now + delay, self._sequence, callback))
        self._sequence += 1

    def step(self) -> bool:
        self._reject_reentry("step")
        self._advancing = True
        try:
            return self._fire_next()
        finally:
            self._advancing = False

    def advance(self, duration: int) -> None:
        if isinstance(duration, bool) or not isinstance(duration, int):
            raise TypeError("duration must be an integer number of milliseconds")
        if duration < 0:
            raise ValueError("duration must not be negative")
        self._reject_reentry("advance")
        deadline = self._now + duration
        self._advancing = True
        try:
            while self._queue and self._queue[0][0] <= deadline:
                self._fire_next()
        finally:
            self._now = deadline
            self._advancing = False

    def _reject_reentry(self, operation: str) -> None:
        if self._advancing:
            raise RuntimeError(
                f"{operation} is already running; a callback must not drive the clock"
            )

    def _fire_next(self) -> bool:
        if not self._queue:
            return False
        due, _, callback = heapq.heappop(self._queue)
        self._now = due
        callback()
        return True
