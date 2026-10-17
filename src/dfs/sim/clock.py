import heapq

from dfs.clock import Callback


class Timer:
    def __init__(self, due: int, callback: Callback) -> None:
        self._due = due
        self._callback = callback
        self._cancelled = False

    @property
    def due(self) -> int:
        return self._due

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True

    def fire(self) -> None:
        self._callback()


class SimClock:
    def __init__(self) -> None:
        self._now = 0
        self._sequence = 0
        self._queue: list[tuple[int, int, Timer]] = []
        self._advancing = False

    def now(self) -> int:
        return self._now

    def next_due(self) -> int | None:
        while self._queue and self._queue[0][2].cancelled:
            heapq.heappop(self._queue)
        return self._queue[0][0] if self._queue else None

    def schedule(self, delay: int, callback: Callback) -> Timer:
        if isinstance(delay, bool) or not isinstance(delay, int):
            raise TypeError("delay must be an integer number of milliseconds")
        if delay < 0:
            raise ValueError("delay must not be negative")
        timer = Timer(self._now + delay, callback)
        heapq.heappush(self._queue, (timer.due, self._sequence, timer))
        self._sequence += 1
        return timer

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
            while (due := self.next_due()) is not None and due <= deadline:
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
        while self._queue:
            due, _, timer = heapq.heappop(self._queue)
            if timer.cancelled:
                continue
            self._now = due
            timer.fire()
            return True
        return False
