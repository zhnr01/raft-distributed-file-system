from collections.abc import Callable
from dataclasses import dataclass, field
from random import Random

from dfs.network import Message
from dfs.sim.clock import SimClock

DeliveryHandler = Callable[[str, Message], None]


@dataclass(frozen=True)
class NetworkConditions:
    min_delay: int = 1
    max_delay: int = 1
    drop_probability: float = 0.0

    def __post_init__(self) -> None:
        if self.min_delay < 0:
            raise ValueError("min_delay must not be negative")
        if self.max_delay < self.min_delay:
            raise ValueError("max_delay must not be less than min_delay")
        if not 0.0 <= self.drop_probability <= 1.0:
            raise ValueError("drop_probability must be between 0.0 and 1.0")


@dataclass
class _Partition:
    left: frozenset[str] = field(default_factory=frozenset)
    right: frozenset[str] = field(default_factory=frozenset)

    def blocks(self, sender: str, recipient: str) -> bool:
        return (sender in self.left and recipient in self.right) or (
            sender in self.right and recipient in self.left
        )


class SimNetwork:
    def __init__(
        self,
        clock: SimClock,
        random: Random,
        conditions: NetworkConditions | None = None,
    ) -> None:
        self._clock = clock
        self._random = random
        self._conditions = conditions if conditions is not None else NetworkConditions()
        self._partition = _Partition()
        self._handlers: list[DeliveryHandler] = []
        self._sent = 0
        self._delivered = 0
        self._dropped = 0

    def subscribe(self, handler: DeliveryHandler) -> None:
        self._handlers.append(handler)

    def shares_randomness_with(self, random: Random) -> bool:
        return self._random is random

    def partition(self, left: set[str], right: set[str]) -> None:
        overlap = left & right
        if overlap:
            raise ValueError(f"partition sides must be disjoint; both contain {sorted(overlap)}")
        self._partition = _Partition(frozenset(left), frozenset(right))

    def heal(self) -> None:
        self._partition = _Partition()

    def send(self, sender: str, recipient: str, message: Message) -> None:
        self._sent += 1
        if self._partition.blocks(sender, recipient):
            self._dropped += 1
            return
        if self._random.random() < self._conditions.drop_probability:
            self._dropped += 1
            return
        delay = self._random.randint(self._conditions.min_delay, self._conditions.max_delay)
        self._clock.schedule(delay, lambda: self._deliver(recipient, message))

    @property
    def sent(self) -> int:
        return self._sent

    @property
    def delivered(self) -> int:
        return self._delivered

    @property
    def dropped(self) -> int:
        return self._dropped

    def _deliver(self, recipient: str, message: Message) -> None:
        self._delivered += 1
        for handler in self._handlers:
            handler(recipient, message)
