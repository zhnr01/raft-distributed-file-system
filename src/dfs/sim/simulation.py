from collections.abc import Callable
from random import Random

from dfs.network import Message
from dfs.sim.clock import SimClock
from dfs.sim.network import NetworkConditions, SimNetwork

ReceiveHandler = Callable[[Message], None]


class Simulation:
    def __init__(self, seed: int, conditions: NetworkConditions | None = None) -> None:
        self._seed = seed
        self._random = Random(seed)
        self.clock = SimClock()
        self.network = SimNetwork(self.clock, self._random, conditions)
        self._handlers: dict[str, ReceiveHandler] = {}
        self.network.subscribe(self._dispatch)

    @property
    def seed(self) -> int:
        return self._seed

    @property
    def random(self) -> Random:
        return self._random

    def register(self, name: str, handler: ReceiveHandler) -> None:
        if name in self._handlers:
            raise ValueError(f"node {name} is already registered")
        self._handlers[name] = handler

    def run_until(self, deadline: int) -> None:
        if deadline < self.clock.now():
            raise ValueError("deadline must not precede the current time")
        self.clock.advance(deadline - self.clock.now())

    def _dispatch(self, recipient: str, message: Message) -> None:
        handler = self._handlers.get(recipient)
        if handler is not None:
            handler(message)
