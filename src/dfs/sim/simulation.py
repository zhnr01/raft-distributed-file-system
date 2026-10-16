from collections.abc import Callable
from random import Random

from dfs.invariants import InvariantChecker, InvariantViolated
from dfs.network import Message
from dfs.sim.clock import SimClock
from dfs.sim.network import NetworkConditions, SimNetwork
from dfs.state import ClusterState

ReceiveHandler = Callable[[Message], None]
Observer = Callable[[], ClusterState]


class Simulation:
    def __init__(self, seed: int, conditions: NetworkConditions | None = None) -> None:
        self._seed = seed
        self._random = Random(seed)
        self.clock = SimClock()
        self.network = SimNetwork(self.clock, self._random, conditions)
        self._handlers: dict[str, ReceiveHandler] = {}
        self._observer: Observer | None = None
        self._checker = InvariantChecker()
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

    def observe(self, observer: Observer) -> None:
        self._observer = observer

    def step(self) -> bool:
        fired = self.clock.step()
        if fired:
            self._check_invariants()
        return fired

    def run_until(self, deadline: int) -> None:
        if deadline < self.clock.now():
            raise ValueError("deadline must not precede the current time")
        while True:
            due = self.clock.next_due()
            if due is None or due > deadline:
                break
            self.step()
        self.clock.advance(deadline - self.clock.now())

    def _check_invariants(self) -> None:
        if self._observer is None:
            return
        for violation in self._checker.check(self._observer()):
            raise InvariantViolated(violation, seed=self._seed, time=self.clock.now())

    def _dispatch(self, recipient: str, message: Message) -> None:
        handler = self._handlers.get(recipient)
        if handler is not None:
            handler(message)
