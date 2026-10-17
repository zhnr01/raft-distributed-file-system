from collections.abc import Callable
from dataclasses import dataclass

from dfs.clock import Callback, Timer
from dfs.network import Message
from dfs.raft import RaftNode
from dfs.sim.network import NetworkConditions
from dfs.sim.simulation import Simulation
from dfs.state import ClusterState, Role

PING_LIMIT = 6
ELECTION_TIMEOUT_MIN = 150
ELECTION_TIMEOUT_MAX = 300
HEARTBEAT_INTERVAL = 50
TICK = 10


class PingNode:
    def __init__(self, name: str, peer: str, simulation: Simulation) -> None:
        self.name = name
        self.peer = peer
        self.transcript: list[tuple[int, str]] = []
        self._simulation = simulation

    def receive(self, message: Message) -> None:
        text = str(message)
        self.transcript.append((self._simulation.clock.now(), text))
        count = int(text.split("-")[1])
        if count < PING_LIMIT:
            self._simulation.network.send(self.name, self.peer, f"ping-{count + 1}")

    def start(self) -> None:
        self._simulation.network.send(self.name, self.peer, "ping-0")


def run_scripted_exchange(seed: int) -> list[tuple[int, str]]:
    conditions = NetworkConditions(min_delay=1, max_delay=40, drop_probability=0.1)
    simulation = Simulation(seed=seed, conditions=conditions)
    first = PingNode("a", "b", simulation)
    second = PingNode("b", "a", simulation)
    simulation.register("a", first.receive)
    simulation.register("b", second.receive)

    first.start()
    simulation.run_until(2000)

    return first.transcript + second.transcript


class ManualTimer:
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
        if not self._cancelled:
            self._callback()


class ManualClock:
    def __init__(self) -> None:
        self._now = 0
        self.timers: list[ManualTimer] = []

    def now(self) -> int:
        return self._now

    def schedule(self, delay: int, callback: Callback) -> Timer:
        timer = ManualTimer(self._now + delay, callback)
        self.timers.append(timer)
        return timer

    def fire(self, timer: ManualTimer) -> None:
        self._now = timer.due
        timer.fire()

    def fire_latest(self) -> None:
        self.fire(self.timers[-1])

    def fire_every_live_timer(self) -> None:
        for timer in list(self.timers):
            if not timer.cancelled:
                self.fire(timer)


class SilentNetwork:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, object]] = []

    def send(self, sender: str, recipient: str, message: Message) -> None:
        self.sent.append((sender, recipient, message))

    def to(self, recipient: str) -> list[object]:
        return [message for _, target, message in self.sent if target == recipient]


def build_node(
    name: str = "n1",
    peers: tuple[str, ...] = ("n2", "n3"),
    clock: ManualClock | None = None,
    network: SilentNetwork | None = None,
) -> tuple[RaftNode, ManualClock, SilentNetwork]:
    resolved_clock = clock if clock is not None else ManualClock()
    resolved_network = network if network is not None else SilentNetwork()
    node = RaftNode(
        node_id=name,
        peers=peers,
        clock=resolved_clock,
        network=resolved_network,
        election_timeout_min=ELECTION_TIMEOUT_MIN,
        election_timeout_max=ELECTION_TIMEOUT_MAX,
        heartbeat_interval=HEARTBEAT_INTERVAL,
    )
    return node, resolved_clock, resolved_network


def _receive_ignoring_reply(node: RaftNode) -> Callable[[Message], None]:
    def receive(message: Message) -> None:
        node.receive(message)

    return receive


def role_of(node: RaftNode) -> Role:
    return node.role


@dataclass
class ElectionOutcome:
    seed: int
    leaders: tuple[str, ...]
    top_term: int


class Cluster:
    def __init__(self, seed: int, size: int = 3) -> None:
        self.simulation = Simulation(
            seed=seed, conditions=NetworkConditions(min_delay=1, max_delay=5)
        )
        self.names = tuple(f"n{index}" for index in range(1, size + 1))
        self.nodes: dict[str, RaftNode] = {}
        for name in self.names:
            node = RaftNode(
                node_id=name,
                peers=tuple(other for other in self.names if other != name),
                clock=self.simulation.clock,
                network=self.simulation.network,
                election_timeout_min=ELECTION_TIMEOUT_MIN,
                election_timeout_max=ELECTION_TIMEOUT_MAX,
                heartbeat_interval=HEARTBEAT_INTERVAL,
                random=self.simulation.random,
            )
            self.nodes[name] = node
            self.simulation.register(name, _receive_ignoring_reply(node))
        self.simulation.observe(self.snapshot)

    def snapshot(self) -> ClusterState:
        return ClusterState(nodes=tuple(self.nodes[name].state() for name in self.names))

    def run(self, ticks: int) -> None:
        for _ in range(ticks):
            self.simulation.run_until(self.simulation.clock.now() + TICK)

    def leaders(self) -> tuple[str, ...]:
        return tuple(name for name in self.names if self.nodes[name].role is Role.LEADER)

    def top_term(self) -> int:
        return max(node.current_term for node in self.nodes.values())


def elect(seed: int, size: int = 3, ticks: int = 200) -> ElectionOutcome:
    cluster = Cluster(seed, size)
    cluster.run(ticks)
    return ElectionOutcome(seed=seed, leaders=cluster.leaders(), top_term=cluster.top_term())
