from random import Random

from dfs.clock import Clock
from dfs.network import Network
from dfs.sim.clock import SimClock
from dfs.sim.network import SimNetwork
from dfs.sim.simulation import Simulation


def _needs_a_clock(clock: Clock) -> int:
    clock.schedule(10, lambda: None)
    return clock.now()


def _needs_a_network(network: Network) -> None:
    network.send("a", "b", "message")


def test_simclock_satisfies_the_clock_protocol() -> None:
    clock = SimClock()

    assert _needs_a_clock(clock) == 0


def test_simnetwork_satisfies_the_network_protocol() -> None:
    network = SimNetwork(SimClock(), Random(1))

    _needs_a_network(network)

    assert network.sent == 1


def test_a_node_can_depend_on_the_protocols_alone() -> None:
    simulation = Simulation(seed=1)
    received: list[object] = []

    def node(clock: Clock, network: Network) -> None:
        network.send("a", "b", f"sent-at-{clock.now()}")

    simulation.register("b", received.append)
    node(simulation.clock, simulation.network)
    simulation.run_until(100)

    assert received == ["sent-at-0"]
