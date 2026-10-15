from random import Random

import pytest

from dfs.network import Message
from dfs.sim.clock import SimClock
from dfs.sim.network import NetworkConditions, SimNetwork
from dfs.sim.simulation import Simulation


def _collect(network: SimNetwork, inbox: list[tuple[int, str, str]], clock: SimClock) -> None:
    def record(recipient: str, message: Message) -> None:
        inbox.append((clock.now(), recipient, str(message)))

    network.subscribe(record)


def test_message_arrives_after_the_configured_delay() -> None:
    clock = SimClock()
    conditions = NetworkConditions(min_delay=20, max_delay=20)
    network = SimNetwork(clock, Random(1), conditions)
    inbox: list[tuple[int, str, str]] = []
    _collect(network, inbox, clock)

    network.send("a", "b", "vote")

    clock.advance(19)
    assert inbox == []

    clock.advance(1)
    assert inbox == [(20, "b", "vote")]


def test_dropped_message_never_arrives() -> None:
    clock = SimClock()
    conditions = NetworkConditions(min_delay=10, max_delay=10, drop_probability=1.0)
    network = SimNetwork(clock, Random(1), conditions)
    inbox: list[tuple[int, str, str]] = []
    _collect(network, inbox, clock)

    network.send("a", "b", "vote")
    clock.advance(1000)

    assert inbox == []


def test_partition_blocks_both_directions_and_healing_restores_delivery() -> None:
    clock = SimClock()
    conditions = NetworkConditions(min_delay=10, max_delay=10)
    network = SimNetwork(clock, Random(1), conditions)
    inbox: list[tuple[int, str, str]] = []
    _collect(network, inbox, clock)

    network.partition({"a"}, {"b"})
    network.send("a", "b", "blocked")
    network.send("b", "a", "also blocked")
    clock.advance(100)
    assert inbox == []

    network.heal()
    network.send("a", "b", "delivered")
    clock.advance(100)
    assert [message for _, _, message in inbox] == ["delivered"]


def test_variable_delay_reorders_messages_sent_in_the_same_instant() -> None:
    clock = SimClock()
    conditions = NetworkConditions(min_delay=1, max_delay=60)
    network = SimNetwork(clock, Random(7), conditions)
    inbox: list[tuple[int, str, str]] = []
    _collect(network, inbox, clock)

    for index in range(8):
        network.send("a", "b", f"m{index}")
    clock.advance(1000)

    arrival_order = [message for _, _, message in inbox]
    assert len(arrival_order) == 8
    assert arrival_order != [f"m{index}" for index in range(8)]
    assert sorted(arrival_order) == sorted(f"m{index}" for index in range(8))


def test_counters_account_for_every_send() -> None:
    clock = SimClock()
    conditions = NetworkConditions(min_delay=1, max_delay=1, drop_probability=0.5)
    network = SimNetwork(clock, Random(5), conditions)

    for index in range(20):
        network.send("a", "b", f"m{index}")
    clock.advance(100)

    assert network.sent == 20
    assert network.delivered + network.dropped == network.sent
    assert network.dropped > 0
    assert network.delivered > 0


def test_a_message_to_an_unregistered_node_reaches_the_wire_then_is_discarded() -> None:
    simulation = Simulation(seed=1)

    simulation.network.send("a", "nobody", "lost")
    simulation.run_until(100)

    assert simulation.network.delivered == 1
    assert simulation.network.dropped == 0


def test_registering_the_same_node_twice_is_rejected() -> None:
    simulation = Simulation(seed=1)
    simulation.register("a", lambda message: None)

    with pytest.raises(ValueError, match="already registered"):
        simulation.register("a", lambda message: None)


def test_a_deadline_in_the_past_is_rejected() -> None:
    simulation = Simulation(seed=1)
    simulation.run_until(50)

    with pytest.raises(ValueError, match="must not precede"):
        simulation.run_until(49)


def test_impossible_conditions_are_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="min_delay must not be negative"):
        NetworkConditions(min_delay=-1)

    with pytest.raises(ValueError, match="max_delay must not be less than min_delay"):
        NetworkConditions(min_delay=10, max_delay=5)

    with pytest.raises(ValueError, match="drop_probability must be between"):
        NetworkConditions(drop_probability=1.5)


def test_a_partition_blocked_send_is_counted_as_dropped() -> None:
    clock = SimClock()
    network = SimNetwork(clock, Random(1), NetworkConditions(min_delay=1, max_delay=1))
    network.partition({"a"}, {"b"})

    network.send("a", "b", "blocked")
    clock.advance(100)

    assert network.sent == 1
    assert network.dropped == 1
    assert network.delivered == 0


def test_overlapping_partition_sides_are_rejected() -> None:
    network = SimNetwork(SimClock(), Random(1))

    with pytest.raises(ValueError, match="must be disjoint"):
        network.partition({"a", "b"}, {"b", "c"})


def test_same_instant_delivery_keeps_send_order() -> None:
    clock = SimClock()
    conditions = NetworkConditions(min_delay=0, max_delay=0)
    network = SimNetwork(clock, Random(1), conditions)
    inbox: list[tuple[int, str, str]] = []
    _collect(network, inbox, clock)

    for index in range(5):
        network.send("a", "b", f"m{index}")
    clock.advance(1)

    assert [message for _, _, message in inbox] == [f"m{index}" for index in range(5)]
