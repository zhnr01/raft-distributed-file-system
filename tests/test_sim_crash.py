from dfs.sim.network import NetworkConditions
from dfs.sim.simulation import Simulation


def _two_node_simulation(seed: int = 1) -> tuple[Simulation, list[tuple[str, str]]]:
    simulation = Simulation(seed=seed, conditions=NetworkConditions(min_delay=1, max_delay=1))
    received: list[tuple[str, str]] = []
    simulation.register("a", lambda message: received.append(("a", str(message))))
    simulation.register("b", lambda message: received.append(("b", str(message))))
    return simulation, received


def test_a_message_sent_to_a_crashed_node_is_not_delivered() -> None:
    simulation, received = _two_node_simulation()

    simulation.crash("b")
    simulation.network.send("a", "b", "hello")
    simulation.run_until(100)

    assert received == []


def test_a_message_in_flight_when_the_node_crashes_is_lost() -> None:
    simulation = Simulation(seed=1, conditions=NetworkConditions(min_delay=10, max_delay=10))
    received: list[str] = []
    simulation.register("b", lambda message: received.append(str(message)))

    simulation.network.send("a", "b", "in-flight")
    simulation.run_until(5)
    simulation.crash("b")
    simulation.run_until(100)

    assert received == []


def test_a_crashed_node_cannot_send() -> None:
    simulation, received = _two_node_simulation()

    simulation.crash("a")
    simulation.network.send("a", "b", "from-the-dead")
    simulation.run_until(100)

    assert received == []


def test_a_restarted_node_receives_messages_again() -> None:
    simulation, received = _two_node_simulation()

    simulation.crash("b")
    simulation.network.send("a", "b", "while-down")
    simulation.run_until(50)
    simulation.restart("b")
    simulation.network.send("a", "b", "after-up")
    simulation.run_until(100)

    assert received == [("b", "after-up")]


def test_crashing_an_unknown_node_is_rejected() -> None:
    simulation, _ = _two_node_simulation()

    try:
        simulation.crash("ghost")
    except KeyError as error:
        assert "ghost" in str(error)
    else:
        raise AssertionError("crashing an unregistered node must raise")


def test_crash_and_restart_are_idempotent() -> None:
    simulation, received = _two_node_simulation()

    simulation.crash("b")
    simulation.crash("b")
    simulation.restart("b")
    simulation.restart("b")
    simulation.network.send("a", "b", "still-works")
    simulation.run_until(100)

    assert received == [("b", "still-works")]


def test_a_crashed_node_is_reported_as_crashed() -> None:
    simulation, _ = _two_node_simulation()

    assert simulation.is_running("b") is True
    simulation.crash("b")
    assert simulation.is_running("b") is False
    simulation.restart("b")
    assert simulation.is_running("b") is True
