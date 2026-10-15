from collections.abc import Callable

from dfs.network import Message
from dfs.sim.network import NetworkConditions
from dfs.sim.simulation import Simulation

NODES = ("n1", "n2", "n3")


def _broadcast_cluster(seed: int) -> tuple[Simulation, dict[str, list[str]]]:
    conditions = NetworkConditions(min_delay=1, max_delay=5)
    simulation = Simulation(seed=seed, conditions=conditions)
    inboxes: dict[str, list[str]] = {name: [] for name in NODES}

    def receiver(name: str) -> Callable[[Message], None]:
        def receive(message: Message) -> None:
            inboxes[name].append(str(message))

        return receive

    for name in NODES:
        simulation.register(name, receiver(name))
    return simulation, inboxes


def _broadcast(simulation: Simulation, sender: str, message: str) -> None:
    for recipient in NODES:
        if recipient != sender:
            simulation.network.send(sender, recipient, message)


def test_a_majority_side_keeps_talking_while_the_minority_is_cut_off() -> None:
    simulation, inboxes = _broadcast_cluster(seed=11)
    simulation.network.partition({"n1", "n2"}, {"n3"})

    _broadcast(simulation, "n1", "from-n1")
    simulation.run_until(100)

    assert inboxes["n2"] == ["from-n1"]
    assert inboxes["n3"] == []

    _broadcast(simulation, "n3", "from-n3")
    simulation.run_until(200)

    assert inboxes["n1"] == []
    assert inboxes["n2"] == ["from-n1"]


def test_healing_a_majority_partition_restores_every_link() -> None:
    simulation, inboxes = _broadcast_cluster(seed=11)
    simulation.network.partition({"n1", "n2"}, {"n3"})
    _broadcast(simulation, "n1", "cut")
    simulation.run_until(100)

    assert inboxes["n3"] == []

    simulation.network.heal()
    _broadcast(simulation, "n1", "healed")
    simulation.run_until(200)

    assert inboxes["n3"] == ["healed"]
    assert inboxes["n2"] == ["cut", "healed"]


def test_a_node_is_never_cut_off_from_itself_by_a_partition() -> None:
    simulation, _ = _broadcast_cluster(seed=11)
    simulation.network.partition({"n1", "n2"}, {"n3"})

    simulation.network.send("n1", "n1", "self")
    simulation.run_until(100)

    assert simulation.network.delivered == 1


def test_a_partitioned_run_is_still_reproducible_from_its_seed() -> None:
    def run(seed: int) -> dict[str, list[str]]:
        simulation, inboxes = _broadcast_cluster(seed)
        _broadcast(simulation, "n1", "before")
        simulation.run_until(50)
        simulation.network.partition({"n1", "n2"}, {"n3"})
        _broadcast(simulation, "n2", "during")
        simulation.run_until(150)
        simulation.network.heal()
        _broadcast(simulation, "n3", "after")
        simulation.run_until(300)
        return inboxes

    assert run(777) == run(777)

    inboxes = run(777)
    assert inboxes["n3"] == ["before"]
    assert inboxes["n1"] == ["during", "after"]
    assert inboxes["n2"] == ["before", "after"]
