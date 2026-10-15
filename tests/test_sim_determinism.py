from dataclasses import dataclass
from random import Random

from dfs.network import Message
from dfs.sim.network import NetworkConditions
from dfs.sim.simulation import Simulation
from tests.test_sim_replay import run_scripted_exchange


def test_two_hundred_seeds_each_replay_identically() -> None:
    mismatched = [
        seed for seed in range(200) if run_scripted_exchange(seed) != run_scripted_exchange(seed)
    ]

    assert mismatched == []


def test_two_hundred_seeds_produce_many_distinct_runs() -> None:
    distinct = {tuple(run_scripted_exchange(seed)) for seed in range(200)}

    assert len(distinct) > 100


def test_partition_does_not_consume_randomness() -> None:
    conditions = NetworkConditions(min_delay=1, max_delay=40)

    unpartitioned = Simulation(seed=99, conditions=conditions)
    unpartitioned.network.send("a", "b", "probe")
    expected = unpartitioned.random.random()

    partitioned = Simulation(seed=99, conditions=conditions)
    partitioned.network.partition({"a"}, {"c"})
    partitioned.network.send("a", "c", "blocked")
    partitioned.network.send("a", "b", "probe")

    assert partitioned.random.random() == expected


def test_the_harness_owns_exactly_one_prng() -> None:
    simulation = Simulation(seed=7)

    assert simulation.network.shares_randomness_with(simulation.random)


def test_the_reported_seed_is_the_seed_that_drives_the_run() -> None:
    simulation = Simulation(seed=4417)

    assert simulation.seed == 4417
    assert simulation.random.random() == Random(4417).random()


def test_a_value_keyed_transcript_is_identical_across_runs() -> None:
    @dataclass(frozen=True)
    class Probe:
        index: int

    def run(seed: int) -> list[tuple[int, int]]:
        conditions = NetworkConditions(min_delay=1, max_delay=30, drop_probability=0.2)
        simulation = Simulation(seed=seed, conditions=conditions)
        seen: list[tuple[int, int]] = []

        def receive(message: Message) -> None:
            assert isinstance(message, Probe)
            seen.append((simulation.clock.now(), message.index))

        simulation.register("b", receive)
        for index in range(12):
            simulation.network.send("a", "b", Probe(index))
        simulation.run_until(500)
        return seen

    assert run(31) == run(31)
    assert run(31) != []
