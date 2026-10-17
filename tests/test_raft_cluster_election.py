import pytest
from conftest import Cluster, elect

from dfs.state import Role

SEEDS = 200


def test_a_three_node_cluster_elects_exactly_one_leader() -> None:
    cluster = Cluster(seed=1)

    cluster.run(ticks=200)

    assert len(cluster.leaders()) == 1


def test_a_three_node_cluster_elects_a_leader_on_every_seed() -> None:
    without_leader = [seed for seed in range(SEEDS) if not elect(seed).leaders]

    assert without_leader == []


def test_no_seed_elects_two_leaders_in_the_same_term() -> None:
    split = [
        outcome for outcome in (elect(seed) for seed in range(SEEDS)) if len(outcome.leaders) > 1
    ]

    assert split == []


def test_every_seed_settles_where_a_poll_cannot_see_churn() -> None:
    top_terms = [elect(seed).top_term for seed in range(SEEDS)]

    assert min(top_terms) >= 1
    assert max(top_terms) <= 3


def test_different_seeds_elect_different_leaders() -> None:
    elected = {
        outcome.leaders[0] for outcome in (elect(seed) for seed in range(SEEDS)) if outcome.leaders
    }

    assert len(elected) > 1


def test_the_run_is_reproducible_from_a_seed() -> None:
    first = elect(4417)
    second = elect(4417)

    assert first == second


def test_election_safety_holds_across_every_seed() -> None:
    checked = 0
    for seed in range(SEEDS):
        cluster = Cluster(seed)
        cluster.run(ticks=200)
        seen: dict[int, str] = {}
        for name in cluster.names:
            node = cluster.nodes[name]
            if node.role is not Role.LEADER:
                continue
            assert node.current_term not in seen, (
                f"seed {seed}: terms {node.current_term} has leaders {seen[node.current_term]} "
                f"and {name}"
            )
            seen[node.current_term] = name
        checked += 1

    assert checked == SEEDS


def test_a_five_node_cluster_also_elects_one_leader() -> None:
    failures = [seed for seed in range(20) if len(elect(seed, size=5).leaders) != 1]

    assert failures == []


@pytest.mark.parametrize("seed", [0, 1, 7, 4417, 199])
def test_a_sample_of_seeds_settles_on_one_leader(seed: int) -> None:
    outcome = elect(seed)

    assert len(outcome.leaders) == 1
