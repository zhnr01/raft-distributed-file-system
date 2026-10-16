import pytest

from dfs.invariants import Invariant, InvariantViolated
from dfs.sim.simulation import Simulation
from dfs.state import ClusterState, LogEntry, NodeState, Role


def _leader_cluster(node_id: str, term: int) -> ClusterState:
    return ClusterState(
        nodes=(
            NodeState(
                node_id=node_id,
                current_term=term,
                role=Role.LEADER,
                log=(LogEntry(term=term, index=1, command=b""),),
                commit_index=0,
            ),
        )
    )


def _quiet_cluster() -> ClusterState:
    return ClusterState(
        nodes=(
            NodeState(
                node_id="n1",
                current_term=1,
                role=Role.FOLLOWER,
                log=(),
                commit_index=0,
            ),
        )
    )


def test_run_until_checks_invariants_after_every_step() -> None:
    simulation = Simulation(seed=1)
    checked_at: list[int] = []

    def observe() -> ClusterState:
        checked_at.append(simulation.clock.now())
        return _quiet_cluster()

    simulation.observe(observe)
    for delay in (10, 20, 30):
        simulation.clock.schedule(delay, lambda: None)
    simulation.run_until(100)

    assert checked_at == [10, 20, 30]


def test_a_violation_halts_the_run_at_the_offending_step() -> None:
    simulation = Simulation(seed=1)
    leaders = iter(("n1", "n2", "n3"))
    reached: list[int] = []

    simulation.observe(lambda: _leader_cluster(next(leaders), 4))
    for delay in (10, 20, 30):
        simulation.clock.schedule(delay, lambda: reached.append(simulation.clock.now()))

    with pytest.raises(InvariantViolated) as error:
        simulation.run_until(100)

    assert error.value.violation.invariant is Invariant.ELECTION_SAFETY
    assert error.value.seed == 1
    assert error.value.time == 20
    assert reached == [10, 20]


def test_a_run_without_an_observer_still_fires_events() -> None:
    simulation = Simulation(seed=1)
    fired: list[int] = []

    simulation.clock.schedule(10, lambda: fired.append(simulation.clock.now()))
    simulation.run_until(50)

    assert fired == [10]


def test_the_violation_message_names_the_seed_and_the_time() -> None:
    simulation = Simulation(seed=4417)
    leaders = iter(("n1", "n2"))

    simulation.observe(lambda: _leader_cluster(next(leaders), 2))
    simulation.clock.schedule(5, lambda: None)
    simulation.clock.schedule(15, lambda: None)

    with pytest.raises(InvariantViolated, match="seed 4417"):
        simulation.run_until(100)


def _entries(*terms: int) -> tuple[LogEntry, ...]:
    return tuple(
        LogEntry(term=term, index=index, command=b"") for index, term in enumerate(terms, start=1)
    )


def _node(node_id: str, term: int, role: Role, log: tuple[LogEntry, ...], commit: int) -> NodeState:
    return NodeState(
        node_id=node_id,
        current_term=term,
        role=role,
        log=log,
        commit_index=commit,
    )


ELECTION_SAFETY_HISTORY = (
    ClusterState(nodes=(_node("n1", 5, Role.LEADER, _entries(5), 0),)),
    ClusterState(nodes=(_node("n2", 5, Role.LEADER, _entries(5), 0),)),
)

LEADER_APPEND_ONLY_HISTORY = (
    ClusterState(nodes=(_node("n1", 5, Role.LEADER, _entries(1, 1, 5), 0),)),
    ClusterState(nodes=(_node("n1", 5, Role.LEADER, _entries(1, 1), 0),)),
)

LOG_MATCHING_HISTORY = (
    ClusterState(
        nodes=(
            _node("n1", 5, Role.FOLLOWER, _entries(1, 4, 5), 0),
            _node("n2", 5, Role.FOLLOWER, _entries(1, 2, 5), 0),
        )
    ),
)

LEADER_COMPLETENESS_HISTORY = (
    ClusterState(nodes=(_node("n1", 2, Role.LEADER, _entries(1, 2), 2),)),
    ClusterState(nodes=(_node("n2", 7, Role.LEADER, _entries(1), 1),)),
)


@pytest.mark.parametrize(
    ("invariant", "history"),
    [
        (Invariant.ELECTION_SAFETY, ELECTION_SAFETY_HISTORY),
        (Invariant.LEADER_APPEND_ONLY, LEADER_APPEND_ONLY_HISTORY),
        (Invariant.LOG_MATCHING, LOG_MATCHING_HISTORY),
        (Invariant.LEADER_COMPLETENESS, LEADER_COMPLETENESS_HISTORY),
    ],
)
def test_every_invariant_halts_a_run_through_the_harness(
    invariant: Invariant, history: tuple[ClusterState, ...]
) -> None:
    simulation = Simulation(seed=7)
    snapshots = iter(history)

    simulation.observe(lambda: next(snapshots))
    for delay in range(10, 10 * len(history) + 10, 10):
        simulation.clock.schedule(delay, lambda: None)

    with pytest.raises(InvariantViolated) as error:
        simulation.run_until(500)

    assert error.value.violation.invariant is invariant
    assert error.value.time == 10 * len(history)
