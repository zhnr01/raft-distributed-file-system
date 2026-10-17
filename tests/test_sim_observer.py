import pytest

from dfs.sim.simulation import Simulation
from dfs.state import ClusterState, NodeState, Role


def _node(node_id: str, term: int = 1, role: Role = Role.FOLLOWER) -> NodeState:
    return NodeState(node_id=node_id, current_term=term, role=role, log=(), commit_index=0)


def _cluster(*node_ids: str) -> ClusterState:
    return ClusterState(nodes=tuple(_node(name) for name in node_ids))


def test_the_observer_must_report_every_registered_node() -> None:
    simulation = Simulation(seed=1)
    for name in ("n1", "n2", "n3"):
        simulation.register(name, lambda message: None)
    simulation.observe(lambda: _cluster("n1", "n2"))
    simulation.clock.schedule(10, lambda: None)

    with pytest.raises(ValueError, match="n3"):
        simulation.step()


def test_a_complete_observation_passes_the_coverage_check() -> None:
    simulation = Simulation(seed=1)
    for name in ("n1", "n2", "n3"):
        simulation.register(name, lambda message: None)
    checked: list[int] = []

    def observe() -> ClusterState:
        checked.append(simulation.clock.now())
        return _cluster("n1", "n2", "n3")

    simulation.observe(observe)
    simulation.clock.schedule(10, lambda: None)
    simulation.step()

    assert checked == [10]


def test_an_unknown_node_in_the_observation_is_rejected() -> None:
    simulation = Simulation(seed=1)
    simulation.register("n1", lambda message: None)
    simulation.observe(lambda: _cluster("n1", "ghost"))
    simulation.clock.schedule(10, lambda: None)

    with pytest.raises(ValueError, match="ghost"):
        simulation.step()


def test_coverage_is_checked_on_every_step_not_only_the_first() -> None:
    simulation = Simulation(seed=1)
    for name in ("n1", "n2"):
        simulation.register(name, lambda message: None)
    observations = iter((_cluster("n1", "n2"), _cluster("n1")))
    simulation.observe(lambda: next(observations))
    simulation.clock.schedule(10, lambda: None)
    simulation.clock.schedule(20, lambda: None)

    simulation.step()

    with pytest.raises(ValueError, match="n2"):
        simulation.step()


def test_an_observation_with_no_registered_nodes_is_rejected() -> None:
    simulation = Simulation(seed=1)
    simulation.register("n1", lambda message: None)
    simulation.observe(lambda: ClusterState(nodes=()))
    simulation.clock.schedule(10, lambda: None)

    with pytest.raises(ValueError, match="n1"):
        simulation.step()


def test_a_complete_observation_lets_the_invariants_run_clean() -> None:
    simulation = Simulation(seed=1)
    for name in ("n1", "n2", "n3"):
        simulation.register(name, lambda message: None)
    simulation.observe(
        lambda: ClusterState(
            nodes=(
                _node("n1", 5, Role.LEADER),
                _node("n2", 5, Role.FOLLOWER),
                _node("n3", 5, Role.FOLLOWER),
            )
        )
    )
    simulation.clock.schedule(10, lambda: None)

    assert simulation.step() is True
