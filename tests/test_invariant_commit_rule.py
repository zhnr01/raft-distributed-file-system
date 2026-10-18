from dfs.invariants import Invariant, InvariantChecker
from dfs.state import ClusterState, LogEntry, NodeState, Role


def _entries(*terms: int) -> tuple[LogEntry, ...]:
    return tuple(
        LogEntry(term=term, index=index, command=b"x") for index, term in enumerate(terms, start=1)
    )


def _node(
    node_id: str, term: int, role: Role, log: tuple[LogEntry, ...], commit_index: int = 0
) -> NodeState:
    return NodeState(
        node_id=node_id,
        current_term=term,
        role=role,
        log=log,
        commit_index=commit_index,
    )


def test_a_quorum_holding_a_previous_term_entry_commits_nothing() -> None:
    checker = InvariantChecker()
    leader = _node("n1", 2, Role.LEADER, _entries(1))
    quorum_holder = _node("n2", 1, Role.FOLLOWER, _entries(1))
    checker.check(ClusterState(nodes=(leader, quorum_holder, _node("n3", 1, Role.FOLLOWER, ()))))

    violations = checker.check(
        ClusterState(
            nodes=(
                _node("n1", 3, Role.FOLLOWER, ()),
                _node("n2", 3, Role.FOLLOWER, ()),
                _node("n3", 4, Role.LEADER, ()),
            )
        )
    )

    assert violations == ()


def test_a_quorum_held_by_the_entrys_own_term_leader_does_commit() -> None:
    checker = InvariantChecker()
    followers = (
        _node("n2", 1, Role.FOLLOWER, _entries(1)),
        _node("n3", 1, Role.FOLLOWER, ()),
    )
    checker.check(ClusterState(nodes=(_node("n1", 1, Role.LEADER, _entries(1)), *followers)))

    violations = checker.check(
        ClusterState(
            nodes=(
                _node("n1", 1, Role.FOLLOWER, ()),
                _node("n2", 1, Role.FOLLOWER, ()),
                _node("n3", 2, Role.LEADER, ()),
            )
        )
    )

    assert [violation.invariant for violation in violations] == [Invariant.LEADER_COMPLETENESS]


def test_one_node_claiming_a_commit_index_cannot_commit_on_its_own() -> None:
    checker = InvariantChecker()
    claimant = _node("n1", 1, Role.FOLLOWER, _entries(1), commit_index=1)
    empty = (
        _node("n2", 1, Role.FOLLOWER, ()),
        _node("n3", 1, Role.FOLLOWER, ()),
    )
    checker.check(ClusterState(nodes=(claimant, *empty)))

    violations = checker.check(
        ClusterState(
            nodes=(
                _node("n1", 3, Role.FOLLOWER, ()),
                _node("n2", 3, Role.FOLLOWER, ()),
                _node("n3", 3, Role.LEADER, ()),
            )
        )
    )

    assert violations == ()
