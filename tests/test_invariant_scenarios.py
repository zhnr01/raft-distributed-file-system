import pytest

from dfs.invariants import Invariant, InvariantChecker, _entries_by_index, _entry_at
from dfs.state import ClusterState, LogEntry, NodeState, Role


def _entries(*terms: int) -> tuple[LogEntry, ...]:
    return tuple(
        LogEntry(term=term, index=index, command=b"x") for index, term in enumerate(terms, start=1)
    )


def _node(
    node_id: str,
    term: int,
    role: Role,
    log: tuple[LogEntry, ...],
    commit_index: int,
) -> NodeState:
    return NodeState(
        node_id=node_id,
        current_term=term,
        role=role,
        log=log,
        commit_index=commit_index,
    )


def test_a_healthy_partition_and_heal_sequence_raises_nothing() -> None:
    checker = InvariantChecker()
    history = [
        ClusterState(
            nodes=(
                _node("n1", 1, Role.LEADER, _entries(1, 1), 2),
                _node("n2", 1, Role.FOLLOWER, _entries(1, 1), 2),
                _node("n3", 1, Role.FOLLOWER, _entries(1, 1), 1),
            )
        ),
        ClusterState(
            nodes=(
                _node("n1", 1, Role.LEADER, _entries(1, 1, 1), 2),
                _node("n2", 2, Role.CANDIDATE, _entries(1, 1), 2),
                _node("n3", 2, Role.FOLLOWER, _entries(1, 1), 2),
            )
        ),
        ClusterState(
            nodes=(
                _node("n1", 1, Role.LEADER, _entries(1, 1, 1), 2),
                _node("n2", 2, Role.LEADER, _entries(1, 1), 2),
                _node("n3", 2, Role.FOLLOWER, _entries(1, 1), 2),
            )
        ),
        ClusterState(
            nodes=(
                _node("n1", 2, Role.FOLLOWER, _entries(1, 1), 2),
                _node("n2", 2, Role.LEADER, _entries(1, 1, 2), 2),
                _node("n3", 2, Role.FOLLOWER, _entries(1, 1, 2), 2),
            )
        ),
        ClusterState(
            nodes=(
                _node("n1", 2, Role.FOLLOWER, _entries(1, 1, 2), 3),
                _node("n2", 2, Role.LEADER, _entries(1, 1, 2), 3),
                _node("n3", 2, Role.FOLLOWER, _entries(1, 1, 2), 3),
            )
        ),
    ]

    violations = [violation for cluster in history for violation in checker.check(cluster)]

    assert violations == []


def test_a_stale_leader_overwriting_a_committed_entry_is_caught() -> None:
    checker = InvariantChecker()
    committed = ClusterState(
        nodes=(
            _node("n1", 2, Role.LEADER, _entries(1, 2), 2),
            _node("n2", 2, Role.FOLLOWER, _entries(1, 2), 2),
            _node("n3", 2, Role.FOLLOWER, _entries(1, 2), 2),
        )
    )

    assert checker.check(committed) == ()

    corrupted = ClusterState(
        nodes=(
            _node("n1", 5, Role.LEADER, _entries(1, 4), 2),
            _node("n2", 5, Role.FOLLOWER, _entries(1, 2), 2),
            _node("n3", 5, Role.FOLLOWER, _entries(1, 2), 2),
        )
    )

    found = [violation.invariant for violation in checker.check(corrupted)]

    assert found == [Invariant.LEADER_COMPLETENESS]


def test_log_matching_is_silent_when_logs_diverge_at_the_compared_index() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 5, Role.FOLLOWER, _entries(1, 4), 0),
            _node("n2", 5, Role.FOLLOWER, _entries(1, 2), 0),
        )
    )

    assert checker.check(cluster) == ()


def test_log_matching_fires_once_the_divergence_sits_below_an_agreeing_entry() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 5, Role.FOLLOWER, _entries(1, 4, 5), 0),
            _node("n2", 5, Role.FOLLOWER, _entries(1, 2, 5), 0),
        )
    )

    found = [violation.invariant for violation in checker.check(cluster)]

    assert found == [Invariant.LOG_MATCHING]


def test_a_candidate_is_not_treated_as_a_leader() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 3, Role.CANDIDATE, _entries(1), 0),
            _node("n2", 3, Role.CANDIDATE, _entries(1), 0),
            _node("n3", 3, Role.LEADER, _entries(1), 0),
        )
    )

    assert checker.check(cluster) == ()


def test_an_empty_cluster_snapshot_is_clean() -> None:
    checker = InvariantChecker()

    assert checker.check(ClusterState(nodes=())) == ()


def _offset_log(start: int, *terms: int) -> tuple[LogEntry, ...]:
    return tuple(
        LogEntry(term=term, index=start + offset, command=b"x") for offset, term in enumerate(terms)
    )


def test_log_matching_is_silent_when_logs_share_no_index() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 3, Role.FOLLOWER, _offset_log(1, 1, 1), 0),
            _node("n2", 3, Role.FOLLOWER, _offset_log(5, 1, 1), 0),
        )
    )

    assert checker.check(cluster) == ()


def test_log_matching_fires_on_an_offset_log_with_a_shared_matching_index() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 3, Role.FOLLOWER, _offset_log(1, 1, 4, 5), 0),
            _node("n2", 3, Role.FOLLOWER, _offset_log(1, 1, 2, 5), 0),
        )
    )

    found = [violation.invariant for violation in checker.check(cluster)]

    assert found == [Invariant.LOG_MATCHING]


def test_log_matching_ignores_positions_and_uses_the_entry_indices() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 3, Role.FOLLOWER, _offset_log(1, 1, 1), 0),
            _node("n2", 3, Role.FOLLOWER, _offset_log(2, 1, 1), 0),
        )
    )

    found = [violation.invariant for violation in checker.check(cluster)]

    assert found == [Invariant.LOG_MATCHING]


def test_log_matching_does_not_assume_a_one_based_origin() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 3, Role.FOLLOWER, _offset_log(0, 5, 9), 0),
            _node("n2", 3, Role.FOLLOWER, _offset_log(0, 3, 9), 0),
        )
    )

    found = [violation.invariant for violation in checker.check(cluster)]

    assert found == [Invariant.LOG_MATCHING]


def test_log_matching_is_silent_on_a_zero_origin_log_with_no_divergence() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _node("n1", 3, Role.FOLLOWER, _offset_log(0, 5, 9), 0),
            _node("n2", 3, Role.FOLLOWER, _offset_log(0, 5, 9), 0),
        )
    )

    assert checker.check(cluster) == ()


def test_both_entry_lookups_agree_on_which_entry_is_at_an_index() -> None:
    log = (
        LogEntry(term=1, index=1, command=b"first"),
        LogEntry(term=1, index=1, command=b"second"),
    )

    assert _entries_by_index(log)[1] is _entry_at(log, 1)
    assert _entries_by_index(log)[1].command == b"first"


def test_a_duplicated_index_is_rejected_at_the_boundary_rather_than_judged() -> None:
    n1 = (
        LogEntry(term=1, index=1, command=b"P"),
        LogEntry(term=1, index=1, command=b"Q"),
        LogEntry(term=9, index=2, command=b"Z"),
    )

    with pytest.raises(ValueError, match="not strictly increasing"):
        _node("n1", 2, Role.FOLLOWER, n1, 0)


def test_leader_completeness_looks_up_the_entry_by_index_not_by_position() -> None:
    checker = InvariantChecker()
    checker.check(ClusterState(nodes=(_node("n1", 2, Role.LEADER, _offset_log(1, 1), 1),)))

    violations = checker.check(
        ClusterState(nodes=(_node("n2", 5, Role.LEADER, _offset_log(5, 5, 5), 0),))
    )

    assert [violation.invariant for violation in violations] == [Invariant.LEADER_COMPLETENESS]
    assert "index 1" in violations[0].detail
