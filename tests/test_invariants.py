from dfs.invariants import Invariant, InvariantChecker
from dfs.state import ClusterState, LogEntry, NodeState, Role


def test_election_safety_catches_two_leaders_in_one_term() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            NodeState(node_id="n1", current_term=5, role=Role.LEADER, log=(), commit_index=0),
            NodeState(node_id="n2", current_term=5, role=Role.LEADER, log=(), commit_index=0),
        )
    )

    violations = checker.check(cluster)

    assert [violation.invariant for violation in violations] == [Invariant.ELECTION_SAFETY]
    assert "n1" in violations[0].detail
    assert "n2" in violations[0].detail
    assert "5" in violations[0].detail


def test_election_safety_catches_two_leaders_that_never_coexist() -> None:
    checker = InvariantChecker()

    def snapshot(leader: str) -> ClusterState:
        return ClusterState(
            nodes=tuple(
                NodeState(
                    node_id=name,
                    current_term=5,
                    role=Role.LEADER if name == leader else Role.FOLLOWER,
                    log=(),
                    commit_index=0,
                )
                for name in ("n1", "n2")
            )
        )

    first = checker.check(snapshot("n1"))
    second = checker.check(snapshot("n2"))

    assert first == ()
    assert [violation.invariant for violation in second] == [Invariant.ELECTION_SAFETY]


def test_one_leader_per_term_over_many_terms_is_clean() -> None:
    checker = InvariantChecker()

    for term in range(1, 50):
        cluster = ClusterState(
            nodes=(
                NodeState(
                    node_id=f"n{term % 3}",
                    current_term=term,
                    role=Role.LEADER,
                    log=(),
                    commit_index=0,
                ),
            )
        )

        assert checker.check(cluster) == ()


def _leader(node_id: str, term: int, log: tuple[LogEntry, ...]) -> NodeState:
    return NodeState(
        node_id=node_id,
        current_term=term,
        role=Role.LEADER,
        log=log,
        commit_index=0,
    )


def _entries(*terms: int) -> tuple[LogEntry, ...]:
    return tuple(
        LogEntry(term=term, index=index, command=b"") for index, term in enumerate(terms, start=1)
    )


def test_leader_append_only_catches_a_leader_deleting_its_own_entry() -> None:
    checker = InvariantChecker()

    checker.check(ClusterState(nodes=(_leader("n1", 5, _entries(1, 1, 5)),)))
    violations = checker.check(ClusterState(nodes=(_leader("n1", 5, _entries(1, 1)),)))

    assert [violation.invariant for violation in violations] == [Invariant.LEADER_APPEND_ONLY]
    assert "n1" in violations[0].detail


def test_leader_append_only_catches_a_leader_overwriting_its_own_entry() -> None:
    checker = InvariantChecker()

    checker.check(ClusterState(nodes=(_leader("n1", 5, _entries(1, 1, 5)),)))
    violations = checker.check(ClusterState(nodes=(_leader("n1", 5, _entries(1, 4, 5)),)))

    assert [violation.invariant for violation in violations] == [Invariant.LEADER_APPEND_ONLY]


def test_leader_append_only_allows_a_leader_appending_entries() -> None:
    checker = InvariantChecker()

    checker.check(ClusterState(nodes=(_leader("n1", 5, _entries(1, 1)),)))
    violations = checker.check(ClusterState(nodes=(_leader("n1", 5, _entries(1, 1, 5, 5)),)))

    assert violations == ()


def test_leader_append_only_allows_truncation_while_the_node_is_a_follower() -> None:
    checker = InvariantChecker()
    peers = (_follower("n2", ()), _follower("n3", ()))

    checker.check(ClusterState(nodes=(_leader("n1", 5, _entries(1, 1, 5)), *peers)))
    checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n1",
                    current_term=9,
                    role=Role.FOLLOWER,
                    log=_entries(1, 1),
                    commit_index=0,
                ),
                *peers,
            )
        )
    )
    violations = checker.check(ClusterState(nodes=(_leader("n1", 9, _entries(1, 1, 9)), *peers)))

    assert violations == ()


def _follower(node_id: str, log: tuple[LogEntry, ...], commit_index: int = 0) -> NodeState:
    return NodeState(
        node_id=node_id,
        current_term=9,
        role=Role.FOLLOWER,
        log=log,
        commit_index=commit_index,
    )


def test_log_matching_catches_divergent_prefixes_under_a_matching_entry() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _follower("n1", _entries(1, 2, 3)),
            _follower("n2", _entries(1, 9, 3)),
        )
    )

    violations = checker.check(cluster)

    assert [violation.invariant for violation in violations] == [Invariant.LOG_MATCHING]
    assert "n1" in violations[0].detail
    assert "n2" in violations[0].detail


def test_log_matching_allows_logs_that_diverge_at_different_terms() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _follower("n1", _entries(1, 1, 5)),
            _follower("n2", _entries(1, 1, 6)),
        )
    )

    assert checker.check(cluster) == ()


def test_log_matching_allows_one_log_being_a_prefix_of_another() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _follower("n1", _entries(1, 1, 5, 5)),
            _follower("n2", _entries(1, 1)),
        )
    )

    assert checker.check(cluster) == ()


def test_log_matching_checks_every_pair_of_nodes() -> None:
    checker = InvariantChecker()
    cluster = ClusterState(
        nodes=(
            _follower("n1", _entries(1, 1, 7)),
            _follower("n2", _entries(1, 1, 8)),
            _follower("n3", _entries(1, 2, 7)),
        )
    )

    violations = checker.check(cluster)

    assert [violation.invariant for violation in violations] == [Invariant.LOG_MATCHING]
    assert "n1" in violations[0].detail
    assert "n3" in violations[0].detail


def test_leader_completeness_catches_a_later_leader_missing_a_committed_entry() -> None:
    checker = InvariantChecker()
    committed = ClusterState(
        nodes=(
            NodeState(
                node_id="n1",
                current_term=2,
                role=Role.LEADER,
                log=_entries(1, 2),
                commit_index=2,
            ),
            _follower("n2", _entries(1, 2), commit_index=2),
        )
    )

    assert checker.check(committed) == ()

    violations = checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n3",
                    current_term=7,
                    role=Role.LEADER,
                    log=_entries(1),
                    commit_index=1,
                ),
            )
        )
    )

    assert [violation.invariant for violation in violations] == [Invariant.LEADER_COMPLETENESS]
    assert "n3" in violations[0].detail
    assert "2" in violations[0].detail


def test_leader_completeness_does_not_accuse_a_leader_of_a_lower_term() -> None:
    checker = InvariantChecker()
    checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n1",
                    current_term=5,
                    role=Role.LEADER,
                    log=_entries(1, 2, 2),
                    commit_index=3,
                ),
            )
        )
    )

    violations = checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n2",
                    current_term=3,
                    role=Role.LEADER,
                    log=_entries(1, 2),
                    commit_index=2,
                ),
            )
        )
    )

    assert violations == ()


def test_leader_completeness_accepts_a_later_leader_that_holds_the_committed_entry() -> None:
    checker = InvariantChecker()
    checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n1",
                    current_term=2,
                    role=Role.LEADER,
                    log=_entries(1, 2),
                    commit_index=2,
                ),
            )
        )
    )

    violations = checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n3",
                    current_term=7,
                    role=Role.LEADER,
                    log=_entries(1, 2, 7),
                    commit_index=2,
                ),
            )
        )
    )

    assert violations == ()


def test_leader_completeness_catches_a_committed_entry_replaced_by_another_term() -> None:
    checker = InvariantChecker()
    checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n1",
                    current_term=4,
                    role=Role.LEADER,
                    log=_entries(1, 4),
                    commit_index=2,
                ),
            )
        )
    )

    violations = checker.check(
        ClusterState(
            nodes=(
                NodeState(
                    node_id="n2",
                    current_term=9,
                    role=Role.LEADER,
                    log=_entries(1, 8),
                    commit_index=2,
                ),
            )
        )
    )

    assert [violation.invariant for violation in violations] == [Invariant.LEADER_COMPLETENESS]
