import pytest

from dfs.state import ClusterState, LogEntry, NodeState, Role


def _node(
    node_id: str = "n1",
    term: int = 1,
    role: Role = Role.FOLLOWER,
    log: tuple[LogEntry, ...] = (),
    commit_index: int = 0,
) -> NodeState:
    return NodeState(
        node_id=node_id,
        current_term=term,
        role=role,
        log=log,
        commit_index=commit_index,
    )


def _cluster(*nodes: NodeState) -> ClusterState:
    return ClusterState(nodes=nodes)


def test_a_well_formed_observation_is_accepted() -> None:
    cluster = _cluster(
        _node("n1", 3, Role.LEADER, (LogEntry(term=1, index=1, command=b"a"),), 1),
        _node("n2", 3, Role.FOLLOWER, (LogEntry(term=1, index=1, command=b"a"),), 1),
    )

    assert cluster.nodes[0].node_id == "n1"


def test_the_same_node_twice_is_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate node"):
        _cluster(_node("n1"), _node("n1"))


def test_a_negative_term_is_rejected() -> None:
    with pytest.raises(ValueError, match="current_term"):
        _node("n1", -1)


def test_term_zero_is_legal_because_the_paper_starts_at_zero() -> None:
    node = _node("n1", 0)

    assert node.current_term == 0


def test_a_commit_index_beyond_the_last_entry_is_rejected() -> None:
    log = (LogEntry(term=1, index=1, command=b""), LogEntry(term=1, index=2, command=b""))

    with pytest.raises(ValueError, match="commit_index"):
        _node("n1", 1, Role.LEADER, log, commit_index=3)


def test_a_commit_index_with_an_empty_log_is_rejected() -> None:
    with pytest.raises(ValueError, match="commit_index"):
        _node("n1", 1, Role.LEADER, (), commit_index=1)


def test_a_negative_commit_index_is_rejected() -> None:
    with pytest.raises(ValueError, match="commit_index"):
        _node("n1", 1, Role.LEADER, (), commit_index=-1)


def test_a_log_whose_indices_are_not_increasing_is_rejected() -> None:
    with pytest.raises(ValueError, match="not strictly increasing"):
        _node(
            "n1",
            1,
            Role.FOLLOWER,
            (
                LogEntry(term=1, index=2, command=b""),
                LogEntry(term=1, index=1, command=b""),
            ),
        )


def test_a_duplicated_index_is_rejected() -> None:
    with pytest.raises(ValueError, match="not strictly increasing"):
        _node(
            "n1",
            1,
            Role.FOLLOWER,
            (
                LogEntry(term=1, index=1, command=b"a"),
                LogEntry(term=1, index=1, command=b"b"),
            ),
        )


def test_a_gap_in_the_log_is_legal_because_compaction_will_produce_one() -> None:
    log = (LogEntry(term=1, index=5, command=b""), LogEntry(term=1, index=6, command=b""))

    node = _node("n1", 1, Role.FOLLOWER, log, commit_index=6)

    assert [entry.index for entry in node.log] == [5, 6]


def test_a_zero_origin_log_is_legal() -> None:
    log = (LogEntry(term=1, index=0, command=b""), LogEntry(term=1, index=1, command=b""))

    node = _node("n1", 1, Role.FOLLOWER, log, commit_index=1)

    assert [entry.index for entry in node.log] == [0, 1]


def test_an_entry_index_below_zero_is_rejected() -> None:
    with pytest.raises(ValueError, match="index"):
        LogEntry(term=1, index=-1, command=b"")


def test_an_empty_node_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="node_id"):
        _node("")


def test_the_error_names_the_offending_node() -> None:
    with pytest.raises(ValueError, match="n7"):
        _cluster(_node("n1"), _node("n7", -4))
