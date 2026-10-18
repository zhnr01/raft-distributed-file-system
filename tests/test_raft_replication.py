import pytest
from conftest import Cluster, ManualClock, SilentNetwork, build_node

from dfs.messages import AppendEntries, AppendEntriesResponse, RequestVoteResponse
from dfs.raft import RaftNode
from dfs.state import LogEntry


def _become_leader(
    name: str = "n1", peers: tuple[str, ...] = ("n2", "n3")
) -> tuple[RaftNode, ManualClock, SilentNetwork]:
    node, clock, network = build_node(name=name, peers=peers)
    clock.fire_every_live_timer()
    for peer in peers:
        node.receive(RequestVoteResponse(term=node.current_term, voter_id=peer, vote_granted=True))
    return node, clock, network


def _entry(term: int, index: int, command: bytes = b"x") -> LogEntry:
    return LogEntry(term=term, index=index, command=command)


def _append(
    term: int = 1,
    leader: str = "n2",
    prev_log_index: int = 0,
    prev_log_term: int = 0,
    entries: tuple[LogEntry, ...] = (),
    leader_commit: int = 0,
) -> AppendEntries:
    return AppendEntries(
        term=term,
        leader_id=leader,
        prev_log_index=prev_log_index,
        prev_log_term=prev_log_term,
        entries=entries,
        leader_commit=leader_commit,
    )


def test_a_follower_stores_entries_whose_predecessor_matches_its_log() -> None:
    node, _, _ = build_node()

    response = node.receive(_append(entries=(_entry(1, 1, b"a"), _entry(1, 2, b"b"))))

    assert isinstance(response, AppendEntriesResponse)
    assert response.success is True
    assert node.log == (_entry(1, 1, b"a"), _entry(1, 2, b"b"))


def test_a_follower_refuses_an_append_whose_predecessor_term_differs() -> None:
    node, _, _ = build_node()
    node.receive(_append(entries=(_entry(1, 1, b"a"),)))
    accepted_before = node.log

    response = node.receive(
        _append(prev_log_index=1, prev_log_term=9, entries=(_entry(9, 2, b"z"),))
    )

    assert isinstance(response, AppendEntriesResponse)
    assert response.success is False
    assert node.log == accepted_before


def test_a_follower_refuses_an_append_that_starts_beyond_its_log() -> None:
    node, _, _ = build_node()

    response = node.receive(
        _append(prev_log_index=7, prev_log_term=1, entries=(_entry(1, 8, b"z"),))
    )

    assert isinstance(response, AppendEntriesResponse)
    assert response.success is False
    assert node.log == ()


def test_a_leader_appends_a_client_command_to_its_own_log_before_replicating() -> None:
    node, _, network = _become_leader()

    node.submit(b"create /x")

    assert node.log == (_entry(1, 1, b"create /x"),)
    assert node.commit_index == 0
    sent = [message for message in network.to("n2") if isinstance(message, AppendEntries)]
    assert sent[-1].entries == (_entry(1, 1, b"create /x"),)
    assert sent[-1].prev_log_index == 0
    assert sent[-1].prev_log_term == 0


def test_a_follower_only_refuses_commands_while_it_thinks_it_is_the_leader() -> None:
    node, _, _ = build_node()

    with pytest.raises(ValueError):
        node.submit(b"create /x")

    assert node.log == ()


def test_an_entry_is_not_committed_until_a_quorum_acknowledges_it() -> None:
    node, _, _ = _become_leader(peers=("n2", "n3", "n4"))

    index = node.submit(b"create /x")

    assert node.commit_index == 0
    node.receive(
        AppendEntriesResponse(
            term=node.current_term,
            voter_id="n2",
            success=True,
            conflict_index=0,
            acknowledged_index=index,
        )
    )
    assert node.commit_index == 0
    node.receive(
        AppendEntriesResponse(
            term=node.current_term,
            voter_id="n3",
            success=True,
            conflict_index=0,
            acknowledged_index=index,
        )
    )
    assert node.commit_index == index


def test_a_duplicate_acknowledgement_from_one_peer_cannot_commit_alone() -> None:
    node, _, _ = _become_leader(peers=("n2", "n3", "n4"))

    index = node.submit(b"create /x")
    for _ in range(3):
        node.receive(
            AppendEntriesResponse(
                term=node.current_term,
                voter_id="n2",
                success=True,
                conflict_index=0,
                acknowledged_index=index,
            )
        )

    assert node.commit_index == 0


def test_an_entry_from_a_previous_term_is_not_committed_by_counting_replicas() -> None:
    node, _, _ = _become_leader(peers=("n2", "n3", "n4"))
    node._current_term = 4
    node._log = (LogEntry(term=2, index=1, command=b"old"),)

    node.receive(
        AppendEntriesResponse(
            term=4, voter_id="n2", success=True, conflict_index=0, acknowledged_index=1
        )
    )
    node.receive(
        AppendEntriesResponse(
            term=4, voter_id="n3", success=True, conflict_index=0, acknowledged_index=1
        )
    )

    assert node.commit_index == 0


def test_committing_a_current_term_entry_carries_earlier_terms_with_it() -> None:
    node, _, _ = _become_leader(peers=("n2", "n3", "n4"))
    node._current_term = 4
    node._log = (
        LogEntry(term=2, index=1, command=b"old"),
        LogEntry(term=4, index=2, command=b"new"),
    )

    node.receive(
        AppendEntriesResponse(
            term=4, voter_id="n2", success=True, conflict_index=0, acknowledged_index=2
        )
    )
    node.receive(
        AppendEntriesResponse(
            term=4, voter_id="n3", success=True, conflict_index=0, acknowledged_index=2
        )
    )

    assert node.commit_index == 2


def test_a_rejected_append_moves_next_index_back_and_retries() -> None:
    node, _, network = _become_leader()
    node._log = (
        LogEntry(term=1, index=1, command=b"a"),
        LogEntry(term=1, index=2, command=b"b"),
    )
    node._next_index["n2"] = 3
    sent_before = len(network.to("n2"))

    node.receive(
        AppendEntriesResponse(
            term=1, voter_id="n2", success=False, conflict_index=2, acknowledged_index=0
        )
    )

    assert node.next_index["n2"] == 2
    retried = network.to("n2")[sent_before:]
    assert len(retried) == 1
    assert isinstance(retried[0], AppendEntries)
    assert retried[0].prev_log_index == 1
    assert retried[0].entries == (LogEntry(term=1, index=2, command=b"b"),)


def test_a_follower_with_a_divergent_tail_is_corrected_not_truncated_blindly() -> None:
    node, _, _ = build_node()
    node.receive(_append(entries=(_entry(1, 1, b"a"), _entry(1, 2, b"b"))))

    node.receive(_append(term=2, prev_log_index=1, prev_log_term=1, entries=(_entry(2, 2, b"B"),)))

    assert node.log == (_entry(1, 1, b"a"), _entry(2, 2, b"B"))


def test_a_follower_learns_the_commit_index_from_the_leader() -> None:
    node, _, _ = build_node()
    node.receive(_append(entries=(_entry(1, 1, b"a"), _entry(1, 2, b"b"))))

    node.receive(_append(entries=(), leader_commit=2))

    assert node.commit_index == 2


def test_a_follower_does_not_trust_a_commit_index_beyond_its_own_log() -> None:
    node, _, _ = build_node()
    node.receive(_append(entries=(_entry(1, 1, b"a"),)))

    node.receive(_append(entries=(), leader_commit=9))

    assert node.commit_index == 1


def test_a_follower_never_moves_its_commit_index_backwards() -> None:
    node, _, _ = build_node()
    node.receive(_append(entries=(_entry(1, 1, b"a"), _entry(1, 2, b"b"))))
    node.receive(_append(entries=(), leader_commit=2))

    node.receive(_append(entries=(), leader_commit=1))

    assert node.commit_index == 2


def test_a_real_cluster_replicates_and_commits_a_command() -> None:
    cluster = Cluster(seed=1)
    cluster.run(ticks=200)
    leader = cluster.leader()

    index = cluster.submit(b"create /x")
    cluster.run(ticks=200)

    assert leader.commit_index == index
    for name in cluster.names:
        assert cluster.nodes[name].log == (
            LogEntry(term=leader.current_term, index=1, command=b"create /x"),
        )


def test_a_real_cluster_replicates_every_command_in_order() -> None:
    cluster = Cluster(seed=3)
    cluster.run(ticks=200)
    leader = cluster.leader()

    for step in range(3):
        cluster.submit(f"create /f{step}".encode())
        cluster.run(ticks=100)

    expected = tuple(
        LogEntry(term=leader.current_term, index=step + 1, command=f"create /f{step}".encode())
        for step in range(3)
    )
    assert leader.log == expected
    for name in cluster.names:
        assert cluster.nodes[name].log == expected
