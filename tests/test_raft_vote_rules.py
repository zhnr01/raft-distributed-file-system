from conftest import build_node

from dfs.messages import RequestVote, RequestVoteResponse
from dfs.raft import RaftNode
from dfs.state import LogEntry


def _with_log(node: RaftNode, *terms: int) -> None:
    node._log = tuple(
        LogEntry(term=term, index=index, command=b"") for index, term in enumerate(terms, start=1)
    )


def test_a_voter_refuses_a_candidate_with_a_shorter_log() -> None:
    node, _, _ = build_node()
    _with_log(node, 1, 1, 1)

    response = node.receive(
        RequestVote(term=1, candidate_id="n2", last_log_index=2, last_log_term=1)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is False


def test_a_voter_refuses_a_candidate_with_a_stale_last_term() -> None:
    node, _, _ = build_node()
    _with_log(node, 1, 5)

    response = node.receive(
        RequestVote(term=9, candidate_id="n2", last_log_index=99, last_log_term=4)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is False


def test_a_voter_grants_to_an_equally_up_to_date_candidate() -> None:
    node, _, _ = build_node()
    _with_log(node, 1, 5)

    response = node.receive(
        RequestVote(term=9, candidate_id="n2", last_log_index=2, last_log_term=5)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is True


def test_a_voter_grants_to_a_longer_log_at_the_same_term() -> None:
    node, _, _ = build_node()
    _with_log(node, 1, 5)

    response = node.receive(
        RequestVote(term=9, candidate_id="n2", last_log_index=3, last_log_term=5)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is True


def test_a_voter_grants_to_a_higher_last_term_even_with_a_shorter_log() -> None:
    node, _, _ = build_node()
    _with_log(node, 1, 1, 1)

    response = node.receive(
        RequestVote(term=9, candidate_id="n2", last_log_index=1, last_log_term=6)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is True


def test_a_two_node_cluster_needs_only_one_peer_vote() -> None:
    node, clock, _ = build_node(peers=("n2",))
    clock.fire_every_live_timer()

    assert node._quorum() == 2
    assert node._votes == {"n1"}


def test_a_node_with_no_peers_is_rejected() -> None:
    try:
        build_node(peers=())
    except ValueError as error:
        assert "peer" in str(error)
    else:
        raise AssertionError("a node with no peers cannot form a cluster")


def test_an_inverted_timeout_range_is_rejected() -> None:
    from conftest import ManualClock, SilentNetwork

    try:
        RaftNode(
            node_id="n1",
            peers=("n2",),
            clock=ManualClock(),
            network=SilentNetwork(),
            election_timeout_min=300,
            election_timeout_max=150,
            heartbeat_interval=50,
        )
    except ValueError as error:
        assert "election_timeout_max" in str(error)
    else:
        raise AssertionError("an inverted timeout range must be rejected")


def test_a_heartbeat_interval_at_the_election_timeout_is_rejected() -> None:
    from conftest import ManualClock, SilentNetwork

    try:
        RaftNode(
            node_id="n1",
            peers=("n2",),
            clock=ManualClock(),
            network=SilentNetwork(),
            election_timeout_min=150,
            election_timeout_max=300,
            heartbeat_interval=150,
        )
    except ValueError as error:
        assert "heartbeat_interval" in str(error)
    else:
        raise AssertionError("a heartbeat as slow as the timeout cannot hold leadership")
