import pytest
from conftest import Cluster, build_node

from dfs.messages import AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse
from dfs.raft import RaftNode
from dfs.state import Role


def _become_leader(name: str = "n1", peers: tuple[str, ...] = ("n2", "n3")) -> RaftNode:
    node, clock, _ = build_node(name=name, peers=peers)
    clock.fire_every_live_timer()
    node.receive(RequestVoteResponse(term=node.current_term, voter_id=peers[0], vote_granted=True))
    return node


def _append(term: int, leader: str = "n2") -> AppendEntries:
    return AppendEntries(
        term=term,
        leader_id=leader,
        prev_log_index=0,
        prev_log_term=0,
        entries=(),
        leader_commit=0,
    )


def test_a_stale_term_vote_request_does_not_disturb_a_leader() -> None:
    node = _become_leader()
    term_before = node.current_term

    response = node.receive(
        RequestVote(term=term_before - 1, candidate_id="n2", last_log_index=0, last_log_term=0)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is False
    assert node.role is Role.LEADER
    assert node.current_term == term_before


def test_a_follower_that_has_not_voted_still_refuses_a_stale_term_request() -> None:
    node, _, _ = build_node()
    node.receive(_append(term=5))
    assert node.current_term == 5
    assert node.voted_for is None

    response = node.receive(
        RequestVote(term=3, candidate_id="n3", last_log_index=0, last_log_term=0)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is False
    assert node.voted_for is None, "a stale request must not consume this term's vote"
    assert node.current_term == 5


def test_a_stale_term_vote_request_does_not_cost_a_leader_its_vote() -> None:
    node = _become_leader()
    term = node.current_term

    node.receive(RequestVote(term=term - 1, candidate_id="n2", last_log_index=0, last_log_term=0))

    assert node.role is Role.LEADER


def test_a_higher_term_append_entries_demotes_a_leader() -> None:
    node = _become_leader()
    higher = node.current_term + 5

    response = node.receive(_append(term=higher))

    assert isinstance(response, AppendEntriesResponse)
    assert node.role is Role.FOLLOWER
    assert node.current_term == higher
    assert response.term == higher
    assert response.success is True


def test_a_higher_term_append_entries_makes_the_old_leader_accept_the_new_one() -> None:
    node = _become_leader()

    node.receive(_append(term=node.current_term + 1, leader="n3"))

    assert node.role is Role.FOLLOWER


def test_a_stale_term_append_entries_is_refused_by_a_leader() -> None:
    node = _become_leader()
    term_before = node.current_term

    response = node.receive(_append(term=term_before - 1))

    assert isinstance(response, AppendEntriesResponse)
    assert response.success is False
    assert node.role is Role.LEADER
    assert node.current_term == term_before


def test_a_heartbeat_keeps_a_follower_from_campaigning() -> None:
    cluster = Cluster(seed=3)
    cluster.run(ticks=60)
    leader = cluster.leaders()[0]
    term_after_settle = cluster.top_term()

    cluster.run(ticks=200)

    assert cluster.leaders() == (leader,)
    assert cluster.top_term() == term_after_settle


def test_a_heartbeat_resets_the_follower_election_timer() -> None:
    cluster = Cluster(seed=5)
    cluster.run(ticks=60)
    follower = next(name for name in cluster.names if name not in cluster.leaders())
    terms_before = cluster.nodes[follower].current_term

    cluster.run(ticks=400)

    assert cluster.nodes[follower].current_term == terms_before


def test_a_candidate_becomes_follower_on_a_higher_term_heartbeat() -> None:
    node = _become_leader()
    node._role = Role.CANDIDATE
    higher = node.current_term + 3

    node.receive(_append(term=higher, leader="n3"))

    assert node.role is Role.FOLLOWER
    assert node.current_term == higher


def test_quorum_is_strictly_more_than_half_the_cluster() -> None:
    three = _become_leader(peers=("n2", "n3"))
    five = _become_leader(peers=("n2", "n3", "n4", "n5"))
    seven = _become_leader(peers=("n2", "n3", "n4", "n5", "n6", "n7"))

    assert three._quorum() == 2
    assert five._quorum() == 3
    assert seven._quorum() == 4


def test_a_candidate_with_two_of_four_votes_does_not_win() -> None:
    node, clock, _ = build_node(peers=("n2", "n3", "n4"))
    clock.fire_every_live_timer()

    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))

    assert node.role is Role.CANDIDATE


def test_a_candidate_with_three_of_four_votes_wins() -> None:
    node, clock, _ = build_node(peers=("n2", "n3", "n4"))
    clock.fire_every_live_timer()

    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))
    node.receive(RequestVoteResponse(term=1, voter_id="n3", vote_granted=True))

    assert node.role is Role.LEADER


def test_a_duplicate_vote_from_one_peer_is_not_counted_twice() -> None:
    node, clock, _ = build_node(peers=("n2", "n3", "n4"))
    clock.fire_every_live_timer()

    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))
    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))

    assert node.role is Role.CANDIDATE


def test_a_vote_response_for_a_higher_term_makes_the_candidate_a_follower() -> None:
    node, clock, _ = build_node()
    clock.fire_every_live_timer()

    node.receive(RequestVoteResponse(term=99, voter_id="n2", vote_granted=True))

    assert node.current_term == 99
    assert node.role is Role.FOLLOWER


@pytest.mark.parametrize("role", [Role.CANDIDATE, Role.FOLLOWER])
def test_a_higher_term_append_entries_always_wins(role: Role) -> None:
    node = _become_leader()
    node._role = role

    node.receive(_append(term=node.current_term + 2, leader="n3"))

    assert node.role is Role.FOLLOWER
