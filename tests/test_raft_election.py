from conftest import (
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
    build_node,
    role_of,
)

from dfs.messages import RequestVote, RequestVoteResponse
from dfs.state import Role


def test_a_new_node_starts_as_a_follower_with_term_zero() -> None:
    node, _, _ = build_node()

    assert node.role is Role.FOLLOWER
    assert node.current_term == 0
    assert node.voted_for is None


def test_an_election_timeout_makes_a_follower_a_candidate() -> None:
    node, clock, _ = build_node()

    clock.fire_every_live_timer()

    assert role_of(node) is Role.CANDIDATE
    assert node.current_term == 1
    assert node.voted_for == "n1"


def test_a_candidate_requests_votes_from_every_peer() -> None:
    node, clock, network = build_node()

    clock.fire_every_live_timer()

    assert network.to("n2")
    assert network.to("n3")
    request = network.to("n2")[0]
    assert isinstance(request, RequestVote)
    assert request.term == 1
    assert request.candidate_id == "n1"


def test_a_candidate_with_a_majority_of_votes_becomes_leader() -> None:
    node, clock, _ = build_node()
    clock.fire_every_live_timer()

    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))

    assert node.role is Role.LEADER


def test_a_candidate_without_a_majority_stays_a_candidate() -> None:
    node, clock, _ = build_node(peers=("n2", "n3", "n4", "n5"))
    clock.fire_every_live_timer()

    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))

    assert node.role is Role.CANDIDATE


def test_a_node_grants_its_first_vote_in_a_term() -> None:
    node, _, _ = build_node()

    first = node.receive(RequestVote(term=1, candidate_id="n2", last_log_index=0, last_log_term=0))

    assert isinstance(first, RequestVoteResponse)
    assert first.vote_granted is True


def test_a_second_candidate_in_the_same_term_is_refused() -> None:
    node, _, _ = build_node()
    node.receive(RequestVote(term=1, candidate_id="n2", last_log_index=0, last_log_term=0))

    second = node.receive(RequestVote(term=1, candidate_id="n3", last_log_index=0, last_log_term=0))

    assert isinstance(second, RequestVoteResponse)
    assert second.vote_granted is False


def test_a_vote_request_with_a_higher_term_makes_a_node_a_follower() -> None:
    node, clock, _ = build_node()
    clock.fire_every_live_timer()
    assert role_of(node) is Role.CANDIDATE

    node.receive(RequestVote(term=9, candidate_id="n2", last_log_index=0, last_log_term=0))

    assert node.role is Role.FOLLOWER
    assert node.current_term == 9
    assert node.voted_for == "n2"


def test_a_vote_request_with_a_stale_term_is_rejected() -> None:
    node, clock, _ = build_node()
    clock.fire_every_live_timer()
    term_before = node.current_term

    response = node.receive(
        RequestVote(term=1, candidate_id="n2", last_log_index=0, last_log_term=0)
    )

    assert isinstance(response, RequestVoteResponse)
    assert response.vote_granted is False
    assert node.current_term == term_before


def test_winning_an_election_sends_heartbeats_to_every_peer() -> None:
    node, clock, network = build_node()
    clock.fire_every_live_timer()
    network.sent.clear()

    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))

    assert network.to("n2")
    assert network.to("n3")


def test_a_leader_steps_down_when_it_sees_a_higher_term() -> None:
    node, clock, _ = build_node()
    clock.fire_every_live_timer()
    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))
    assert role_of(node) is Role.LEADER

    node.receive(RequestVote(term=7, candidate_id="n2", last_log_index=0, last_log_term=0))

    assert node.role is Role.FOLLOWER
    assert node.current_term == 7


def test_an_election_timeout_is_drawn_from_the_configured_range() -> None:
    _, clock, _ = build_node()

    delay = clock.timers[0].due
    assert ELECTION_TIMEOUT_MIN <= delay <= ELECTION_TIMEOUT_MAX


def test_two_nodes_draw_different_timeouts_so_votes_rarely_split() -> None:
    delays = {build_node(name=name)[1].timers[0].due for name in ("n1", "n2", "n3", "n4", "n5")}

    assert len(delays) > 1


def test_the_election_timer_can_be_cancelled() -> None:
    node, clock, _ = build_node()
    timer = clock.timers[0]

    timer.cancel()
    clock.fire(timer)

    assert node.role is Role.FOLLOWER
    assert node.current_term == 0


def test_a_candidate_whose_election_times_out_campaigns_again() -> None:
    node, clock, network = build_node()
    clock.fire_every_live_timer()
    assert role_of(node) is Role.CANDIDATE
    assert node.current_term == 1
    first_campaign = len(network.sent)

    clock.fire_latest()

    assert role_of(node) is Role.CANDIDATE
    assert node.current_term == 2
    assert node.voted_for == "n1"
    assert len(network.sent) > first_campaign


def test_a_renewed_candidate_forgets_the_previous_round_votes() -> None:
    node, clock, _ = build_node(peers=("n2", "n3", "n4"))
    clock.fire_every_live_timer()
    node.receive(RequestVoteResponse(term=1, voter_id="n2", vote_granted=True))
    assert node._votes == {"n1", "n2"}

    clock.fire_latest()

    assert node._votes == {"n1"}, "votes from the previous term must not carry over"
    assert role_of(node) is Role.CANDIDATE
