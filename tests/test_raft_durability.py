from pathlib import Path

from conftest import (
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
    HEARTBEAT_INTERVAL,
    Cluster,
    ManualClock,
    MemoryStorage,
    OrderingStorage,
    SilentNetwork,
)

from dfs.messages import AppendEntries, RequestVote, RequestVoteResponse
from dfs.network import Message
from dfs.raft import RaftNode
from dfs.state import LogEntry, Role
from dfs.storage import FileStorage, PersistentState, Storage


class RecordingNetwork(SilentNetwork):
    def __init__(self, timeline: list[str]) -> None:
        super().__init__()
        self._timeline = timeline
        self.durable_at_send: list[PersistentState] = []
        self._storage: OrderingStorage | None = None

    def watch(self, storage: OrderingStorage) -> None:
        self._storage = storage

    def send(self, sender: str, recipient: str, message: Message) -> None:
        self._timeline.append("send")
        assert self._storage is not None
        self.durable_at_send.append(self._storage.load())
        super().send(sender, recipient, message)


def _request_vote(term: int, candidate_id: str) -> RequestVote:
    return RequestVote(
        term=term,
        candidate_id=candidate_id,
        last_log_index=0,
        last_log_term=0,
    )


def _granted_vote(node: RaftNode, term: int, candidate_id: str) -> bool:
    reply = node.receive(_request_vote(term=term, candidate_id=candidate_id))
    assert isinstance(reply, RequestVoteResponse)
    return reply.vote_granted


def _node(
    name: str,
    clock: ManualClock,
    network: SilentNetwork,
    storage: Storage,
) -> RaftNode:
    return RaftNode(
        node_id=name,
        peers=("n2", "n3"),
        clock=clock,
        network=network,
        storage=storage,
        election_timeout_min=ELECTION_TIMEOUT_MIN,
        election_timeout_max=ELECTION_TIMEOUT_MAX,
        heartbeat_interval=HEARTBEAT_INTERVAL,
    )


def test_a_vote_is_durable_before_the_reply_is_sent() -> None:
    timeline: list[str] = []
    storage = OrderingStorage(timeline)
    network = RecordingNetwork(timeline)
    network.watch(storage)
    node = _node("n1", ManualClock(), network, storage)

    node.receive(_request_vote(term=5, candidate_id="n2"))

    assert network.durable_at_send == [PersistentState(current_term=5, voted_for="n2", log=())]


def test_an_appended_entry_is_durable_before_the_reply_is_sent() -> None:
    timeline: list[str] = []
    storage = OrderingStorage(timeline)
    network = RecordingNetwork(timeline)
    network.watch(storage)
    node = _node("n1", ManualClock(), network, storage)
    entry = LogEntry(term=1, index=1, command=b"create /a")

    node.receive(
        AppendEntries(
            term=1,
            leader_id="n2",
            prev_log_index=0,
            prev_log_term=0,
            entries=(entry,),
            leader_commit=0,
        )
    )

    assert network.durable_at_send == [
        PersistentState(current_term=1, voted_for=None, log=(entry,))
    ]


def test_a_repeated_heartbeat_does_not_rewrite_the_state_file(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    storage = FileStorage(path)
    clock = ManualClock()
    network = SilentNetwork()
    node = _node("n1", clock, network, storage)
    heartbeat = AppendEntries(
        term=1,
        leader_id="n2",
        prev_log_index=0,
        prev_log_term=0,
        entries=(),
        leader_commit=0,
    )
    node.receive(heartbeat)
    after_first = path.read_bytes()

    first_save = path.stat().st_mtime_ns
    node.receive(heartbeat)
    node.receive(heartbeat)

    assert path.read_bytes() == after_first
    assert path.stat().st_mtime_ns == first_save


def test_a_repeated_heartbeat_is_not_written_again(tmp_path: Path) -> None:
    storage = MemoryStorage()
    node = _node("n1", ManualClock(), SilentNetwork(), storage)
    heartbeat = AppendEntries(
        term=1,
        leader_id="n2",
        prev_log_index=0,
        prev_log_term=0,
        entries=(),
        leader_commit=0,
    )

    node.receive(heartbeat)
    saved_once = storage.saves
    node.receive(heartbeat)
    node.receive(heartbeat)

    assert saved_once == 1
    assert storage.saves == 1


def test_a_restarted_node_keeps_its_term_and_its_log(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    clock = ManualClock()
    network = SilentNetwork()
    first = _node("n1", clock, network, FileStorage(path))
    first.receive(_request_vote(term=4, candidate_id="n2"))

    revived = _node("n1", clock, network, FileStorage(path))

    assert revived.current_term == 4
    assert revived.voted_for == "n2"
    assert revived.log == first.log


def test_a_restarted_node_does_not_vote_twice_in_the_same_term(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    clock = ManualClock()
    network = SilentNetwork()

    first = _node("n1", clock, network, FileStorage(path))
    assert _granted_vote(first, 5, "n2") is True
    del first

    revived = _node("n1", clock, network, FileStorage(path))

    assert _granted_vote(revived, 5, "n3") is False


def test_a_restarted_node_holds_its_committed_entries_before_any_repair() -> None:
    cluster = Cluster(seed=4417)
    cluster.run(200)
    leader_name = cluster.leader().node_id
    doomed = tuple(name for name in cluster.names if name != leader_name)[0]

    cluster.submit(b"create /committed")
    cluster.run(20)
    committed_index = cluster.nodes[leader_name].commit_index
    assert committed_index > 0, "the fixture failed to commit anything"
    expected = cluster.nodes[doomed].log[:committed_index]
    assert expected, "the fixture's follower held nothing"

    cluster.crash(doomed)
    cluster.restart(doomed)

    assert cluster.nodes[doomed].log[:committed_index] == expected


def test_a_node_killed_mid_replication_is_repaired_by_the_leader() -> None:
    cluster = Cluster(seed=4417)
    cluster.run(200)
    leader = cluster.leader()
    leader_name = leader.node_id
    followers = tuple(name for name in cluster.names if name != leader_name)
    doomed = followers[0]

    cluster.submit(b"create /committed")
    cluster.run(20)
    committed_index = cluster.nodes[leader_name].commit_index
    committed_entries = cluster.nodes[leader_name].log[:committed_index]
    assert committed_entries, "the fixture failed to commit anything"

    cluster.crash(doomed)
    cluster.submit(b"create /in-flight")
    cluster.run(5)
    cluster.restart(doomed)
    cluster.run(100)

    survived = cluster.nodes[doomed].log
    assert survived[: len(committed_entries)] == committed_entries


def test_a_restarted_follower_is_taught_the_commit_index_again() -> None:
    cluster = Cluster(seed=4417)
    cluster.run(200)
    leader_name = cluster.leader().node_id
    doomed = tuple(name for name in cluster.names if name != leader_name)[0]

    cluster.submit(b"create /committed")
    cluster.run(20)
    cluster.crash(doomed)
    cluster.restart(doomed)
    cluster.run(100)

    assert cluster.nodes[doomed].commit_index > 0


def test_a_term_learned_from_a_refused_leader_is_durable(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    clock = ManualClock()
    network = SilentNetwork()
    first = _node("n1", clock, network, FileStorage(path))
    first.receive(
        AppendEntries(
            term=6,
            leader_id="n2",
            prev_log_index=4,
            prev_log_term=5,
            entries=(),
            leader_commit=0,
        )
    )
    assert first.current_term == 6
    del first

    revived = _node("n1", clock, network, FileStorage(path))

    assert revived.current_term == 6


def test_a_candidates_own_vote_is_durable(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    clock = ManualClock()
    network = SilentNetwork()
    first = _node("n1", clock, network, FileStorage(path))
    clock.fire_every_live_timer()
    assert first.role is Role.CANDIDATE
    del first

    revived = _node("n1", clock, network, FileStorage(path))

    assert revived.current_term == 1
    assert revived.voted_for == "n1"


def test_a_restarted_leader_keeps_the_entries_it_accepted(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    clock = ManualClock()
    network = SilentNetwork()
    first = _node("n1", clock, network, FileStorage(path))
    clock.fire_every_live_timer()
    for peer in ("n2", "n3"):
        first.receive(
            RequestVoteResponse(term=first.current_term, voter_id=peer, vote_granted=True)
        )
    assert first.role is Role.LEADER
    accepted = first.submit(b"create /kept")
    del first

    revived = _node("n1", clock, network, FileStorage(path))

    assert [entry.index for entry in revived.log] == [accepted]
