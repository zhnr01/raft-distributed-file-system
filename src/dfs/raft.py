from random import Random

from dfs.clock import Clock, Timer
from dfs.messages import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from dfs.network import Message, Network
from dfs.state import LogEntry, NodeState, Role

Reply = RequestVoteResponse | AppendEntriesResponse


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: tuple[str, ...],
        clock: Clock,
        network: Network,
        election_timeout_min: int,
        election_timeout_max: int,
        heartbeat_interval: int,
        random: Random | None = None,
    ) -> None:
        if not peers:
            raise ValueError("a node needs at least one peer")
        if election_timeout_min <= 0:
            raise ValueError("election_timeout_min must be positive")
        if election_timeout_max < election_timeout_min:
            raise ValueError("election_timeout_max must not be less than the minimum")
        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        if heartbeat_interval >= election_timeout_min:
            raise ValueError(
                "heartbeat_interval must be well below the election timeout, "
                "or a live leader cannot stop followers from campaigning"
            )
        self._node_id = node_id
        self._peers = peers
        self._clock = clock
        self._network = network
        self._timeout_min = election_timeout_min
        self._timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval
        self._random = random if random is not None else Random(node_id)
        self._current_term = 0
        self._voted_for: str | None = None
        self._role = Role.FOLLOWER
        self._log: tuple[LogEntry, ...] = ()
        self._commit_index = 0
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        self._votes: set[str] = set()
        self._election_timer: Timer | None = None
        self._heartbeat_timer: Timer | None = None
        self._start_election_timer()

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def role(self) -> Role:
        return self._role

    @property
    def current_term(self) -> int:
        return self._current_term

    @property
    def voted_for(self) -> str | None:
        return self._voted_for

    @property
    def log(self) -> tuple[LogEntry, ...]:
        return self._log

    @property
    def commit_index(self) -> int:
        return self._commit_index

    @property
    def next_index(self) -> dict[str, int]:
        return dict(self._next_index)

    @property
    def match_index(self) -> dict[str, int]:
        return dict(self._match_index)

    def submit(self, command: bytes) -> int:
        if self._role is not Role.LEADER:
            raise ValueError(f"node {self._node_id} cannot accept commands as {self._role.value}")
        entry = LogEntry(term=self._current_term, index=self._last_log_index() + 1, command=command)
        self._log = (*self._log, entry)
        self._replicate()
        return entry.index

    def _replicate(self) -> None:
        for peer in self._peers:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str) -> None:
        next_index = self._next_index[peer]
        prev_log_index = next_index - 1
        prev_log_term = self._term_at(prev_log_index)
        entries = tuple(entry for entry in self._log if entry.index >= next_index)
        message = AppendEntries(
            term=self._current_term,
            leader_id=self._node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self._commit_index,
        )
        self._network.send(self._node_id, peer, message)

    def _term_at(self, index: int) -> int:
        if index == 0:
            return 0
        for entry in self._log:
            if entry.index == index:
                return entry.term
        return 0

    def state(self) -> NodeState:
        return NodeState(
            node_id=self._node_id,
            current_term=self._current_term,
            role=self._role,
            log=self._log,
            commit_index=self._commit_index,
        )

    def _cluster_size(self) -> int:
        return len(self._peers) + 1

    def _quorum(self) -> int:
        return self._cluster_size() // 2 + 1

    def _start_election_timer(self) -> None:
        if self._election_timer is not None:
            self._election_timer.cancel()
        delay = self._random.randint(self._timeout_min, self._timeout_max)
        self._election_timer = self._clock.schedule(delay, self._on_election_timeout)

    def _on_election_timeout(self) -> None:
        self._become_candidate()

    def _become_candidate(self) -> None:
        self._role = Role.CANDIDATE
        self._current_term += 1
        self._voted_for = self._node_id
        self._votes = {self._node_id}
        self._start_election_timer()
        if len(self._votes) >= self._quorum():
            self._become_leader()
            return
        request = RequestVote(
            term=self._current_term,
            candidate_id=self._node_id,
            last_log_index=self._last_log_index(),
            last_log_term=self._last_log_term(),
        )
        for peer in self._peers:
            self._network.send(self._node_id, peer, request)

    def _become_leader(self) -> None:
        self._role = Role.LEADER
        if self._election_timer is not None:
            self._election_timer.cancel()
            self._election_timer = None
        last = self._last_log_index()
        self._next_index = dict.fromkeys(self._peers, last + 1)
        self._match_index = dict.fromkeys(self._peers, 0)
        self._broadcast_heartbeat()
        self._schedule_heartbeat()

    def _schedule_heartbeat(self) -> None:
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()
        self._heartbeat_timer = self._clock.schedule(self._heartbeat_interval, self._on_heartbeat)

    def _on_heartbeat(self) -> None:
        if self._role is not Role.LEADER:
            return
        self._broadcast_heartbeat()
        self._schedule_heartbeat()

    def _become_follower(self, term: int) -> None:
        if term > self._current_term:
            self._current_term = term
            self._voted_for = None
        self._role = Role.FOLLOWER
        self._votes = set()

    def _last_log_index(self) -> int:
        return self._log[-1].index if self._log else 0

    def _last_log_term(self) -> int:
        return self._log[-1].term if self._log else 0

    def _broadcast_heartbeat(self) -> None:
        for peer in self._peers:
            self._send_append_entries(peer)

    def receive(self, message: Message) -> Reply | None:
        if isinstance(message, RequestVote):
            return self._on_request_vote(message)
        if isinstance(message, RequestVoteResponse):
            self._on_request_vote_response(message)
            return None
        if isinstance(message, AppendEntries):
            return self._on_append_entries(message)
        if isinstance(message, AppendEntriesResponse):
            self._on_append_entries_response(message)
            return None
        raise TypeError(f"unknown message type {type(message).__name__}")

    def _on_request_vote(self, request: RequestVote) -> RequestVoteResponse:
        if request.term < self._current_term:
            return self._answer_vote(request.candidate_id, self._refuse_vote())
        if request.term > self._current_term:
            self._become_follower(request.term)
        already_voted_for_someone_else = (
            self._voted_for is not None and self._voted_for != request.candidate_id
        )
        if already_voted_for_someone_else:
            return self._answer_vote(request.candidate_id, self._refuse_vote())
        if not self._candidate_log_is_at_least_as_current(request):
            return self._answer_vote(request.candidate_id, self._refuse_vote())
        self._voted_for = request.candidate_id
        self._start_election_timer()
        granted = RequestVoteResponse(
            term=self._current_term, voter_id=self._node_id, vote_granted=True
        )
        return self._answer_vote(request.candidate_id, granted)

    def _answer_vote(self, candidate_id: str, response: RequestVoteResponse) -> RequestVoteResponse:
        if candidate_id != self._node_id:
            self._network.send(self._node_id, candidate_id, response)
        return response

    def _refuse_vote(self) -> RequestVoteResponse:
        return RequestVoteResponse(
            term=self._current_term, voter_id=self._node_id, vote_granted=False
        )

    def _candidate_log_is_at_least_as_current(self, request: RequestVote) -> bool:
        if request.last_log_term != self._last_log_term():
            return request.last_log_term > self._last_log_term()
        return request.last_log_index >= self._last_log_index()

    def _on_request_vote_response(self, response: RequestVoteResponse) -> None:
        if response.term > self._current_term:
            self._become_follower(response.term)
            return
        if self._role is not Role.CANDIDATE or response.term < self._current_term:
            return
        if not response.vote_granted:
            return
        self._votes.add(response.voter_id)
        if len(self._votes) >= self._quorum():
            self._become_leader()

    def _on_append_entries(self, message: AppendEntries) -> AppendEntriesResponse:
        if message.term < self._current_term:
            return self._answer_leader(
                message.leader_id,
                AppendEntriesResponse(
                    term=self._current_term,
                    voter_id=self._node_id,
                    success=False,
                    conflict_index=self._last_log_index() + 1,
                    acknowledged_index=0,
                ),
            )
        if message.term > self._current_term:
            self._become_follower(message.term)
        self._role = Role.FOLLOWER
        self._start_election_timer()
        if not self._predecessor_matches(message):
            return self._answer_leader(
                message.leader_id,
                AppendEntriesResponse(
                    term=self._current_term,
                    voter_id=self._node_id,
                    success=False,
                    conflict_index=self._last_log_index() + 1,
                    acknowledged_index=0,
                ),
            )
        self._store_entries(message)
        self._adopt_leader_commit(message)
        return self._answer_leader(
            message.leader_id,
            AppendEntriesResponse(
                term=self._current_term,
                voter_id=self._node_id,
                success=True,
                conflict_index=0,
                acknowledged_index=self._last_log_index(),
            ),
        )

    def _predecessor_matches(self, message: AppendEntries) -> bool:
        if message.prev_log_index == 0:
            return message.prev_log_term == 0
        for entry in self._log:
            if entry.index == message.prev_log_index:
                return entry.term == message.prev_log_term
        return False

    def _store_entries(self, message: AppendEntries) -> None:
        by_index = {entry.index: entry for entry in self._log}
        for incoming in message.entries:
            existing = by_index.get(incoming.index)
            if existing is not None and existing.term != incoming.term:
                by_index = {
                    index: entry for index, entry in by_index.items() if index < incoming.index
                }
            by_index[incoming.index] = incoming
        self._log = tuple(by_index[index] for index in sorted(by_index))

    def _adopt_leader_commit(self, message: AppendEntries) -> None:
        highest_local = self._last_log_index()
        self._commit_index = max(self._commit_index, min(message.leader_commit, highest_local))

    def _answer_leader(
        self, leader_id: str, response: AppendEntriesResponse
    ) -> AppendEntriesResponse:
        if leader_id != self._node_id:
            self._network.send(self._node_id, leader_id, response)
        return response

    def _on_append_entries_response(self, response: AppendEntriesResponse) -> None:
        if response.term > self._current_term:
            self._become_follower(response.term)
            return
        if self._role is not Role.LEADER or response.term < self._current_term:
            return
        peer = response.voter_id
        if response.success:
            self._match_index[peer] = max(
                self._match_index.get(peer, 0), response.acknowledged_index
            )
            self._next_index[peer] = self._match_index[peer] + 1
            self._advance_commit_index()
            return
        self._next_index[peer] = max(1, response.conflict_index)
        self._send_append_entries(peer)

    def _advance_commit_index(self) -> None:
        for index in range(self._last_log_index(), self._commit_index, -1):
            if self._term_at(index) != self._current_term:
                continue
            holders = 1 + sum(1 for match in self._match_index.values() if match >= index)
            if holders >= self._quorum():
                self._commit_index = index
                return
