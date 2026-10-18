from dataclasses import dataclass

from dfs.state import LogEntry


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteResponse:
    term: int
    voter_id: str
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesResponse:
    term: int
    voter_id: str
    success: bool
    conflict_index: int
    acknowledged_index: int
