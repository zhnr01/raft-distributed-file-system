from dataclasses import dataclass
from enum import Enum


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    command: bytes


@dataclass(frozen=True)
class NodeState:
    node_id: str
    current_term: int
    role: Role
    log: tuple[LogEntry, ...]
    commit_index: int


@dataclass(frozen=True)
class ClusterState:
    nodes: tuple[NodeState, ...]
