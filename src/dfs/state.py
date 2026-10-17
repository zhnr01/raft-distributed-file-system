from dataclasses import dataclass
from enum import Enum


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


def _reject_non_integer(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer, not {type(value).__name__}")


@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    command: bytes

    def __post_init__(self) -> None:
        _reject_non_integer("term", self.term)
        _reject_non_integer("index", self.index)
        if self.term < 1:
            raise ValueError(f"log entry term must be at least 1, got {self.term}")
        if self.index < 0:
            raise ValueError(f"log entry index must not be negative, got {self.index}")


@dataclass(frozen=True)
class NodeState:
    node_id: str
    current_term: int
    role: Role
    log: tuple[LogEntry, ...]
    commit_index: int

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise ValueError(f"node_id must be a non-empty string, got {self.node_id!r}")
        _reject_non_integer("current_term", self.current_term)
        _reject_non_integer("commit_index", self.commit_index)
        if self.current_term < 0:
            raise ValueError(
                f"node {self.node_id} reports current_term {self.current_term}, "
                "but terms start at 0"
            )
        if self.commit_index < 0:
            raise ValueError(
                f"node {self.node_id} reports negative commit_index {self.commit_index}"
            )
        self._reject_malformed_log()

    def _reject_malformed_log(self) -> None:
        previous: int | None = None
        for entry in self.log:
            if previous is not None and entry.index <= previous:
                raise ValueError(
                    f"node {self.node_id} reports log indices that are not strictly "
                    f"increasing: {previous} then {entry.index}"
                )
            previous = entry.index
        if not self.log:
            if self.commit_index != 0:
                raise ValueError(
                    f"node {self.node_id} reports commit_index {self.commit_index} "
                    "with an empty log"
                )
            return
        if self.commit_index > self.log[-1].index:
            raise ValueError(
                f"node {self.node_id} reports commit_index {self.commit_index} "
                f"beyond its last entry at index {self.log[-1].index}"
            )


@dataclass(frozen=True)
class ClusterState:
    nodes: tuple[NodeState, ...]

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for node in self.nodes:
            if node.node_id in seen:
                raise ValueError(
                    f"duplicate node {node.node_id} in one snapshot; the checker "
                    "cannot tell a node apart from itself"
                )
            seen.add(node.node_id)
