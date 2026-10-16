from dataclasses import dataclass
from enum import Enum
from itertools import combinations

from dfs.state import ClusterState, LogEntry, Role


class Invariant(Enum):
    ELECTION_SAFETY = "election safety"
    LEADER_APPEND_ONLY = "leader append-only"
    LOG_MATCHING = "log matching"
    LEADER_COMPLETENESS = "leader completeness"


@dataclass(frozen=True)
class Violation:
    invariant: Invariant
    detail: str


class InvariantViolated(Exception):
    def __init__(self, violation: Violation, seed: int, time: int) -> None:
        super().__init__(
            f"{violation.invariant.value} violated at t={time} on seed {seed}: {violation.detail}"
        )
        self.violation = violation
        self.seed = seed
        self.time = time


class InvariantChecker:
    def __init__(self) -> None:
        self._leaders_by_term: dict[int, set[str]] = {}
        self._leader_logs: dict[tuple[str, int], tuple[LogEntry, ...]] = {}
        self._committed: dict[int, _Committed] = {}

    def check(self, cluster: ClusterState) -> tuple[Violation, ...]:
        violations: list[Violation] = []
        violations.extend(self._check_election_safety(cluster))
        violations.extend(self._check_leader_append_only(cluster))
        violations.extend(self._check_log_matching(cluster))
        violations.extend(self._check_leader_completeness(cluster))
        return tuple(violations)

    def _check_election_safety(self, cluster: ClusterState) -> list[Violation]:
        violations: list[Violation] = []
        for node in cluster.nodes:
            if node.role is not Role.LEADER:
                continue
            claimants = self._leaders_by_term.setdefault(node.current_term, set())
            claimants.add(node.node_id)
            if len(claimants) > 1:
                violations.append(
                    Violation(
                        invariant=Invariant.ELECTION_SAFETY,
                        detail=f"term {node.current_term} has leaders {sorted(claimants)}",
                    )
                )
        return violations

    def _check_leader_append_only(self, cluster: ClusterState) -> list[Violation]:
        violations: list[Violation] = []
        for node in cluster.nodes:
            if node.role is not Role.LEADER:
                continue
            key = (node.node_id, node.current_term)
            remembered = self._leader_logs.get(key)
            if remembered is not None and not _is_prefix(remembered, node.log):
                violations.append(
                    Violation(
                        invariant=Invariant.LEADER_APPEND_ONLY,
                        detail=(
                            f"leader {node.node_id} in term {node.current_term} "
                            f"changed its log from {_shape(remembered)} to {_shape(node.log)}"
                        ),
                    )
                )
            self._leader_logs[key] = node.log
        return violations

    def _check_log_matching(self, cluster: ClusterState) -> list[Violation]:
        violations: list[Violation] = []
        for first, second in combinations(cluster.nodes, 2):
            first_entries = _entries_by_index(first.log)
            second_entries = _entries_by_index(second.log)
            agreed = [
                index
                for index, entry in first_entries.items()
                if (other := second_entries.get(index)) is not None and other.term == entry.term
            ]
            if not agreed:
                continue
            index = max(agreed)
            divergence = _first_divergence_through(first_entries, second_entries, index)
            if divergence is not None:
                violations.append(
                    Violation(
                        invariant=Invariant.LOG_MATCHING,
                        detail=(
                            f"{first.node_id} and {second.node_id} agree at index {index} "
                            f"but differ at index {divergence}: "
                            f"{_shape(first.log)} vs {_shape(second.log)}"
                        ),
                    )
                )
        return violations

    def _check_leader_completeness(self, cluster: ClusterState) -> list[Violation]:
        violations: list[Violation] = []
        for node in cluster.nodes:
            for entry in node.log:
                if entry.index > node.commit_index:
                    continue
                known = self._committed.get(entry.index)
                if known is None:
                    self._committed[entry.index] = _Committed(entry=entry, term=node.current_term)
                elif node.current_term < known.term:
                    self._committed[entry.index] = _Committed(
                        entry=known.entry, term=node.current_term
                    )
        for node in cluster.nodes:
            if node.role is not Role.LEADER:
                continue
            for index, known in sorted(self._committed.items()):
                if node.current_term <= known.term:
                    continue
                held = _entry_at(node.log, index)
                if held != known.entry:
                    violations.append(
                        Violation(
                            invariant=Invariant.LEADER_COMPLETENESS,
                            detail=(
                                f"leader {node.node_id} in term {node.current_term} lacks the "
                                f"entry committed at index {index} in term {known.entry.term}; "
                                f"holds {_shape((held,)) if held is not None else 'nothing'}"
                            ),
                        )
                    )
        return violations


@dataclass(frozen=True)
class _Committed:
    entry: LogEntry
    term: int


def _entry_at(log: tuple[LogEntry, ...], index: int) -> LogEntry | None:
    for entry in log:
        if entry.index == index:
            return entry
    return None


def _entries_by_index(log: tuple[LogEntry, ...]) -> dict[int, LogEntry]:
    entries: dict[int, LogEntry] = {}
    for entry in log:
        entries.setdefault(entry.index, entry)
    return entries


def _first_divergence_through(
    first_entries: dict[int, LogEntry], second_entries: dict[int, LogEntry], index: int
) -> int | None:
    for position in sorted(set(first_entries) | set(second_entries)):
        if position > index:
            break
        if first_entries.get(position) != second_entries.get(position):
            return position
    return None


def _is_prefix(remembered: tuple[LogEntry, ...], current: tuple[LogEntry, ...]) -> bool:
    return len(current) >= len(remembered) and current[: len(remembered)] == remembered


def _shape(log: tuple[LogEntry | None, ...]) -> list[tuple[int, int] | None]:
    return [None if entry is None else (entry.index, entry.term) for entry in log]
