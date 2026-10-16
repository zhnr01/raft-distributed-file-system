from random import Random

from dfs.invariants import Invariant, InvariantChecker
from dfs.state import ClusterState, LogEntry, NodeState, Role

NODES = ("n1", "n2", "n3")
COMMANDS = (b"a", b"b")
PAIRS = 2000
HISTORIES = 300
STEPS = 12


def _random_log(random: Random) -> tuple[LogEntry, ...]:
    terms = sorted(random.randint(1, 3) for _ in range(random.randint(0, 5)))
    return tuple(
        LogEntry(term=term, index=index, command=random.choice(COMMANDS))
        for index, term in enumerate(terms, start=1)
    )


def _follower(node_id: str, log: tuple[LogEntry, ...]) -> NodeState:
    return NodeState(
        node_id=node_id,
        current_term=3,
        role=Role.FOLLOWER,
        log=log,
        commit_index=0,
    )


def _random_history(random: Random) -> list[ClusterState]:
    terms = dict.fromkeys(NODES, 1)
    roles = dict.fromkeys(NODES, Role.FOLLOWER)
    logs: dict[str, tuple[LogEntry, ...]] = dict.fromkeys(NODES, ())
    history: list[ClusterState] = []
    for _ in range(STEPS):
        for name in NODES:
            roll = random.random()
            if roll < 0.2:
                terms[name] += 1
                roles[name] = Role.FOLLOWER
            elif roll < 0.3:
                roles[name] = Role.LEADER
            log = logs[name]
            change = random.random()
            if change < 0.4:
                entry = LogEntry(
                    term=terms[name], index=len(log) + 1, command=random.choice(COMMANDS)
                )
                logs[name] = (*log, entry)
            elif change < 0.5 and log:
                logs[name] = log[: random.randint(0, len(log) - 1)]
        history.append(
            ClusterState(
                nodes=tuple(
                    NodeState(
                        node_id=name,
                        current_term=terms[name],
                        role=roles[name],
                        log=logs[name],
                        commit_index=0,
                    )
                    for name in NODES
                )
            )
        )
    return history


def _log_matching_holds_by_definition(
    first: tuple[LogEntry, ...], second: tuple[LogEntry, ...]
) -> bool:
    for index in range(1, min(len(first), len(second)) + 1):
        if first[index - 1].term != second[index - 1].term:
            continue
        if first[:index] != second[:index]:
            return False
    return True


def _first_election_safety_breach_by_definition(history: list[ClusterState]) -> int | None:
    for step in range(1, len(history) + 1):
        leaders: dict[int, set[str]] = {}
        for cluster in history[:step]:
            for node in cluster.nodes:
                if node.role is Role.LEADER:
                    leaders.setdefault(node.current_term, set()).add(node.node_id)
        if any(len(names) > 1 for names in leaders.values()):
            return step
    return None


def _first_append_only_breach_by_definition(history: list[ClusterState]) -> int | None:
    for step, cluster in enumerate(history, start=1):
        for node in cluster.nodes:
            if node.role is not Role.LEADER:
                continue
            for earlier in history[: step - 1]:
                for before in earlier.nodes:
                    same_leadership = (
                        before.node_id == node.node_id
                        and before.current_term == node.current_term
                        and before.role is Role.LEADER
                    )
                    if same_leadership and node.log[: len(before.log)] != before.log:
                        return step
    return None


def _first_reported_step(history: list[ClusterState], invariant: Invariant) -> int | None:
    checker = InvariantChecker()
    for step, cluster in enumerate(history, start=1):
        if any(violation.invariant is invariant for violation in checker.check(cluster)):
            return step
    return None


def test_log_matching_agrees_with_the_definition_on_random_pairs() -> None:
    random = Random(2026)
    disagreements: list[tuple[tuple[LogEntry, ...], tuple[LogEntry, ...]]] = []
    violated = 0

    for _ in range(PAIRS):
        first, second = _random_log(random), _random_log(random)
        cluster = ClusterState(nodes=(_follower("n1", first), _follower("n2", second)))
        reported = any(
            violation.invariant is Invariant.LOG_MATCHING
            for violation in InvariantChecker().check(cluster)
        )
        holds = _log_matching_holds_by_definition(first, second)
        violated += not holds
        if reported is holds:
            disagreements.append((first, second))

    assert disagreements == []
    assert 0 < violated < PAIRS


def test_election_safety_agrees_with_the_definition_on_random_histories() -> None:
    random = Random(4417)
    outcomes: list[int | None] = []

    for _ in range(HISTORIES):
        history = _random_history(random)
        expected = _first_election_safety_breach_by_definition(history)

        assert _first_reported_step(history, Invariant.ELECTION_SAFETY) == expected
        outcomes.append(expected)

    assert None in outcomes
    assert len(set(outcomes)) > 3


def test_append_only_agrees_with_the_definition_on_random_histories() -> None:
    random = Random(31)
    outcomes: list[int | None] = []

    for _ in range(HISTORIES):
        history = _random_history(random)
        expected = _first_append_only_breach_by_definition(history)

        assert _first_reported_step(history, Invariant.LEADER_APPEND_ONLY) == expected
        outcomes.append(expected)

    assert None in outcomes
    assert len(set(outcomes)) > 3
