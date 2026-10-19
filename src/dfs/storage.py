import base64
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dfs.state import LogEntry

CHECKSUM_KEY = "checksum"
PAYLOAD_KEY = "payload"


class StorageCorrupted(ValueError):
    pass


@dataclass(frozen=True)
class PersistentState:
    current_term: int
    voted_for: str | None
    log: tuple[LogEntry, ...]

    def __post_init__(self) -> None:
        if isinstance(self.current_term, bool) or not isinstance(self.current_term, int):
            raise TypeError(
                f"current_term must be an integer, not {type(self.current_term).__name__}"
            )
        if self.current_term < 0:
            raise ValueError(f"current_term must not be negative, got {self.current_term}")
        if self.voted_for is not None and (
            not isinstance(self.voted_for, str) or not self.voted_for
        ):
            raise ValueError(
                f"voted_for must be None or a non-empty string, got {self.voted_for!r}"
            )
        previous: int | None = None
        for entry in self.log:
            if previous is not None and entry.index <= previous:
                raise ValueError(
                    f"persisted log indices are not strictly increasing: "
                    f"{previous} then {entry.index}"
                )
            previous = entry.index


class Storage(Protocol):
    def save(self, state: PersistentState) -> None: ...

    def load(self) -> PersistentState: ...


def _checksum(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _encode_payload(state: PersistentState) -> str:
    document = {
        "current_term": state.current_term,
        "voted_for": state.voted_for,
        "log": [
            {
                "term": entry.term,
                "index": entry.index,
                "command": base64.b64encode(entry.command).decode("ascii"),
            }
            for entry in state.log
        ],
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def _require_object(raw: object, message: str) -> dict[str, object]:
    if not isinstance(raw, dict):
        raise StorageCorrupted(message)
    return raw


def _require_integer(value: object, message: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StorageCorrupted(message)
    return value


def _require_string(value: object, message: str) -> str:
    if not isinstance(value, str):
        raise StorageCorrupted(message)
    return value


def _decode_payload(payload: str) -> PersistentState:
    try:
        decoded: object = json.loads(payload)
    except json.JSONDecodeError as error:
        raise StorageCorrupted(f"persisted state is not valid JSON: {error}") from error
    document = _require_object(decoded, "persisted state must be a JSON object")
    raw_term = document.get("current_term")
    current_term = _require_integer(
        raw_term,
        f"persisted current_term must be an integer, got {type(raw_term).__name__}",
    )
    voted_for = _optional_string(
        document.get("voted_for"), "persisted voted_for must be null or a string"
    )
    entries = document.get("log")
    if not isinstance(entries, list):
        raise StorageCorrupted("persisted log must be a list")
    try:
        return PersistentState(
            current_term=current_term,
            voted_for=voted_for,
            log=tuple(_decode_entry(entry) for entry in entries),
        )
    except StorageCorrupted:
        raise
    except (TypeError, ValueError) as error:
        raise StorageCorrupted(f"persisted state is not a legal Raft state: {error}") from error


def _optional_string(value: object, message: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, message)


def _decode_entry(raw: object) -> LogEntry:
    entry = _require_object(raw, f"persisted log entry must be an object, got {type(raw).__name__}")
    term = _require_integer(
        entry.get("term"),
        f"persisted entry term must be an integer, got {type(entry.get('term')).__name__}",
    )
    index = _require_integer(
        entry.get("index"),
        f"persisted entry index must be an integer, got {type(entry.get('index')).__name__}",
    )
    command = _require_string(
        entry.get("command"),
        f"persisted entry command must be a string, got {entry.get('command')!r}",
    )
    try:
        decoded = base64.b64decode(command, validate=True)
    except ValueError as error:
        raise StorageCorrupted(f"persisted entry command is not valid base64: {error}") from error
    return LogEntry(term=term, index=index, command=decoded)


class FileStorage:
    def __init__(self, path: Path) -> None:
        self._path = path

    def save(self, state: PersistentState) -> None:
        payload = _encode_payload(state)
        document = json.dumps({CHECKSUM_KEY: _checksum(payload), PAYLOAD_KEY: payload})
        self._write_atomically(document.encode("utf-8"))

    def load(self) -> PersistentState:
        if not self._path.exists():
            return PersistentState(current_term=0, voted_for=None, log=())
        try:
            decoded: object = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise StorageCorrupted(f"{self._path} is not valid JSON: {error}") from error
        document = _require_object(decoded, f"{self._path} must contain a JSON object")
        payload = _require_string(
            document.get(PAYLOAD_KEY), f"{self._path} has no string {PAYLOAD_KEY!r}"
        )
        recorded = _require_string(
            document.get(CHECKSUM_KEY), f"{self._path} has no string {CHECKSUM_KEY!r}"
        )
        actual = _checksum(payload)
        if actual != recorded:
            raise StorageCorrupted(
                f"{self._path} failed its checksum: recorded {recorded}, computed {actual}"
            )
        return _decode_payload(payload)

    def _write_atomically(self, data: bytes) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_name(f"{self._path.name}.tmp{os.getpid()}")
        try:
            with open(temporary, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._path)
        finally:
            temporary.unlink(missing_ok=True)
