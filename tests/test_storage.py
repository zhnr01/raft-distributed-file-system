import hashlib
import json
from pathlib import Path

import pytest

from dfs.state import LogEntry
from dfs.storage import FileStorage, PersistentState, StorageCorrupted

VALID_ENTRY: dict[str, object] = {"term": 1, "index": 1, "command": "aGVsbG8="}
VALID: dict[str, object] = {"current_term": 1, "voted_for": "n2", "log": [VALID_ENTRY]}


def _payload(**changes: object) -> str:
    return json.dumps({**VALID, **changes})


def _payload_with_entry(**changes: object) -> str:
    return _payload(log=[{**VALID_ENTRY, **changes}])


def _write_with_valid_checksum(path: Path, payload: str) -> None:
    checksum = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    path.write_text(json.dumps({"checksum": checksum, "payload": payload}), encoding="utf-8")


MALFORMED_PAYLOADS = {
    "payload is not JSON": "{not json",
    "payload is not an object": "[]",
    "current_term is a boolean": _payload(current_term=True),
    "current_term is a string": _payload(current_term="1"),
    "current_term is negative": _payload(current_term=-1),
    "voted_for is a number": _payload(voted_for=7),
    "voted_for is empty": _payload(voted_for=""),
    "log is not a list": _payload(log={}),
    "an entry is not an object": _payload(log=[1]),
    "an entry has no term": _payload(log=[{"index": 1, "command": "aGVsbG8="}]),
    "an entry term is zero": _payload_with_entry(term=0),
    "an entry index is a boolean": _payload_with_entry(index=True),
    "an entry command is not a string": _payload_with_entry(command=5),
    "an entry command is not strict base64": _payload_with_entry(command="aGVs!bG8="),
    "log indices go backwards": _payload(log=[{**VALID_ENTRY, "index": 2}, VALID_ENTRY]),
}

BROKEN_ENVELOPES = {
    "the file is not an object": "[]",
    "the file has no payload": json.dumps({"checksum": "0" * 64}),
    "the file has no checksum": json.dumps({"payload": json.dumps(VALID)}),
    "the payload is not a string": json.dumps({"checksum": "0" * 64, "payload": VALID}),
}


def test_a_saved_state_loads_back_identical(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    state = PersistentState(
        current_term=7,
        voted_for="n2",
        log=(
            LogEntry(term=1, index=1, command=b"create /a"),
            LogEntry(term=7, index=2, command=b"create /b"),
        ),
    )

    FileStorage(path).save(state)

    assert FileStorage(path).load() == state


def test_a_state_file_whose_payload_was_altered_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    storage = FileStorage(path)
    storage.save(PersistentState(current_term=3, voted_for=None, log=()))
    document = json.loads(path.read_text(encoding="utf-8"))
    tampered = document["payload"].replace('"current_term":3', '"current_term":9')
    assert tampered != document["payload"], "the fixture failed to alter the payload"
    path.write_text(json.dumps({**document, "payload": tampered}), encoding="utf-8")

    with pytest.raises(StorageCorrupted):
        storage.load()


def test_a_truncated_state_file_is_refused_rather_than_read_as_empty(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    storage = FileStorage(path)
    storage.save(
        PersistentState(
            current_term=4,
            voted_for="n3",
            log=(LogEntry(term=4, index=1, command=b"payload"),),
        )
    )
    whole = path.read_bytes()
    path.write_bytes(whole[: len(whole) // 2])

    with pytest.raises(StorageCorrupted):
        storage.load()


def test_a_rejected_state_never_reaches_the_disk(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    storage = FileStorage(path)
    committed = PersistentState(current_term=2, voted_for="n2", log=())
    storage.save(committed)

    with pytest.raises(ValueError):
        storage.save(PersistentState(current_term=-1, voted_for=None, log=()))

    assert storage.load() == committed


def test_a_write_that_fails_at_the_rename_leaves_the_previous_state_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "n1.state"
    storage = FileStorage(path)
    committed = PersistentState(current_term=2, voted_for="n2", log=())
    storage.save(committed)

    def refuse_the_rename(source: object, destination: object) -> None:
        raise OSError("simulated failure at the rename")

    monkeypatch.setattr("dfs.storage.os.replace", refuse_the_rename)

    with pytest.raises(OSError):
        storage.save(
            PersistentState(
                current_term=9,
                voted_for=None,
                log=(LogEntry(term=9, index=1, command=b"half-written"),),
            )
        )

    assert storage.load() == committed
    assert sorted(entry.name for entry in tmp_path.iterdir()) == ["n1.state"]


def test_saving_into_a_directory_that_does_not_exist_yet_creates_it(tmp_path: Path) -> None:
    path = tmp_path / "data" / "n1" / "raft.state"
    state = PersistentState(current_term=1, voted_for=None, log=())

    FileStorage(path).save(state)

    assert FileStorage(path).load() == state


def test_a_boolean_term_is_refused_before_it_can_poison_the_file() -> None:
    with pytest.raises(TypeError):
        PersistentState(current_term=True, voted_for=None, log=())


def test_a_hand_written_state_file_with_a_valid_checksum_loads(tmp_path: Path) -> None:
    path = tmp_path / "n1.state"
    _write_with_valid_checksum(path, json.dumps(VALID))

    assert FileStorage(path).load() == PersistentState(
        current_term=1,
        voted_for="n2",
        log=(LogEntry(term=1, index=1, command=b"hello"),),
    )


@pytest.mark.parametrize(
    "payload", list(MALFORMED_PAYLOADS.values()), ids=list(MALFORMED_PAYLOADS.keys())
)
def test_a_checksummed_file_holding_an_impossible_state_is_refused(
    tmp_path: Path, payload: str
) -> None:
    path = tmp_path / "n1.state"
    _write_with_valid_checksum(path, payload)

    with pytest.raises(StorageCorrupted):
        FileStorage(path).load()


@pytest.mark.parametrize(
    "document", list(BROKEN_ENVELOPES.values()), ids=list(BROKEN_ENVELOPES.keys())
)
def test_a_state_file_with_a_broken_envelope_is_refused(tmp_path: Path, document: str) -> None:
    path = tmp_path / "n1.state"
    path.write_text(document, encoding="utf-8")

    with pytest.raises(StorageCorrupted):
        FileStorage(path).load()
