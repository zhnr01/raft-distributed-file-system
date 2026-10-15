from typing import Any, cast

import pytest

from dfs.clock import Callback
from dfs.sim.clock import SimClock


def test_advance_fires_due_callbacks_in_time_order() -> None:
    clock = SimClock()
    fired_at: list[int] = []

    for delay in (300, 100, 200):
        clock.schedule(delay, lambda: fired_at.append(clock.now()))

    clock.advance(300)

    assert fired_at == [100, 200, 300]
    assert clock.now() == 300


def test_same_time_callbacks_fire_in_scheduling_order() -> None:
    clock = SimClock()
    order: list[str] = []

    def append_name(name: str) -> Callback:
        return lambda: order.append(name)

    for name in ("first", "second", "third"):
        clock.schedule(100, append_name(name))

    clock.advance(100)

    assert order == ["first", "second", "third"]


def test_advance_stops_before_a_callback_that_is_not_due() -> None:
    clock = SimClock()
    fired: list[str] = []

    clock.schedule(50, lambda: fired.append("inside"))
    clock.schedule(500, lambda: fired.append("outside"))

    clock.advance(100)

    assert fired == ["inside"]
    assert clock.now() == 100


def test_callback_scheduled_during_advance_runs_when_still_due() -> None:
    clock = SimClock()
    fired: list[int] = []

    def rearm() -> None:
        fired.append(clock.now())
        clock.schedule(30, lambda: fired.append(clock.now()))

    clock.schedule(10, rearm)
    clock.advance(100)

    assert fired == [10, 40]


def test_nothing_happens_without_an_explicit_advance() -> None:
    clock = SimClock()
    fired: list[str] = []

    clock.schedule(1, lambda: fired.append("should not fire"))

    assert clock.now() == 0
    assert fired == []

    clock.advance(0)

    assert clock.now() == 0
    assert fired == []


def test_a_zero_delay_callback_fires_on_the_next_advance() -> None:
    clock = SimClock()
    fired: list[int] = []

    clock.schedule(0, lambda: fired.append(clock.now()))

    assert fired == []

    clock.advance(1)

    assert fired == [0]


def test_a_callback_due_exactly_at_the_deadline_fires() -> None:
    clock = SimClock()
    fired: list[int] = []

    clock.schedule(100, lambda: fired.append(clock.now()))
    clock.advance(100)

    assert fired == [100]


def test_advance_over_an_empty_queue_still_moves_time() -> None:
    clock = SimClock()

    clock.advance(70)
    clock.advance(30)

    assert clock.now() == 100


def test_a_negative_delay_is_rejected() -> None:
    clock = SimClock()

    with pytest.raises(ValueError, match="delay must not be negative"):
        clock.schedule(-1, lambda: None)


def test_a_negative_duration_is_rejected() -> None:
    clock = SimClock()

    with pytest.raises(ValueError, match="duration must not be negative"):
        clock.advance(-1)


def test_advance_from_inside_a_callback_is_rejected() -> None:
    clock = SimClock()
    failures: list[Exception] = []

    def reenter() -> None:
        try:
            clock.advance(100)
        except Exception as error:
            failures.append(error)

    clock.schedule(10, reenter)
    clock.advance(50)

    assert len(failures) == 1
    assert isinstance(failures[0], RuntimeError)
    assert "advance is already running" in str(failures[0])


def test_virtual_time_never_moves_backwards() -> None:
    clock = SimClock()
    observed: list[int] = []

    clock.schedule(10, lambda: observed.append(clock.now()))
    clock.schedule(90, lambda: observed.append(clock.now()))
    clock.advance(50)
    observed.append(clock.now())
    clock.advance(60)
    observed.append(clock.now())

    assert observed == sorted(observed)
    assert observed == [10, 50, 90, 110]


def test_a_non_integer_delay_is_rejected() -> None:
    clock = SimClock()
    not_an_int: Any = float("nan")

    with pytest.raises(TypeError, match="delay must be an integer"):
        clock.schedule(not_an_int, lambda: None)

    with pytest.raises(TypeError, match="delay must be an integer"):
        clock.schedule(cast(int, 0.5), lambda: None)

    with pytest.raises(TypeError, match="delay must be an integer"):
        clock.schedule(cast(int, float("inf")), lambda: None)

    with pytest.raises(TypeError, match="delay must be an integer"):
        clock.schedule(cast(int, True), lambda: None)


def test_a_non_integer_duration_is_rejected() -> None:
    clock = SimClock()

    with pytest.raises(TypeError, match="duration must be an integer"):
        clock.advance(cast(int, float("nan")))


def test_a_raising_callback_leaves_the_clock_at_the_deadline() -> None:
    clock = SimClock()
    fired: list[str] = []

    clock.schedule(10, lambda: fired.append("before"))
    clock.schedule(20, _explode)
    clock.schedule(30, lambda: fired.append("after"))

    with pytest.raises(RuntimeError, match="boom"):
        clock.advance(100)

    assert clock.now() == 100
    assert fired == ["before"]

    clock.advance(0)

    assert fired == ["before", "after"]


def _explode() -> None:
    raise RuntimeError("boom")
