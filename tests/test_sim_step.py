from dfs.clock import Callback
from dfs.sim.clock import SimClock


def test_step_fires_exactly_one_due_event() -> None:
    clock = SimClock()
    fired: list[str] = []

    clock.schedule(10, lambda: fired.append("first"))
    clock.schedule(20, lambda: fired.append("second"))

    assert clock.step() is True
    assert fired == ["first"]
    assert clock.now() == 10

    assert clock.step() is True
    assert fired == ["first", "second"]
    assert clock.now() == 20


def test_step_reports_false_on_an_empty_queue() -> None:
    clock = SimClock()

    assert clock.step() is False
    assert clock.now() == 0


def test_step_does_not_move_time_past_the_event_it_fires() -> None:
    clock = SimClock()

    clock.schedule(500, lambda: None)
    clock.step()

    assert clock.now() == 500


def test_step_respects_the_scheduling_order_tiebreak() -> None:
    clock = SimClock()
    order: list[str] = []

    for name in ("first", "second", "third"):
        clock.schedule(5, _append(order, name))

    clock.step()
    clock.step()

    assert order == ["first", "second"]


def test_run_until_still_drains_to_the_deadline() -> None:
    clock = SimClock()
    fired: list[int] = []

    clock.schedule(10, lambda: fired.append(clock.now()))
    clock.schedule(90, lambda: fired.append(clock.now()))
    clock.schedule(900, lambda: fired.append(clock.now()))

    clock.advance(100)

    assert fired == [10, 90]
    assert clock.now() == 100


def test_step_from_inside_a_callback_is_rejected() -> None:
    clock = SimClock()
    failures: list[Exception] = []

    def reenter() -> None:
        try:
            clock.step()
        except Exception as error:
            failures.append(error)

    clock.schedule(10, reenter)
    clock.advance(50)

    assert len(failures) == 1
    assert isinstance(failures[0], RuntimeError)


def _append(sink: list[str], name: str) -> Callback:
    def record() -> None:
        sink.append(name)

    return record
