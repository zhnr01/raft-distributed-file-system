from dfs.sim.clock import SimClock


def test_cancel_stops_a_scheduled_callback_from_firing() -> None:
    clock = SimClock()
    fired: list[str] = []

    timer = clock.schedule(10, lambda: fired.append("election"))
    timer.cancel()
    clock.advance(100)

    assert fired == []


def test_cancelling_one_timer_leaves_the_others_alive() -> None:
    clock = SimClock()
    fired: list[str] = []

    first = clock.schedule(10, lambda: fired.append("first"))
    clock.schedule(20, lambda: fired.append("second"))
    first.cancel()
    clock.advance(100)

    assert fired == ["second"]


def test_a_cancelled_timer_does_not_move_the_clock_to_its_due_time() -> None:
    clock = SimClock()
    fired: list[int] = []

    clock.schedule(50, lambda: fired.append(clock.now())).cancel()
    clock.schedule(80, lambda: fired.append(clock.now()))

    assert clock.next_due() == 80
    clock.step()

    assert fired == [80]


def test_cancelling_an_already_fired_timer_is_harmless() -> None:
    clock = SimClock()
    fired: list[str] = []

    timer = clock.schedule(10, lambda: fired.append("once"))
    clock.advance(20)
    timer.cancel()
    clock.advance(50)

    assert fired == ["once"]


def test_a_cancelled_timer_does_not_consume_a_step() -> None:
    clock = SimClock()

    clock.schedule(10, lambda: None).cancel()
    clock.schedule(20, lambda: None)

    assert clock.step() is True
    assert clock.now() == 20
