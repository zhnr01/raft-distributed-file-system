from conftest import run_scripted_exchange


def test_same_seed_replays_an_identical_exchange() -> None:
    assert run_scripted_exchange(4417) == run_scripted_exchange(4417)


def test_different_seeds_produce_different_exchanges() -> None:
    transcripts = {seed: tuple(run_scripted_exchange(seed)) for seed in range(40)}

    assert len(set(transcripts.values())) > 1


def test_the_exchange_actually_delivers_messages() -> None:
    transcript = run_scripted_exchange(4417)

    assert transcript != []
