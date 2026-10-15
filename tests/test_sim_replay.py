from dfs.network import Message
from dfs.sim.network import NetworkConditions
from dfs.sim.simulation import Simulation

PING_LIMIT = 6


class PingNode:
    def __init__(self, name: str, peer: str, simulation: Simulation) -> None:
        self.name = name
        self.peer = peer
        self.transcript: list[tuple[int, str]] = []
        self._simulation = simulation

    def receive(self, message: Message) -> None:
        text = str(message)
        self.transcript.append((self._simulation.clock.now(), text))
        count = int(text.split("-")[1])
        if count < PING_LIMIT:
            self._simulation.network.send(self.name, self.peer, f"ping-{count + 1}")

    def start(self) -> None:
        self._simulation.network.send(self.name, self.peer, "ping-0")


def run_scripted_exchange(seed: int) -> list[tuple[int, str]]:
    conditions = NetworkConditions(min_delay=1, max_delay=40, drop_probability=0.1)
    simulation = Simulation(seed=seed, conditions=conditions)
    first = PingNode("a", "b", simulation)
    second = PingNode("b", "a", simulation)
    simulation.register("a", first.receive)
    simulation.register("b", second.receive)

    first.start()
    simulation.run_until(2000)

    return [(at, f"{text}") for at, text in first.transcript + second.transcript]


def test_same_seed_replays_an_identical_exchange() -> None:
    assert run_scripted_exchange(4417) == run_scripted_exchange(4417)


def test_different_seeds_produce_different_exchanges() -> None:
    transcripts = {seed: tuple(run_scripted_exchange(seed)) for seed in range(40)}

    assert len(set(transcripts.values())) > 1


def test_the_exchange_actually_delivers_messages() -> None:
    transcript = run_scripted_exchange(4417)

    assert transcript != []
