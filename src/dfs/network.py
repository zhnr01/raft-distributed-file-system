from typing import Protocol

Message = object


class Network(Protocol):
    def send(self, sender: str, recipient: str, message: Message) -> None: ...
