from collections import deque
from typing import TYPE_CHECKING, Optional

import redis

from ..invocation import DistributedInvocation
from ..util.redis_keys import Key
from .base_broker import BaseBroker

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation


class RedisQueue:
    def __init__(
        self, app: "Pynenc", client: redis.Redis, name: str, namespace: str = "queue"
    ):
        self.client = client
        self.key = Key(app.app_id, "broker")

    def send_message(self, message: str) -> None:
        self.client.rpush(self.key.default_queue(), message)

    def receive_message(self) -> Optional[str]:
        if msg := self.client.lpop(self.key.default_queue()):
            return msg.decode()
        return None

    def purge(self) -> None:
        self.key.purge(self.client)


class RedisBroker(BaseBroker):
    def __init__(self, app: "Pynenc") -> None:
        client = redis.Redis(host="localhost", port=6379, db=0)
        self.queue = RedisQueue(app, client, "default")
        super().__init__(app)

    def route_invocation(self, invocation: "DistributedInvocation") -> None:
        self.queue.send_message(invocation.to_json())

    def retrieve_invocation(self) -> Optional["DistributedInvocation"]:
        if inv := self.queue.receive_message():
            return DistributedInvocation.from_json(self.app, inv)
        return None

    def purge(self) -> None:
        return self.queue.purge()
