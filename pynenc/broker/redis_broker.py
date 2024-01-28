from functools import cached_property
from typing import TYPE_CHECKING, Optional

import redis

from pynenc.broker.base_broker import BaseBroker
from pynenc.conf.config_broker import ConfigBrokerRedis
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.util.redis_keys import Key

if TYPE_CHECKING:
    from ..app import Pynenc


class RedisQueue:
    """
    RedisQueue: A helper class for managing message queues in Redis.

    This class provides methods for sending, receiving, and purging messages in a Redis queue.
    It is designed to work specifically with the RedisBroker class to handle message queuing.

    :param Pynenc app: A reference to the Pynenc application.
    :param redis.Redis client: A Redis client instance.
    :param str name: The name of the queue.
    :param str namespace: The namespace for the queue, defaults to 'queue'.
    """

    def __init__(
        self, app: "Pynenc", client: redis.Redis, name: str, namespace: str = "queue"
    ):
        self.client = client
        self.key = Key(app.app_id, "broker")

    def send_message(self, message: str) -> None:
        """
        Send a message to the Redis queue.

        This method appends a message to the right end of the Redis list,
        effectively queuing it for processing.

        :param str message: The message to be queued.
        """
        self.client.rpush(self.key.default_queue(), message)

    def receive_message(self) -> Optional[str]:
        """
        Receive a message from the Redis queue.

        This method pops a message from the left end of the Redis list.
        If a message is available, it is returned after decoding; otherwise, None is returned.

        :return: The message from the queue, or None if the queue is empty.
        """
        if msg := self.client.lpop(self.key.default_queue()):
            return msg.decode()
        return None

    def purge(self) -> None:
        """
        Purge all messages from the Redis queue.

        This method clears all messages in the queue, effectively resetting it.
        """
        self.key.purge(self.client)


class RedisBroker(BaseBroker):
    """
    A Redis-backed implementation of the BaseBroker.

    This subclass of BaseBroker implements the abstract methods for routing,
    retrieving, and purging invocations using Redis as the message broker.
    It is suitable for production environments where robustness and scalability
    are required.

    :param Pynenc app: A reference to the Pynenc application.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        client = redis.Redis(
            host=self.conf.redis_host, port=self.conf.redis_port, db=self.conf.redis_db
        )
        self.queue = RedisQueue(app, client, "default")

    @cached_property
    def conf(self) -> ConfigBrokerRedis:
        return ConfigBrokerRedis(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def route_invocation(self, invocation: "DistributedInvocation") -> None:
        """
        Route an invocation by sending it to the Redis queue.

        This method serializes the DistributedInvocation object to JSON
        and sends it to the Redis queue for processing using the `send_message` method.

        :param DistributedInvocation invocation: The invocation to be queued.
        """
        self.queue.send_message(invocation.to_json())

    def retrieve_invocation(self) -> Optional["DistributedInvocation"]:
        """
        Retrieve the next invocation from the Redis queue.

        This method receives a message from the Redis queue using the `receive_message` method.
        If a message is received, it deserializes the JSON string back into a DistributedInvocation object.

        :return: The next invocation from the queue, or None if the queue is empty.
        """
        if inv := self.queue.receive_message():
            return DistributedInvocation.from_json(self.app, inv)
        return None

    def purge(self) -> None:
        """
        Purge all invocations from the Redis queue.

        This method delegates to the `purge` method of the RedisQueue to clear all messages.
        """
        return self.queue.purge()
