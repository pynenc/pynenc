from functools import cached_property
from typing import TYPE_CHECKING, Optional

import redis

from pynenc.broker.base_broker import BaseBroker
from pynenc.conf.config_broker import ConfigBrokerRedis
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.util.redis_client import get_redis_client
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
        self.app = app
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

    def send_messages_batch(self, messages: list[str]) -> None:
        """
        Send multiple messages to the Redis queue in a single operation.

        This method uses a Redis pipeline to send multiple messages to the queue
        in a single operation, which can improve performance when routing multiple invocations.

        :param list[str] messages: The list of messages to be queued.
        """
        with self.client.pipeline() as pipe:
            for message in messages:
                pipe.rpush(self.key.default_queue(), message)
            pipe.execute()

    def receive_message(self) -> Optional[str]:
        """
        Receive a message from the Redis queue.

        This method blocks for a configured timeout waiting for a message.
        Uses BLPOP for efficient waiting instead of polling.

        :return: The message from the queue, or None if timeout is reached.
        """
        if msg := self.client.blpop(
            self.key.default_queue(), timeout=self.app.broker.conf.queue_timeout_sec
        ):
            # blpop returns tuple of (key, value)
            return msg[1].decode()
        return None

    def count_invocations(self) -> int:
        """
        Get the number of messages in the Redis queue.

        This method queries the Redis queue for the number of messages currently in the queue.

        :return: The number of messages in the queue.
        """
        return self.client.llen(self.key.default_queue())

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
        self._client: redis.Redis | None = None
        self._queue: RedisQueue | None = None

    @property
    def client(self) -> redis.Redis:
        """Lazy initialization of Redis client"""
        if self._client is None:
            self.app.logger.debug("Lazy initializing Redis client for queue")
            self._client = get_redis_client(self.conf)
        return self._client

    @property
    def queue(self) -> RedisQueue:
        if self._queue is None:
            self._queue = RedisQueue(self.app, self.client, "default")
        return self._queue

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

    def route_invocations(self, invocations: list[DistributedInvocation]) -> None:
        """
        Routes multiple invocations at once using Redis pipeline for better performance.

        :param list[DistributedInvocation] invocations: The invocations to be routed.
        """
        if not invocations:
            return

        messages = [invocation.to_json() for invocation in invocations]
        self.queue.send_messages_batch(messages)

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

    def count_invocations(self) -> int:
        """
        Get the number of invocations in the Redis queue.

        This method queries the Redis queue for the number of messages currently in the queue.

        :return: The number of invocations in the queue.
        """
        return self.queue.count_invocations()

    def purge(self) -> None:
        """
        Purge all invocations from the Redis queue.

        This method delegates to the `purge` method of the RedisQueue to clear all messages.
        """
        return self.queue.purge()
