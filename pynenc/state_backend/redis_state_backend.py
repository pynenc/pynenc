import json
from functools import cached_property
from typing import TYPE_CHECKING

import redis

from pynenc import exceptions
from pynenc.conf.config_state_backend import ConfigStateBackendRedis
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.state_backend.base_state_backend import BaseStateBackend, InvocationHistory
from pynenc.util.redis_keys import Key

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.types import Params, Result


class RedisStateBackend(BaseStateBackend):
    """
    A Redis-based implementation of the state backend.

    This backend uses Redis to store and retrieve the state of invocations, including their data,
    history, results, and exceptions. It's suitable for distributed systems where shared state management is required.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.client = redis.Redis(
            host=self.conf.redis_host, port=self.conf.redis_port, db=self.conf.redis_db
        )
        self.key = Key(app.app_id, "state_backend")

    @cached_property
    def conf(self) -> ConfigStateBackendRedis:
        return ConfigStateBackendRedis(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def purge(self) -> None:
        """Clears all data from the Redis backend for the current `app.app_id`."""
        self.key.purge(self.client)

    def _upsert_invocation(self, invocation: "DistributedInvocation") -> None:
        """
        Inserts or updates an invocation in Redis.

        :param DistributedInvocation invocation: The invocation object to upsert.
        """
        self.client.set(
            self.key.invocation(invocation.invocation_id), invocation.to_json()
        )

    def _get_invocation(
        self, invocation_id: str
    ) -> "DistributedInvocation[Params, Result]":
        """
        Retrieves an invocation from Redis by its ID.

        :param DistributedInvocation invocation_id: The ID of the invocation to retrieve.
        :return: The retrieved invocation object.
        """
        if inv := self.client.get(self.key.invocation(invocation_id)):
            return DistributedInvocation.from_json(self.app, inv.decode())
        raise KeyError(f"Invocation {invocation_id} not found")

    def _add_history(
        self,
        invocation: "DistributedInvocation",
        invocation_history: "InvocationHistory",
    ) -> None:
        """
        Adds a history record to an invocation in Redis.

        :param DistributedInvocation invocation: The invocation to add history for.
        :param InvocationHistory invocation_history: The history record to add.
        """
        self.client.rpush(
            self.key.history(invocation.invocation_id),
            invocation_history.to_json(),
        )

    def _get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list[InvocationHistory]:
        """
        Retrieves the history of an invocation from Redis.

        :param DistributedInvocation invocation: The invocation to get the history for.
        :return: A list of invocation history records.
        """
        return [
            InvocationHistory.from_json(h.decode())
            for h in self.client.lrange(
                self.key.history(invocation.invocation_id), 0, -1
            )
        ]

    def _set_result(
        self, invocation: "DistributedInvocation[Params, Result]", result: "Result"
    ) -> None:
        """
        Sets the result for an invocation in Redis.

        :param DistributedInvocation invocation: The invocation to set the result for.
        :param Result result: The result of the invocation.
        """
        self.client.set(
            self.key.result(invocation.invocation_id),
            self.app.serializer.serialize(result),
        )

    def _get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        """
        Retrieves the result of an invocation from Redis.

        :param DistributedInvocation invocation: The invocation to get the result for.
        :return: The result of the invocation.
        """
        if res := self.client.get(self.key.result(invocation.invocation_id)):
            return self.app.serializer.deserialize(res.decode())
        raise KeyError(f"Result for invocation {invocation.invocation_id} not found")

    def _set_exception(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        exception: "Exception",
    ) -> None:
        """
        Sets the exception for an invocation in Redis.

        :param DistributedInvocation invocation: The invocation to set the exception for.
        :param Exception exception: The exception to set.
        """
        serialized_exception: dict[str, str | bool] = {
            "error_name": exception.__class__.__name__
        }
        if isinstance(exception, exceptions.PynencError):
            serialized_exception["pynenc_error"] = True
            serialized_exception["error_data"] = exception.to_json()
        else:
            serialized_exception["pynenc_error"] = False
            serialized_exception["error_data"] = self.app.serializer.serialize(
                exception
            )
        self.client.set(
            self.key.exception(invocation.invocation_id),
            json.dumps(serialized_exception),
        )

    def _get_exception(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> Exception:
        """
        Retrieves the exception of an invocation from Redis.

        :param DistributedInvocation invocation: The invocation to get the exception for.
        :return: The exception of the invocation.
        """
        if exc := self.client.get(self.key.exception(invocation.invocation_id)):
            serialized_exception = json.loads(exc.decode())
            if serialized_exception["pynenc_error"]:
                return exceptions.PynencError.from_json(
                    serialized_exception["error_name"],
                    serialized_exception["error_data"],
                )
            return self.app.serializer.deserialize(serialized_exception["error_data"])
        raise KeyError(f"Exception for invocation {invocation.invocation_id} not found")
