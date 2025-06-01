import json
from functools import cached_property
from typing import TYPE_CHECKING, Any

import redis

from pynenc import exceptions
from pynenc.conf.config_redis import ConfigRedis
from pynenc.conf.config_state_backend import ConfigStateBackendRedis
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.state_backend.base_state_backend import BaseStateBackend, InvocationHistory
from pynenc.util.redis_client import get_redis_client
from pynenc.util.redis_keys import Key
from pynenc.workflow import WorkflowIdentity

if TYPE_CHECKING:
    from pynenc.app import AppInfo, Pynenc
    from pynenc.types import Params, Result


class RedisStateBackend(BaseStateBackend):
    """
    A Redis-based implementation of the state backend.

    This backend uses Redis to store and retrieve the state of invocations, including their data,
    history, results, and exceptions. It's suitable for distributed systems where shared state management is required.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._client: redis.Redis | None = None
        self.key = Key(app.app_id, "state_backend")

    @cached_property
    def conf(self) -> ConfigStateBackendRedis:
        return ConfigStateBackendRedis(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def client(self) -> redis.Redis:
        """Lazy initialization of Redis client"""
        if self._client is None:
            self._client = get_redis_client(self.conf)
        return self._client

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

    def get_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, default: Any = None
    ) -> Any:
        """
        Get a value from workflow data.

        :param workflow_identity: Workflow identity
        :param key: Data key to retrieve
        :param default: Default value if key doesn't exist
        :return: Stored value or default
        """
        data_key = self.key.workflow_data_value(workflow_identity.workflow_id, key)
        serialized_value = self.client.get(data_key)

        if serialized_value is None:
            return default

        return self.app.serializer.deserialize(serialized_value.decode())

    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """
        Set a value in workflow data.

        :param workflow_identity: Workflow identity
        :param key: Data key to set
        :param value: Value to store
        """
        data_key = self.key.workflow_data_value(workflow_identity.workflow_id, key)
        serialized_value = self.app.serializer.serialize(value)
        self.client.set(data_key, serialized_value)

    def get_workflow_deterministic_value(
        self, workflow: "WorkflowIdentity", key: str
    ) -> Any:
        """
        Retrieve a deterministic value for workflow operations.

        :param workflow: The workflow identity
        :param key: Key identifying the deterministic value
        :return: The stored value or None if not found
        """
        deterministic_key = self.key.workflow_deterministic_value(
            workflow.workflow_id, key
        )
        value = self.client.get(deterministic_key)

        if value is None:
            return None

        return self.app.serializer.deserialize(value.decode())

    def set_workflow_deterministic_value(
        self, workflow: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """
        Store a deterministic value for workflow operations.

        :param workflow: The workflow identity
        :param key: Key identifying the deterministic value
        :param value: The value to store (must be serializable)
        """
        deterministic_key = self.key.workflow_deterministic_value(
            workflow.workflow_id, key
        )
        serialized_value = self.app.serializer.serialize(value)
        self.client.set(deterministic_key, serialized_value)

    def store_app_info(self, app_info: "AppInfo") -> None:
        """
        Register this app's information in the state backend for discovery.

        :param app_info: The app information to store
        """
        self.client.set(self.key.all_apps_info_key(app_info.app_id), app_info.to_json())

    def get_app_info(self) -> "AppInfo":
        """
        Retrieve information of the current app.

        :return: The app information
        :raises ValueError: If app info is not found
        """
        app_info_data = self.client.get(self.key.all_apps_info_key(self.app.app_id))

        if not app_info_data:
            raise ValueError(f"No app info found for app_id '{self.app.app_id}'")

        return AppInfo.from_json(app_info_data.decode())

    @staticmethod
    def get_all_app_infos() -> dict[str, "AppInfo"]:
        """
        Retrieve all app information registered in this state backend.

        :return: Dictionary mapping app_id to app information
        """
        from pynenc.app import AppInfo

        redis_client = get_redis_client(ConfigRedis())
        # Scan for all app info keys
        pattern = Key.all_apps_info_key("*")
        all_keys = redis_client.keys(pattern)
        # Extract all available app IDs and Info
        result = {}
        for key in all_keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            app_id = key_str.split(":")[-1]  # Last part is app_id
            app_info_data = redis_client.get(key_str)
            if app_info_data:
                app_info = AppInfo.from_json(app_info_data.decode())
                result[app_id] = app_info
        return result
