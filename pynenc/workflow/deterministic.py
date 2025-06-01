"""
Deterministic execution system for Pynenc workflows.

This module handles deterministic operations in workflows, ensuring that
non-deterministic functions like random numbers, timestamps, and UUIDs
behave deterministically across workflow executions and replays.
"""
from __future__ import annotations

import datetime
import hashlib
import random
import uuid
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from pynenc.arguments import Arguments
from pynenc.call import Call

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task
    from pynenc.workflow.identity import WorkflowIdentity

T = TypeVar("T")


class DeterministicExecutor:
    """
    Handles deterministic operations for workflow execution.

    This class ensures that operations like random number generation,
    time functions, and task executions behave deterministically
    across workflow replays by using deterministic seeds and storing
    results in the state backend.
    """

    def __init__(self, workflow_identity: WorkflowIdentity, app: Pynenc) -> None:
        """
        Initialize the deterministic execution engine.

        :param workflow_identity: The workflow identity
        :param app: The Pynenc application instance
        """
        self.workflow_identity = workflow_identity
        self.app = app
        self._operation_counters: dict[str, int] = {}

    def _get_next_sequence(self, operation: str) -> int:
        """
        Get the next sequence number for an operation.

        Tracks the current position for this executor instance, starting from 0
        and incrementing as operations are performed or replayed.

        :param operation: The operation name
        :return: The next sequence number
        """
        if operation not in self._operation_counters:
            self._operation_counters[operation] = 0

        # Increment counter for this executor instance
        self._operation_counters[operation] += 1
        return self._operation_counters[operation]

    def _deterministic_operation(self, operation: str, generator: Callable[[], T]) -> T:
        """
        Execute an operation with deterministic results.

        First checks if a value exists for this operation sequence, otherwise
        generates a new value using the provided generator and stores it.

        :param operation: Operation type (e.g., "random", "time")
        :param generator: Function to generate value if not already recorded
        :return: Deterministic operation result
        """
        # Get next sequence number for this executor instance
        sequence = self._get_next_sequence(operation)
        operation_key = f"{operation}:{sequence}"

        # Check if we have a recorded value first
        value = self.app.state_backend.get_workflow_deterministic_value(
            self.workflow_identity, operation_key
        )

        if value is not None:
            # Return recorded value (replay mode)
            return value

        # Generate new value and store it
        value = generator()
        self.app.state_backend.set_workflow_deterministic_value(
            self.workflow_identity, operation_key, value
        )

        # Update the total count in state backend for this operation type
        total_count_key = f"counter:{operation}"
        current_total = (
            self.app.state_backend.get_workflow_deterministic_value(
                self.workflow_identity, total_count_key
            )
            or 0
        )
        self.app.state_backend.set_workflow_deterministic_value(
            self.workflow_identity, total_count_key, max(current_total, sequence)
        )

        return value

    def get_base_time(self) -> datetime.datetime:
        """
        Get or establish workflow base time for deterministic timestamps.

        :return: Base time for deterministic timestamps
        """
        base_time_key = "workflow:base_time"
        base_time = self.app.state_backend.get_workflow_deterministic_value(
            self.workflow_identity, base_time_key
        )

        if base_time is None:
            base_time = datetime.datetime.now(datetime.timezone.utc)
            self.app.state_backend.set_workflow_deterministic_value(
                self.workflow_identity, base_time_key, base_time
            )

        return base_time

    def get_operation_count(self, operation: str) -> int:
        """
        Get current count for an operation type from this executor instance.

        This represents how many operations of this type have been processed
        by this executor instance, not the total stored count.

        :param operation: Operation type name
        :return: Current operation count for this executor instance
        """
        return self._operation_counters.get(operation, 0)

    def random(self) -> float:
        """
        Generate a deterministic random number using workflow-specific seed.

        :return: A random float between 0.0 and 1.0
        """

        def random_generator() -> float:
            # Create deterministic seed from workflow and current sequence
            sequence = self._operation_counters.get("random", 0) + 1
            seed_string = f"{self.workflow_identity.workflow_id}:random:{sequence}"
            seed = int(hashlib.md5(seed_string.encode()).hexdigest()[:8], 16)

            # Use deterministic random generator
            temp_random = random.Random(seed)
            return temp_random.random()

        return self._deterministic_operation("random", random_generator)

    def utc_now(self) -> datetime.datetime:
        """
        Get current time deterministically by advancing from base time.

        :return: Deterministic datetime with UTC timezone
        """

        def time_generator() -> datetime.datetime:
            base_time = self.get_base_time()
            # Use current sequence number to advance time deterministically
            sequence = self._operation_counters.get("time", 0) + 1
            return base_time + datetime.timedelta(seconds=sequence)

        return self._deterministic_operation("time", time_generator)

    def uuid(self) -> str:
        """
        Generate a deterministic UUID using workflow-specific seed.

        :return: UUID string
        """

        def uuid_generator() -> str:
            # Create deterministic UUID from workflow and current sequence
            sequence = self._operation_counters.get("uuid", 0) + 1
            seed_string = f"{self.workflow_identity.workflow_id}:uuid:{sequence}"
            hash_bytes = hashlib.md5(seed_string.encode()).digest()
            return str(uuid.UUID(bytes=hash_bytes))

        return self._deterministic_operation("uuid", uuid_generator)

    def execute_task(
        self, task: Task, *args: Any, **kwargs: Any
    ) -> DistributedInvocation:
        """
        Execute a task with deterministic replay capabilities.

        Returns a DistributedInvocation that allows flexible handling of results.
        During replay, if the same task with identical arguments was already executed,
        it returns the existing invocation from the state backend.

        :param task: The task to execute
        :param args: Positional arguments
        :param kwargs: Keyword arguments
        :return: DistributedInvocation for flexible result handling
        """
        # Create arguments and call to get the proper call_id
        arguments = Arguments.from_call(task.func, *args, **kwargs)
        call: Call = Call(task=task, _arguments=arguments)
        # Use call_id as the unique key - no need for additional hashing
        task_invocation_key = f"task_invocation:{call.call_id}"
        # Check if we already have a recorded invocation ID
        cached_invocation_id = self.app.state_backend.get_workflow_deterministic_value(
            self.workflow_identity, task_invocation_key
        )
        if cached_invocation_id is not None:
            # Return existing invocation from state backend
            return self.app.state_backend.get_invocation(cached_invocation_id)
        # Execute the task and record the invocation ID
        invocation = task(*args, **kwargs)
        # Store only the invocation ID for future replays
        self.app.state_backend.set_workflow_deterministic_value(
            self.workflow_identity, task_invocation_key, invocation.invocation_id
        )
        return invocation  # type: ignore[return-value]
