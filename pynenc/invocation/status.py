from enum import StrEnum, auto


class InvocationStatus(StrEnum):
    """
    An enumeration representing the status of a task invocation.

    The PENDING status will expire after the time specified in
    :attr:`~pynenc.conf.config_pynenc.ConfigPynenc.max_pending_seconds`.

    ```{note}
    FAILED, RETRY, and SUCCESS are the same category and can be considered as subtypes of a hypothetical TERMINATED status.
    ```

    :cvar REGISTERED:
        The task call has been routed and is registered.

    :cvar REROUTED:
        The task call has been re-routed and is registered.
        This status is used when a registered task cannot be set to pending and then run
        in case of running concurrency checks (e.g., only one instance of the task can run at the same time).

    :cvar PENDING:
        The task call was picked by a runner but is not yet executed.
        The status pending will expire after Config.max_pending_seconds.

    :cvar RUNNING:
        The task call is currently running.

    :cvar SUCCESS:
        The task call finished without errors.

    :cvar PAUSED:
        The task call execution is paused.

    :cvar SCHEDULED:
        A task has been registered to run at a specific time. This is a subtype of REGISTERED.

    :cvar FAILED:
        The task call finished with exceptions.

    :cvar RETRY:
        The task call finished with a retriable exception.
    """

    REGISTERED = auto()
    REROUTED = auto()
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    PAUSED = auto()
    SCHEDULED = auto()
    FAILED = auto()
    RETRY = auto()

    def is_available_for_run(self) -> bool:
        """
        Check if the task is in a state where it is available to be picked up and run by any broker.
        This means the task is not currently being executed, paused, or about to be executed by any other broker.

        :return:
            True if the status is runnable by any broker, False otherwise.
        """
        return self in {
            InvocationStatus.REGISTERED,
            InvocationStatus.REROUTED,
            InvocationStatus.RETRY,
        }

    def is_final(self) -> bool:
        """
        Checks if the status is a final status.

        :return: True if the status is final, False otherwise.
        """
        return self in {InvocationStatus.SUCCESS, InvocationStatus.FAILED}
