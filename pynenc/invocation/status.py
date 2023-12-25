from enum import StrEnum, auto


class InvocationStatus(StrEnum):
    """
    An enumeration representing the status of a task invocation.

    The PENDING status will expire after the time specified in
    :attr:`~pynenc.conf.config_pynenc.ConfigPynenc.max_pending_seconds`.

    Notes
    -----
    FAILED, RETRY, and SUCCESS are the same category and can be considered as subtypes of a hypothetical TERMINATED status.
    """

    #: The task call has been routed and is registered
    REGISTERED = auto()

    #: The task call has been re-routed and is registered
    #: This status is used when a registered task cannot be set to pending and then runned
    #: in case of running concurrency checks (eg. only one instance of the task can run at the same time)
    # TODO: in case of running concurrency, retry, schedule (run after x seconds) or reuse running invocation
    REROUTED = auto()

    #: The task call was picked by a runner but is not yet executed.
    #: The status pending will expire after Config.max_pending_seconds
    PENDING = auto()

    #: The task call is currently running.
    RUNNING = auto()

    #: The task call finished without errors.
    SUCCESS = auto()

    #: The task call execution is paused.
    PAUSED = auto()

    #: A task has been registered to run at a specific time. This is a subtype of REGISTERED.
    SCHEDULED = auto()

    #: The task call finished with exceptions.
    FAILED = auto()

    #: The task call finished with a retriable exception.
    RETRY = auto()

    def is_available_for_run(self) -> bool:
        """
        Check if the task is in a state where it is available to be picked up and run by any broker.
        This means the task is not currently being executed, paused, or about to be executed by any other broker.

        Returns
        -------
        bool
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

        Returns
        -------
        bool
            True if the status is final, False otherwise.
        """
        return self in {InvocationStatus.SUCCESS, InvocationStatus.FAILED}
