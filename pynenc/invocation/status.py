from enum import Enum


class InvocationStatus(Enum):
    """
    An enumeration representing the status of a task invocation.

    Attributes
    ----------
    REGISTERED : str
        The task call has been routed and is registered
    PENDING : str
        The task call was picked by a runner but is not yet executed.
        (config.max_pending_seconds)
        The status pending will expire after Config.max_pending_seconds
    RUNNING : str
        The task call is currently running.
    SUCCESS : str
        The task call finished without errors.
    PAUSED : str
        The task call execution is paused.
    SCHEDULED : str
        A task has been registered to run at a specific time. This is a subtype of REGISTERED.
    FAILED : str
        The task call finished with exceptions.
    RETRY : str
        The task call finished with a retriable exception.

    Notes
    -----
    FAILED, RETRY, and SUCCESS are the same category and can be considered as subtypes of a hypothetical TERMINATED status.
    """

    REGISTERED = "Registered"
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCESS = "Success"
    PAUSED = "Paused"
    SCHEDULED = "Scheduled"
    FAILED = "Failed"
    RETRY = "Retry"

    def is_available_for_run(self) -> bool:
        """
        Check if the task is in a state where it is available to be picked up and run by any broker.
        This means the task is not currently being executed, paused, or about to be executed by any other broker.

        Returns:
        bool
            True if the status is runnable by any broker, False otherwise.
        """
        return self in {
            InvocationStatus.REGISTERED,
            InvocationStatus.RETRY,
        }

    def is_final(self) -> bool:
        """
        Checks if the status is a final status.

        Returns:
        bool
            True if the status is final, False otherwise.
        """
        return self in {InvocationStatus.SUCCESS, InvocationStatus.FAILED}
