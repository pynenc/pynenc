from enum import Enum


class InvocationStatus(Enum):
    """
    An enumeration representing the status of a task invocation.

    Attributes
    ----------
    REGISTERED : int
        The task call has been routed, is registered and pending of execution.
    RUNNING : int
        The task call is currently running.
    SUCCESS : int
        The task call finished without errors.
    PAUSED : int
        The task call execution is paused.
    SCHEDULED : int
        A task has been registered to run at a specific time. This is a subtype of REGISTERED.
    FAILED : int
        The task call finished with exceptions.
    RETRY : int
        The task call finished with a retriable exception.
    PENDING : int
        The task call was picked by a runner but is not yet executed.

    Notes
    -----
    FAILED, RETRY, and SUCCESS are the same category and can be considered as subtypes of a hypothetical TERMINATED status.
    """

    REGISTERED = "Registered"
    RUNNING = "Running"
    SUCCESS = "Success"
    PAUSED = "Paused"
    SCHEDULED = "Scheduled"
    FAILED = "Failed"
    RETRY = "Retry"
    PENDING = "Pending"
