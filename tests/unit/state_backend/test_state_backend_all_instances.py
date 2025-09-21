from datetime import datetime
from typing import TYPE_CHECKING

from pynenc.state_backend.base_state_backend import InvocationHistory, InvocationStatus

if TYPE_CHECKING:
    from pynenc import Pynenc


def test_history_records_are_stored_and_ordered(app_instance: "Pynenc") -> None:
    """
    Test that invocation history entries are stored with timestamp and status
    and are returned in timestamp order.
    """
    backend = app_instance.state_backend

    # Use the actual InvocationHistory class and set timestamps explicitly
    # This keeps the test simple and ensures type compatibility.
    hist1 = InvocationHistory(InvocationStatus.REGISTERED)
    hist1._timestamp = datetime.fromtimestamp(1.0)

    hist2 = InvocationHistory(InvocationStatus.RUNNING)
    hist2._timestamp = datetime.fromtimestamp(2.0)

    hist3 = InvocationHistory(InvocationStatus.FAILED)
    hist3._timestamp = datetime.fromtimestamp(3.0)

    backend._add_histories(["inv-1"], hist2)
    backend._add_histories(["inv-1"], hist1)
    backend._add_histories(["inv-1"], hist3)

    histories = backend.get_history("inv-1")
    assert [hist.status for hist in histories] == [
        hist1.status,
        hist2.status,
        hist3.status,
    ]
