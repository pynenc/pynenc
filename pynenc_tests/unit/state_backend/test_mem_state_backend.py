import pytest

from pynenc import Pynenc
from pynenc.exceptions import InvocationNotFoundError
from pynenc.identifiers.invocation_id import InvocationId

app = Pynenc()


def test_invocation_not_found() -> None:
    """Test that requesting a non-existent invocation raises the correct error."""
    with pytest.raises(InvocationNotFoundError):
        app.state_backend.get_invocation(InvocationId("non_existent_id"))
