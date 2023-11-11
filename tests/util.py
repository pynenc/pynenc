import uuid
import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _pytest.python import Metafunc
    from _pytest.fixtures import FixtureRequest


def get_unique_id(length: int = 16) -> str:
    _id = uuid.uuid4()
    return hashlib.sha256(_id.bytes).hexdigest()[:length]


def get_module_name(request: "FixtureRequest") -> tuple[str, str]:
    """Get the module name and test name from a pytest request."""
    test_name = request.node.name.replace("[", "(").replace("]", ")")
    test_module = request.node.module.__name__
    return test_module, test_name
