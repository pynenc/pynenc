import hashlib
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest


def get_unique_id(length: int = 16) -> str:
    """Return a short stable unique id for tests.

    :param int length: number of hex characters to return
    :return: shortened sha256 hex of uuid4
    """
    _id = uuid.uuid4()
    return hashlib.sha256(_id.bytes).hexdigest()[:length]


def get_module_name(request: "FixtureRequest") -> tuple[str, str]:
    """Get the module name and test name from a pytest request.

    :param FixtureRequest request: the pytest request object
    :return: tuple(module_name, test_name)
    """
    test_name = request.node.name.replace("[", "(").replace("]", ")")
    test_module = request.node.module.__name__
    return test_module, test_name
