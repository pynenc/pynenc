from unittest.mock import MagicMock
from typing import Any

import pytest

from pynenc import Pynenc
from pynenc.orchestrator.mem_orchestrator import MemOrchestrator


@pytest.fixture
def app() -> Pynenc:
    return Pynenc()


def test_subclass_instantiation(app: Pynenc) -> None:
    """Test that is not possible to change any component subclass once initialized"""

    app.set_orchestrator_cls(MemOrchestrator)
    with pytest.raises(Exception) as exc_info:
        _ = app.orchestrator
        app.set_orchestrator_cls(MemOrchestrator)
