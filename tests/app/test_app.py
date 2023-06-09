from unittest.mock import MagicMock
from typing import Any

import pytest

from pynenc import Pynenc
from pynenc.orchestrator.mem_orchestrator import MemOrchestrator
from pynenc.broker.mem_broker import MemBroker
from pynenc.state_backend.mem_state_backend import MemStateBackend


@pytest.fixture
def app() -> Pynenc:
    return Pynenc()


def test_subclass_instantiation(app: Pynenc) -> None:
    """Test that is not possible to change any component subclass once initialized"""

    app.set_orchestrator_cls(MemOrchestrator)
    with pytest.raises(Exception):
        _ = app.orchestrator
        app.set_orchestrator_cls(MemOrchestrator)

    app.set_broker_cls(MemBroker)
    with pytest.raises(Exception):
        _ = app.broker
        app.set_broker_cls(MemBroker)

    app.set_state_backend_cls(MemStateBackend)
    with pytest.raises(Exception):
        _ = app.state_backend
        app.set_state_backend_cls(MemStateBackend)
