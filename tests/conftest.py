from unittest.mock import MagicMock
from functools import cached_property

import pytest

from pynenc import Pynenc
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.broker.base_broker import BaseBroker


class MockBroker(BaseBroker):
    _route_invocation = MagicMock()
    _retrieve_invocation = MagicMock()

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._route_invocation.reset_mock()
        self._retrieve_invocation.reset_mock()


class MockBaseOrchestrator(BaseOrchestrator):
    get_existing_invocations = MagicMock()
    set_invocation_status = MagicMock()

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.get_existing_invocations.reset_mock()
        self.set_invocation_status.reset_mock()


class MockPynenc(Pynenc):
    @cached_property
    def broker(self) -> MockBroker:
        return MockBroker(self)

    @cached_property
    def orchestrator(self) -> MockBaseOrchestrator:
        return MockBaseOrchestrator(self)


@pytest.fixture
def mock_base_app() -> MockPynenc:
    return MockPynenc()
