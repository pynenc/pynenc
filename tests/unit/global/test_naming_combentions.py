from pynenc.broker.base_broker import BaseBroker
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend.base_state_backend import BaseStateBackend

BASE_CLS = [BaseBroker, BaseOrchestrator, BaseRunner, BaseSerializer, BaseStateBackend]


def test_class_suffix() -> None:
    """Test that the subclasses of the base classes have the correct suffix"""
    for cls in BASE_CLS:
        for sub_cls in cls.__subclasses__():
            assert sub_cls.__name__.endswith(cls.__name__.split("Base")[1])
