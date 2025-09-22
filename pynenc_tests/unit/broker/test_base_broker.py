from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.conf.config_broker import ConfigBroker
from pynenc.invocation import DistributedInvocation
from pynenc_tests.conftest import MockBroker, MockPynenc

mock_base_app = MockPynenc()


@mock_base_app.task
def dummy_task(x: int, y: int) -> int:
    return x + y


def test_route_task() -> None:
    """Test that the broker will generate an invocation, route it and change status"""

    # basically same test as tests/orchestrator/test_base_orchestrator.py -> test_route_default
    # but calling directly the broker without passing trhough orchestrator

    invocation: DistributedInvocation = mock_base_app.broker.route_call(
        Call(dummy_task, Arguments({"x": 0, "y": 0}))
    )
    assert isinstance(invocation, DistributedInvocation)
    mock_base_app.broker.route_invocation_mock.assert_called_once()


def test_base_broker_conf() -> None:
    # Create an instance of BaseBroker
    broker = MockBroker(app=mock_base_app)

    # Test the conf property
    conf = broker.conf
    assert isinstance(conf, ConfigBroker)
