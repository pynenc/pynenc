from pynenc_tests.conftest import MockPynenc


mock_app = MockPynenc()


@mock_app.task(queue="undeclared")
def undeclared_queue_task(label: str) -> str:
    return f"undeclared:{label}"


@mock_app.task(queue="default")
def default_queue_task(label: str) -> str:
    return f"default:{label}"
