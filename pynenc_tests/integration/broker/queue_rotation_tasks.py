from pynenc_tests.conftest import MockPynenc


mock_app = MockPynenc()


@mock_app.task(queue="default")
def default_rotation_task(label: str) -> str:
    return f"default:{label}"


@mock_app.task(queue="payments")
def payment_rotation_task(label: str) -> str:
    return f"payments:{label}"


@mock_app.task(queue="reports")
def report_rotation_task(label: str) -> str:
    return f"reports:{label}"
