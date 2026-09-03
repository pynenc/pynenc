from pynenc_tests.conftest import MockPynenc


mock_app = MockPynenc()


@mock_app.task(queue="undeclared")
def undeclared_combo_task(label: str) -> str:
    return f"undeclared:{label}"


@mock_app.task(queue="default")
def default_combo_task(label: str) -> str:
    return f"default:{label}"


@mock_app.task(queue="payments")
def payment_combo_task(label: str) -> str:
    return f"payment:{label}"


@mock_app.task(queue="payments", priority=100.0)
def urgent_payment_combo_task(label: str) -> str:
    return f"urgent-payment:{label}"
