from unittest.mock import MagicMock, patch

from pynenc import Pynenc
from pynenc.cli.namespace import PynencCLINamespace
from pynenc.cli.runner_cli import start_runner_command

app = Pynenc()
app.conf.runner_cls = "ThreadRunner"


def test_start_runner() -> None:
    """Test start_runner_command"""
    args = PynencCLINamespace()
    args.app = "tests.unit.cli.test_runner_cli.app"
    args.app_instance = app
    # app.runner.run = MagicMock()
    with patch.object(app.runner, "run", new_callable=MagicMock) as mock_run:
        with patch("pynenc.cli.runner_cli.print") as mock_print:
            start_runner_command(args)
            mock_print.assert_called_with(
                "Starting runner for app: tests.unit.cli.test_runner_cli.app"
            )
        mock_run.assert_called()
