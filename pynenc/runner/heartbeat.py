"""
Heartbeat utility for Pynenc runners.

Provides a function to start a background thread that periodically sends heartbeats for a runner context.
"""

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.runner.runner_context import RunnerContext


def start_invocation_runner_heartbeat(
    app: "Pynenc", runner_ctx: "RunnerContext"
) -> threading.Thread:
    """
    Start a background thread that periodically sends a heartbeat for a runner context.

    This is used for invocation runners (not atomic service runners).
    """

    def heartbeat_loop() -> None:
        while True:
            app.orchestrator.register_runner_heartbeat(
                runner_ctx, can_run_atomic_service=False
            )
            time.sleep(app.runner.conf.atomic_service_check_interval_minutes * 60)

    thread = threading.Thread(target=heartbeat_loop, daemon=True)
    thread.start()
    return thread
