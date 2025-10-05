import logging
import os
import signal
import subprocess


def _cleanup_multiprocessing_children() -> None:
    """Kill lingering Python multiprocessing child processes (spawn/resource_tracker) before app init."""
    try:
        # List all python processes
        result = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True, check=True
        )
        for line in result.stdout.splitlines():
            if "python" in line and (
                "multiprocessing.spawn" in line
                or "multiprocessing.resource_tracker" in line
            ):
                parts = line.split()
                pid = int(parts[1])
                try:
                    # Send SIGKILL to the process
                    os.kill(pid, signal.SIGKILL)
                    logging.info(f"Killed lingering multiprocessing process {pid}")
                except Exception as e:
                    logging.warning(f"Failed to kill process {pid}: {e}")
    except Exception as e:
        logging.warning(f"Failed to cleanup multiprocessing children: {e}")
