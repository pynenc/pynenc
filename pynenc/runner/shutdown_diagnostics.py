"""
Shutdown diagnostics utilities for runner termination events.

Centralises signal classification, system environment collection, and structured
logging so every runner produces consistent, actionable output on SIGTERM/OOM.
"""

import os
import platform
import signal
from enum import StrEnum, auto
from logging import Logger
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from multiprocessing import Process
    from threading import Thread


class ShutdownReason(StrEnum):
    NORMAL = auto()
    SIGTERM = auto()
    SIGINT = auto()
    # SIGKILL is the OOM signal in containerised environments (Kubernetes OOMKilled)
    OOM_KILLED = auto()
    UNKNOWN_SIGNAL = auto()


def classify_signal(signum: int | None) -> ShutdownReason:
    """Classify a signal number into a ShutdownReason."""
    if signum is None:
        return ShutdownReason.NORMAL
    match signum:
        case signal.SIGTERM:
            return ShutdownReason.SIGTERM
        case signal.SIGINT:
            return ShutdownReason.SIGINT
        case signal.SIGKILL:
            return ShutdownReason.OOM_KILLED
        case _:
            return ShutdownReason.UNKNOWN_SIGNAL


def _system_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    try:
        info["load_avg"] = os.getloadavg()
    except (AttributeError, OSError):
        pass
    try:
        import resource  # Unix only

        usage = resource.getrusage(resource.RUSAGE_SELF)
        # Linux reports in KB, macOS in bytes
        div = 1024 if platform.system() == "Linux" else 1024 * 1024
        info["mem_rss_mb"] = round(usage.ru_maxrss / div, 1)
    except (ImportError, OSError):
        pass
    return info


def log_runner_shutdown(
    logger: Logger,
    runner_cls: str,
    runner_id: str,
    signum: int | None,
    *,
    processes: "dict[str, tuple[Process, str | None]] | None" = None,
    threads: "dict[str, tuple[Thread, str | None]] | None" = None,
    waiting_inv_ids: list[str] | None = None,
) -> None:
    """
    Log structured diagnostics for a runner shutdown event.

    Collects system environment info automatically. Processes and threads are
    passed as dicts mapping an identifier to (object, invocation_id_or_None).

    :param Logger logger: Logger to write to.
    :param str runner_cls: Runner class name.
    :param str runner_id: Runner instance ID.
    :param int | None signum: Signal that triggered shutdown, if any.
    :param processes: Child processes keyed by runner_id.
    :param threads: Active threads keyed by an identifier.
    :param waiting_inv_ids: Invocations blocked waiting for results.
    """
    reason = classify_signal(signum)
    is_oom = reason == ShutdownReason.OOM_KILLED
    log_fn = logger.critical if is_oom else logger.warning

    sys_data = _system_info()
    sys_parts = [
        f"os={sys_data['platform']}",
        f"py={sys_data['python']}",
        f"cpus={sys_data['cpu_count']}",
    ]
    if "load_avg" in sys_data:
        sys_parts.append(f"load={sys_data['load_avg']}")
    if "mem_rss_mb" in sys_data:
        sys_parts.append(f"rss={sys_data['mem_rss_mb']}MB")

    lines = [
        "=" * 60,
        f"RUNNER SHUTDOWN {'[OOM DETECTED] ' if is_oom else ''}DIAGNOSTICS",
        f"  runner={runner_cls}({runner_id[:8]}) pid={os.getpid()} signal={signum} reason={reason}",
        f"  sys: {' | '.join(sys_parts)}",
    ]

    if processes:
        lines.append(f"  processes ({len(processes)}):")
        for rid, (proc, inv_id) in processes.items():
            state = "ALIVE" if proc.is_alive() else "DEAD"
            inv = f" inv={inv_id}" if inv_id else ""
            lines.append(f"    {state} pid={proc.pid} id={rid[:8]}{inv}")

    if threads:
        lines.append(f"  threads ({len(threads)}):")
        for key, (thread, inv_id) in threads.items():
            state = "ALIVE" if thread.is_alive() else "DEAD"
            inv = f" inv={inv_id}" if inv_id else ""
            lines.append(f"    {state} name={thread.name} key={key[:8]}{inv}")

    if waiting_inv_ids:
        lines.append(
            f"  waiting ({len(waiting_inv_ids)}): {', '.join(waiting_inv_ids)}"
        )

    lines.append("=" * 60)
    log_fn("\n".join(lines))

    if is_oom:
        logger.critical(
            "OOM KILLED: likely memory exhaustion. Check resource limits and task "
            "payload sizes. Affected invocations will be rerouted."
        )
