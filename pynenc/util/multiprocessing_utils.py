"""
Multiprocessing utilities for Pynenc runners.

Provides validation and helpful error messages for multiprocessing usage,
particularly for detecting missing if __name__ == '__main__' guards.

Key components:
- warn_missing_main_guard: Detects and warns about unsafe multiprocessing usage
"""

import sys
import warnings


def warn_missing_main_guard() -> None:
    """
    Detect and warn about missing if __name__ == '__main__' guard.

    Checks if the main module appears to be running without proper protection
    for multiprocessing spawning. This is required on macOS and Windows when
    using runners that spawn worker processes (like MultiThreadRunner).

    The warning is only shown if running from a file (not interactive shell).
    """
    main_module = sys.modules.get("__main__")
    if main_module is None or not hasattr(main_module, "__file__"):
        # Not main module or running from interactive shell - safe to skip
        return

    warnings.warn(
        "\n"
        "┌─────────────────────────────────────────────────────────────────────┐\n"
        "│ MultiThreadRunner requires if __name__ == '__main__': guard        │\n"
        "├─────────────────────────────────────────────────────────────────────┤\n"
        "│                                                                     │\n"
        "│ On macOS and Windows, wrap your application startup code:          │\n"
        "│                                                                     │\n"
        "│     if __name__ == '__main__':                                      │\n"
        "│         from multiprocessing import freeze_support                  │\n"
        "│         freeze_support()                                            │\n"
        "│         app.runner.run()                                            │\n"
        "│                                                                     │\n"
        "│ This prevents recursive spawning when creating worker processes.   │\n"
        "│                                                                     │\n"
        "│ See: https://docs.python.org/3/library/multiprocessing.html        │\n"
        "└─────────────────────────────────────────────────────────────────────┘\n",
        RuntimeWarning,
        stacklevel=4,
    )
