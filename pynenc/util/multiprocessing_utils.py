"""
Multiprocessing utilities and configuration for Pynenc runners.

This module ensures consistent multiprocessing behavior across platforms,
particularly for macOS which requires the 'spawn' start method to avoid
fork-related issues with certain libraries and debuggers.

Key components:
- configure_multiprocessing: Sets up spawn method for cross-platform compatibility
"""

import multiprocessing


def configure_multiprocessing() -> None:
    """
    Configure multiprocessing to use spawn method on all platforms.

    This prevents issues with:
    - macOS fork safety problems
    - VSCode debugger compatibility
    - Manager() connection issues

    Should be called once at module import, idempotent if called multiple times.
    """
    if multiprocessing.get_start_method(allow_none=True) != "spawn":
        try:
            multiprocessing.set_start_method("spawn", force=True)
        except RuntimeError:
            # Already set in a parent process, which is fine
            pass


# Configure at module import
configure_multiprocessing()
